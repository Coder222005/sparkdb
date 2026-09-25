"""Tests for Storage Optimization (Batch/Columnar Properties, Zero-Copy LRU) and Binary MessagePack Protocol."""
import json
import os
import shutil
import tempfile
import threading
import urllib.request
from http.server import HTTPServer
import pytest

import msgpack
from sparkdb import SparkDB
from sparkdb.core.engine import SparkDB as EngineSparkDB
from sparkdb.core.property_store import PropertyStore, DiskPropertyStore
from sparkdb.client.remote_client import RemoteGraphClient, HttpResponseWrapper
from sparkdb.server.app import SparkDBRequestHandler, HAS_MSGPACK


@pytest.fixture
def temp_store_dir():
    temp_dir = tempfile.mkdtemp(prefix="sparkdb_storage_opt_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="module")
def remote_test_server():
    temp_dir = tempfile.mkdtemp(prefix="sparkdb_msgpack_srv_")
    db = EngineSparkDB(storage_dir=temp_dir)
    SparkDBRequestHandler.db = db

    server = HTTPServer(("127.0.0.1", 0), SparkDBRequestHandler)
    host, port = server.server_address

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield host, port

    server.shutdown()
    server.server_close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_batch_get_nodes_properties_in_memory():
    store = PropertyStore()
    store.set_node_properties(1, {"name": "Node1", "val": 10})
    store.set_node_properties(2, {"name": "Node2", "val": 20})
    store.set_node_properties(3, {"name": "Node3", "val": 30})

    batch = store.batch_get_nodes_properties([1, 2, 3, 999])
    assert len(batch) == 4
    assert batch[1] == {"name": "Node1", "val": 10}
    assert batch[2] == {"name": "Node2", "val": 20}
    assert batch[3] == {"name": "Node3", "val": 30}
    assert batch[999] == {}


def test_batch_get_nodes_properties_disk_large_chunking(temp_store_dir):
    db_file = os.path.join(temp_store_dir, "batch_disk.db")
    store = DiskPropertyStore(db_path=db_file, lru_cache_size=2000)

    # Insert 1250 nodes (spanning > 2 chunks of 500)
    batch_data = {i: {"index": i, "val": i * 1.5, "tag": f"tag_{i % 5}"} for i in range(1250)}
    store.set_nodes_properties_batch(batch_data)

    # Clear in-memory LRU cache to force SQLite batch read in 500-sized chunks
    store._node_lru.clear()
    assert len(store._node_lru) == 0

    all_ids = list(range(1250)) + [9999, 10000]
    res = store.batch_get_nodes_properties(all_ids)

    assert len(res) == 1252
    assert res[0]["index"] == 0
    assert res[500]["index"] == 500
    assert res[1249]["index"] == 1249
    assert res[9999] == {}
    assert res[10000] == {}

    # Verify that fetched nodes were populated into the LRU cache
    assert 0 in store._node_lru
    assert 500 in store._node_lru
    assert 1249 in store._node_lru


def test_zero_copy_property_reference(temp_store_dir):
    # In-memory store
    mem_store = PropertyStore()
    mem_store.set_node_properties(10, {"cpu": 4, "ram": 16})
    r1 = mem_store.get_node_properties(10)
    r2 = mem_store.get_node_properties(10)
    assert r1 is r2  # Same cached dict instance (zero-copy)

    # Disk store with LRU cache
    db_file = os.path.join(temp_store_dir, "zerocopy.db")
    disk_store = DiskPropertyStore(db_path=db_file, lru_cache_size=100)
    disk_store.set_node_properties(20, {"cores": 8, "gpu": True})

    d1 = disk_store.get_node_properties(20)
    d2 = disk_store.get_node_properties(20)
    assert d1 is d2  # Same cached dict instance from LRU (zero-copy)


def test_numeric_property_extraction_and_aggregations(temp_store_dir):
    db_file = os.path.join(temp_store_dir, "aggs.db")
    store = DiskPropertyStore(db_path=db_file, lru_cache_size=1000)

    # Setup numeric nodes including some non-numeric and boolean edge cases
    store.set_nodes_properties_batch({
        1: {"cost": 10.0, "active": True, "label": "a"},
        2: {"cost": 20.0, "active": False, "label": "b"},
        3: {"cost": 30.0, "active": True, "label": "c"},
        4: {"cost": 40.0, "active": True, "label": "d"},
        5: {"name": "No cost"},
    })

    node_ids = [1, 2, 3, 4, 5]
    vals = store.get_numeric_property_values(node_ids, "cost")
    assert vals == [10.0, 20.0, 30.0, 40.0]

    # Test aggregate functions directly from cache
    assert store.aggregate_numeric_property(node_ids, "cost", "sum") == 100.0
    assert store.aggregate_numeric_property(node_ids, "cost", "avg") == 25.0
    assert store.aggregate_numeric_property(node_ids, "cost", "min") == 10.0
    assert store.aggregate_numeric_property(node_ids, "cost", "max") == 40.0
    assert store.aggregate_numeric_property(node_ids, "cost", "count") == 4.0

    # Ensure booleans are not treated as numbers
    assert store.get_numeric_property_values([1, 2], "active") == []


def test_server_msgpack_and_json_content_negotiation(remote_test_server):
    host, port = remote_test_server
    base_url = f"http://{host}:{port}"

    # 1. Health request with standard Accept -> JSON
    req_json = urllib.request.Request(f"{base_url}/health", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req_json) as resp:
        assert "application/json" in resp.headers.get("Content-Type", "")
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "healthy"

    # 2. Health request with Accept: application/msgpack -> MessagePack
    req_msgpack = urllib.request.Request(f"{base_url}/health", headers={"Accept": "application/msgpack"})
    with urllib.request.urlopen(req_msgpack) as resp:
        assert "application/msgpack" in resp.headers.get("Content-Type", "")
        raw = resp.read()
        data = msgpack.unpackb(raw, raw=False)
        assert data["status"] == "healthy"

    # 3. Health request with ?format=msgpack query param -> MessagePack
    req_param = urllib.request.Request(f"{base_url}/health?format=msgpack")
    with urllib.request.urlopen(req_param) as resp:
        assert "application/msgpack" in resp.headers.get("Content-Type", "")
        data = msgpack.unpackb(resp.read(), raw=False)
        assert data["status"] == "healthy"


def test_remote_client_binary_protocol_e2e(remote_test_server):
    host, port = remote_test_server
    client = SparkDB(host=host, port=port)
    g = client.select_graph("binary_protocol_test")

    # Verify query over remote client using negotiated binary protocol
    res_create = g.query("CREATE (a:Server {hostname: 'edge-01', load: 0.75})")
    assert res_create.nodes_created == 1

    res_match = g.query("MATCH (s:Server) RETURN s.hostname, s.load")
    assert res_match.result_set == [["edge-01", 0.75]]

    # Test ontology retrieval over binary protocol
    ont = g.get_ontology()
    assert "Server" in ont["node_labels"]
    assert "hostname" in ont["property_keys_by_label"]["Server"]


def test_remote_client_transparent_json_fallback(remote_test_server, monkeypatch):
    host, port = remote_test_server
    base_url = f"http://{host}:{port}"

    # Simulate client environment where msgpack is NOT installed
    import sparkdb.client.remote_client as rc_mod
    monkeypatch.setattr(rc_mod, "HAS_MSGPACK", False)

    rc = RemoteGraphClient(name="json_fallback_graph", base_url=base_url)
    res = rc.query("CREATE (u:User {name: 'Alice'})")
    assert res.nodes_created == 1

    match_res = rc.query("MATCH (u:User) RETURN u.name")
    assert match_res.result_set == [["Alice"]]
