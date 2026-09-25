"""Tests for SparkDB Tiered Hybrid Storage Mode (DiskPropertyStore via SQLite WAL + LRU Cache).

Verifies zero-OOM memory safety by spilling node/edge properties to disk
while keeping topology in memory.
"""
import os
import shutil
import tempfile
import pytest

from sparkdb import SparkDB
from sparkdb.core.property_store import DiskPropertyStore


@pytest.fixture
def temp_hybrid_dir():
    temp_dir = tempfile.mkdtemp(prefix="sparkdb_hybrid_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_disk_property_store_crud(temp_hybrid_dir):
    db_file = os.path.join(temp_hybrid_dir, "props.db")
    store = DiskPropertyStore(db_path=db_file, lru_cache_size=10)

    # 1. Node CRUD
    store.set_node_properties(1, {"name": "AMF", "type": "ControlPlane"})
    props = store.get_node_properties(1)
    assert props["name"] == "AMF"
    assert props["type"] == "ControlPlane"

    # Update node properties
    store.set_node_properties(1, {"version": "v1.2"})
    updated = store.get_node_properties(1)
    assert updated["name"] == "AMF"
    assert updated["version"] == "v1.2"

    # 2. Edge CRUD
    store.set_edge_properties(1, "CONNECTS_TO", 2, {"latency_ms": 1.5})
    e_props = store.get_edge_properties(1, "CONNECTS_TO", 2)
    assert e_props["latency_ms"] == 1.5

    # 3. Delete
    store.delete_node_properties(1)
    assert store.get_node_properties(1) == {}


def test_disk_property_store_lru_eviction(temp_hybrid_dir):
    db_file = os.path.join(temp_hybrid_dir, "props_lru.db")
    # Small cache of size 5
    store = DiskPropertyStore(db_path=db_file, lru_cache_size=5)

    for i in range(20):
        store.set_node_properties(i, {"val": f"node_{i}", "payload": "x" * 100})

    # Cache should only hold at most 5 items in RAM
    assert len(store._node_lru) <= 5

    # Access an evicted node (e.g. node 0)
    reloaded = store.get_node_properties(0)
    assert reloaded["val"] == "node_0"
    # Now node 0 is back in the LRU cache
    assert 0 in store._node_lru


def test_disk_property_store_indexing(temp_hybrid_dir):
    db_file = os.path.join(temp_hybrid_dir, "props_idx.db")
    store = DiskPropertyStore(db_path=db_file, lru_cache_size=100)

    store.set_node_properties(10, {"role": "UPF", "vendor": "VendorA"})
    store.set_node_properties(20, {"role": "SMF", "vendor": "VendorA"})
    store.set_node_properties(30, {"role": "UPF", "vendor": "VendorB"})

    upf_nodes = store.find_nodes_by_property("role", "UPF")
    assert upf_nodes == {10, 30}

    vendora_nodes = store.find_nodes_by_property("vendor", "VendorA")
    assert vendora_nodes == {10, 20}


def test_sparkdb_hybrid_cypher_e2e(temp_hybrid_dir):
    db = SparkDB(storage_dir=temp_hybrid_dir)
    g = db.select_graph("hybrid_infra", storage_mode="hybrid", lru_cache_size=50)

    # CREATE query with rich properties
    g.query("""
    CREATE (amf:NetworkFunction {name: 'AMF', role: 'ControlPlane', ip: '10.0.0.1'}),
           (smf:NetworkFunction {name: 'SMF', role: 'ControlPlane', ip: '10.0.0.2'}),
           (upf:NetworkFunction {name: 'UPF', role: 'UserPlane', ip: '10.0.0.3'}),
           (smf)-[:CONTROLS {protocol: 'PFCP', keepalive_s: 30}]->(upf)
    """)

    # MATCH query checking properties from disk store
    res = g.query("MATCH (s:NetworkFunction)-[r:CONTROLS]->(u:NetworkFunction) RETURN s.name, r.protocol, u.name")
    assert res.header == ["s.name", "r.protocol", "u.name"]
    assert res.result_set == [["SMF", "PFCP", "UPF"]]

    # Verify properties persisted on disk
    props_db = os.path.join(temp_hybrid_dir, "hybrid_infra", "properties.db")
    assert os.path.exists(props_db)


def test_sparkdb_hybrid_persistence(temp_hybrid_dir):
    # 1. First session
    db1 = SparkDB(storage_dir=temp_hybrid_dir)
    g1 = db1.select_graph("hybrid_persistent", storage_mode="hybrid", lru_cache_size=10)
    g1.query("CREATE (n:NF {name: 'gNodeB', cells: 12})")
    g1.checkpoint()

    # 2. Re-instantiate in a fresh DB session
    db2 = SparkDB(storage_dir=temp_hybrid_dir)
    g2 = db2.select_graph("hybrid_persistent", storage_mode="hybrid", lru_cache_size=10)

    res = g2.query("MATCH (n:NF) RETURN n.name, n.cells")
    assert len(res.result_set) == 1
    assert res.result_set[0] == ["gNodeB", 12]

