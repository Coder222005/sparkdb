"""Tests for SparkDB Remote Client over HTTP REST (Docker / Server mode)."""
import os
import shutil
import tempfile
import threading
import time
from http.server import HTTPServer
import pytest

from sparkdb import SparkDB
from sparkdb.core.engine import SparkDB as EngineSparkDB
from sparkdb.server.server import SparkDBRequestHandler


@pytest.fixture(scope="module")
def remote_server():
    temp_dir = tempfile.mkdtemp(prefix="sparkdb_remote_test_")
    db = EngineSparkDB(storage_dir=temp_dir)
    SparkDBRequestHandler.db = db

    # Bind to port 0 to let OS allocate an available port
    server = HTTPServer(("127.0.0.1", 0), SparkDBRequestHandler)
    host, port = server.server_address

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield host, port

    server.shutdown()
    server.server_close()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_remote_client_crud(remote_server):
    host, port = remote_server
    client = SparkDB(host=host, port=port)
    assert client.mode == "remote"

    g = client.select_graph("remote_graph")

    # 1. CREATE nodes and relationships via HTTP
    create_res = g.query("""
    CREATE (amf:NF {name: 'AMF'}), (smf:NF {name: 'SMF'}),
           (smf)-[:CONNECTS]->(amf)
    """)
    assert create_res.nodes_created == 2
    assert create_res.relationships_created == 1

    # 2. MATCH query via HTTP
    res = g.query("MATCH (s:NF)-[:CONNECTS]->(d:NF) RETURN s.name AS src, d.name AS dst")
    assert res.header == ["src", "dst"]
    assert res.result_set == [["SMF", "AMF"]]

    # 3. List graphs
    graphs = client.list_graphs()
    assert "remote_graph" in graphs

    # 4. Checkpoint
    cp = g.checkpoint()
    assert cp.get("status") == "ok"

    # 5. Drop graph
    dropped = client.drop_graph("remote_graph")
    assert dropped is True


def test_remote_client_project_isolation(remote_server):
    host, port = remote_server
    client = SparkDB(host=host, port=port)

    # 1. Create two isolated projects
    p1 = client.select_project("project_5g_core")
    p2 = client.select_project("project_ran_oai")

    p1.query("CREATE (n:Service {name: 'AMF'})")
    p2.query("CREATE (n:Service {name: 'CU-CP'})")

    # 2. List projects
    projects = client.list_projects()
    assert "project_5g_core" in projects
    assert "project_ran_oai" in projects

    # 3. Verify strict isolation
    res1 = p1.query("MATCH (n:Service) RETURN n.name")
    assert res1.result_set == [["AMF"]]

    res2 = p2.query("MATCH (n:Service) RETURN n.name")
    assert res2.result_set == [["CU-CP"]]

    # 4. Clean up
    client.drop_project("project_5g_core")
    client.drop_project("project_ran_oai")


def test_remote_query_batch_parallel(remote_server):
    host, port = remote_server
    client = SparkDB(host=host, port=port)
    g = client.select_graph("batch_test_graph")

    # Ingest test nodes with variables
    for i in range(20):
        g.query(f"CREATE (e:Entity {{id: {i}, name: 'Node_{i}', val: {i * 10}}})")

    # Issue 20 queries in a single parallel batch
    queries = [
        {"query": "MATCH (n:Entity {id: $id}) RETURN n.name AS name, n.val AS val", "params": {"id": i}}
        for i in range(20)
    ]

    results = g.query_batch(queries, parallel=True, max_workers=4)
    assert len(results) == 20
    for i, r in enumerate(results):
        assert r.header == ["name", "val"]
        assert r.result_set == [[f"Node_{i}", i * 10]]

    # Clean up
    client.drop_graph("batch_test_graph")


def test_remote_query_batch_with_mutations(remote_server):
    host, port = remote_server
    client = SparkDB(host=host, port=port)
    g = client.select_graph("batch_mutation_graph")

    # Batch with mixed creations and queries (triggers safe sequential execution)
    batch = [
        "CREATE (d1:Device {dev_id: 1, type: 'sensor'})",
        "CREATE (d2:Device {dev_id: 2, type: 'gateway'})",
        {"query": "MATCH (d:Device) RETURN count(d) AS total"},
    ]

    results = g.query_batch(batch, parallel=True)
    assert len(results) == 3
    assert results[0].nodes_created == 1
    assert results[1].nodes_created == 1
    assert results[2].result_set == [[2]]

    client.drop_graph("batch_mutation_graph")


def test_remote_query_batch_cold_project_prewarm(remote_server):
    host, port = remote_server
    client = SparkDB(host=host, port=port)

    # Edge case: Query a cold graph that has never been instantiated or selected
    cold_graph = client.select_graph("completely_cold_graph")
    batch = [
        "CREATE (c:ColdItem {code: 'ICE-01'})",
        "MATCH (c:ColdItem) RETURN c.code AS code",
    ]
    results = cold_graph.query_batch(batch, parallel=True, max_workers=4)
    assert len(results) == 2
    assert results[0].nodes_created == 1
    assert results[1].result_set == [["ICE-01"]]

    client.drop_graph("completely_cold_graph")


def test_embedded_query_batch(tmp_path):
    # Test embedded in-process GraphClient query_batch
    db = SparkDB(storage_dir=str(tmp_path))
    g = db.select_graph("embedded_batch_graph")

    g.query("CREATE (u1:User {uid: 101, role: 'admin'}), (u2:User {uid: 102, role: 'guest'})")

    queries = [
        {"query": "MATCH (u:User {uid: $uid}) RETURN u.role", "params": {"uid": 101}},
        {"query": "MATCH (u:User {uid: $uid}) RETURN u.role", "params": {"uid": 102}},
    ]
    results = g.query_batch(queries, parallel=True, max_workers=2)
    assert len(results) == 2
    assert results[0].result_set == [["admin"]]
    assert results[1].result_set == [["guest"]]


