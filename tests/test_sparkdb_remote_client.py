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

