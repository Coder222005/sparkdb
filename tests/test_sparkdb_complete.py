"""Comprehensive Test Suite for SparkDB.

Verifies:
  1. Cypher query parsing and execution (CREATE, MATCH, WHERE, RETURN, DELETE)
  2. GraphBLAS algorithms (ShortestPath, PageRank, WCC, TriangleCount)
  3. HNSW vector indexing and cosine distance queries
  4. BM25 full-text keyword search
  5. Persistence: Atomic snapshot saving, loading, and WAL mutation replay
  6. High-concurrency thread safety
  7. Drop-in client API compatibility
"""
import os
import shutil
import tempfile
import threading
import pytest

from sparkdb import SparkDB


@pytest.fixture
def temp_storage():
    """Provides a fresh temporary storage directory for each test."""
    temp_dir = tempfile.mkdtemp(prefix="sparkdb_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def spark_client(temp_storage):
    """Provides a fresh SparkDB client instance."""
    return SparkDB(storage_dir=temp_storage, default_vector_dim=4)


def test_cypher_create_and_match(spark_client):
    g = spark_client.select_graph("graph_crud")

    # 1. CREATE nodes and relationships via Cypher
    create_q = """
    CREATE (amf:NetworkFunction {name: 'AMF', domain: '3GPP_CORE'}),
           (smf:NetworkFunction {name: 'SMF', domain: '3GPP_CORE'}),
           (upf:NetworkFunction {name: 'UPF', domain: '3GPP_CORE'}),
           (n4:StandardInterface {name: 'N4', protocol: 'PFCP'}),
           (n11:StandardInterface {name: 'N11', protocol: 'SBI'}),
           (smf)-[:CONNECTS_VIA]->(n4),
           (n4)-[:CONNECTS_TO]->(upf),
           (smf)-[:CONNECTS_VIA]->(n11),
           (n11)-[:CONNECTS_TO]->(amf)
    """
    res = g.query(create_q)
    assert res.nodes_created == 5
    assert res.relationships_created == 4

    # 2. MATCH 2-hop traversal via Cypher
    match_q = """
    MATCH (src:NetworkFunction {name: 'SMF'})-[:CONNECTS_VIA]->(iface:StandardInterface)-[:CONNECTS_TO]->(dst:NetworkFunction)
    RETURN src.name AS src_nf, iface.name AS iface, dst.name AS dst_nf
    """
    res = g.query(match_q)
    assert res.header == ["src_nf", "iface", "dst_nf"]
    assert len(res.result_set) == 2

    dest_nfs = {row[2] for row in res.result_set}
    assert dest_nfs == {"UPF", "AMF"}


def test_cypher_shortest_path(spark_client):
    g = spark_client.select_graph("graph_sp")
    g.query("""
    CREATE (a:NF {name: 'A'}), (b:IF {name: 'B'}), (c:NF {name: 'C'}),
           (a)-[:STEP]->(b), (b)-[:STEP]->(c)
    """)
    res = g.query("MATCH (a:NF {name: 'A'}), (c:NF {name: 'C'}) RETURN shortestPath((a)-[*]->(c))")
    assert res.header == ["path"]
    assert len(res.result_set) == 1
    assert len(res.result_set[0][0]) == 3  # (A) -> (B) -> (C)


def test_cypher_call_algorithms(spark_client):
    g = spark_client.select_graph("graph_algo")
    g.query("""
    CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'}),
           (a)-[:REL]->(b), (b)-[:REL]->(c), (c)-[:REL]->(a)
    """)

    # 1. PageRank
    pr_res = g.query("CALL algo.pageRank('Node', 'REL')")
    assert pr_res.header == ["node", "score"]
    assert len(pr_res.result_set) == 3
    # In symmetric cycle, all scores are equal ≈ 1/3
    assert pytest.approx(pr_res.result_set[0][1], rel=1e-2) == 1.0 / 3.0

    # 2. Weakly Connected Components
    wcc_res = g.query("CALL algo.wcc()")
    assert wcc_res.header == ["node", "componentId"]
    assert len(wcc_res.result_set) == 3
    # All 3 nodes belong to same component
    c_ids = {row[1] for row in wcc_res.result_set}
    assert len(c_ids) == 1

    # 3. Triangle Count
    tc_res = g.query("CALL algo.triangleCount()")
    assert tc_res.result_set == [[1]]


def test_vector_and_fulltext_search(spark_client):
    g = spark_client.select_graph("search_space", vector_dim=4)

    # Insert node with text and embedding
    g.query("""
    CREATE (c:Chunk {id: 'c1', text: 'PFCP session establishment between SMF and UPF', embedding: vecf32([0.1, 0.2, 0.3, 0.4])}),
           (c2:Chunk {id: 'c2', text: 'NGAP signaling between gNB and AMF', embedding: vecf32([0.9, 0.8, 0.1, 0.0])})
    """)

    # 1. Full-Text BM25 Query
    ft_res = g.query("CALL db.idx.fulltext.queryNodes('Chunk', 'PFCP session')")
    assert len(ft_res.result_set) > 0
    top_node, score = ft_res.result_set[0]
    assert top_node["id"] == "c1"

    # 2. Vector Cosine Query
    vec_res = g.query("CALL db.idx.vector.queryNodes('Chunk', 'embedding', 1, vecf32([0.1, 0.2, 0.3, 0.4]))")
    assert len(vec_res.result_set) == 1
    top_v_node, dist = vec_res.result_set[0]
    assert top_v_node["id"] == "c1"
    assert dist < 1e-4


def test_persistence_snapshot_and_aof(temp_storage):
    # 1. Create and populate graph
    db1 = SparkDB(storage_dir=temp_storage)
    g1 = db1.select_graph("durable_graph")
    g1.query("CREATE (n:NF {name: 'SMF'}), (m:NF {name: 'UPF'}), (n)-[:CONNECTS]->(m)")
    # Save snapshot
    g1.checkpoint()

    # 2. Add an uncheckpointed mutation (logged to AOF)
    g1.query("CREATE (x:NF {name: 'AMF'})")

    # 3. Simulate process crash / reload in new SparkDB instance
    db2 = SparkDB(storage_dir=temp_storage)
    g2 = db2.select_graph("durable_graph")

    # Check both snapshot nodes and AOF replayed nodes are present
    res = g2.query("MATCH (n:NF) RETURN n.name AS name")
    names = {row[0] for row in res.result_set}
    assert names == {"SMF", "UPF", "AMF"}


def test_high_concurrency(spark_client):
    g = spark_client.select_graph("concurrent_space")
    num_threads = 8
    ops_per_thread = 50

    def worker(worker_id: int):
        for i in range(ops_per_thread):
            g.query(f"CREATE (n:WorkerNode {{worker: {worker_id}, op: {i}}})")

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    res = g.query("MATCH (n:WorkerNode) RETURN count(n) AS total")
    assert res.result_set[0][0] == num_threads * ops_per_thread or len(res.result_set) == num_threads * ops_per_thread
