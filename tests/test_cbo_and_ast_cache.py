"""Tests for Cypher Query Optimizer (CBO) & AST Caching in SparkDB."""
import concurrent.futures
import pytest
from sparkdb import SparkDB
from sparkdb.cypher.parser import CypherParser, LRUCache, _COMPILED_PLAN_CACHE, _TEMPLATE_PLAN_CACHE


def test_lru_cache_capacity_and_eviction():
    """Verify thread-safe LRUCache enforces minimum capacity of 2000 and evicts LRU entries."""
    cache = LRUCache(capacity=2048)
    assert cache.capacity >= 2000

    # Insert 2500 items
    for i in range(2500):
        cache.set(f"key_{i}", f"val_{i}")

    assert len(cache) == 2048
    # Earliest items (0..451) should have been evicted
    assert "key_0" not in cache
    assert "key_451" not in cache
    # Recent items should still be in cache
    assert "key_2499" in cache
    assert cache.get("key_2499") == "val_2499"


def test_lru_cache_thread_safety():
    """Verify concurrent reads and writes to LRUCache across multiple threads."""
    cache = LRUCache(capacity=2048)

    def worker(worker_id: int):
        for i in range(200):
            k = f"key_{worker_id}_{i}"
            cache.set(k, i)
            val = cache.get(k)
            assert val == i

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, w) for w in range(8)]
        for f in concurrent.futures.as_completed(futures):
            f.result()

    assert len(cache) > 0


def test_ast_template_caching_and_literal_normalization():
    """Verify repeated query structures (literals vs $param) reuse template ASTs and skip parsing."""
    CypherParser.clear_cache()

    q1 = "MATCH (g:Gateway {id: 101, name: 'GW1'}) RETURN g"
    q2 = "MATCH (g:Gateway {id: 102, name: 'GW2'}) RETURN g"
    q3 = "MATCH (g:Gateway {id: $id, name: $name}) RETURN g"

    # Parse q1
    stmt1 = CypherParser.parse(q1)
    assert stmt1.type == "MATCH"
    assert stmt1.details["nodes"][0]["properties"] == {"id": 101, "name": "GW1"}

    stats_after_1 = CypherParser.get_cache_stats()
    template_hits_1 = stats_after_1["template_plan_cache"]["hits"]

    # Parse q2: structure is identical, only literals changed
    stmt2 = CypherParser.parse(q2)
    assert stmt2.type == "MATCH"
    assert stmt2.details["nodes"][0]["properties"] == {"id": 102, "name": "GW2"}

    stats_after_2 = CypherParser.get_cache_stats()
    # Template cache should have been hit for q2
    assert stats_after_2["template_plan_cache"]["hits"] > template_hits_1

    # Parse q3 with parameters
    stmt3 = CypherParser.parse(q3, params={"id": 103, "name": "GW3"})
    assert stmt3.type == "MATCH"
    assert stmt3.details["nodes"][0]["properties"] == {"id": 103, "name": "GW3"}

    # Repeated identical query hits compiled plan cache directly
    stmt1_repeat = CypherParser.parse(q1)
    assert stmt1_repeat.details["nodes"][0]["properties"] == {"id": 101, "name": "GW1"}
    stats_after_repeat = CypherParser.get_cache_stats()
    assert stats_after_repeat["compiled_plan_cache"]["hits"] > 0


def test_graph_space_label_cardinality_tracking(tmp_path):
    """Verify GraphSpace tracks label_counts incrementally across all mutations."""
    db = SparkDB(storage_dir=str(tmp_path))
    project = db.select_project("stats_proj")
    space = project._space

    assert space.label_counts == {}
    assert space.get_label_count("User") == 0

    # 1. create_node
    u1 = space.create_node(labels=["User", "Admin"], properties={"name": "Alice"})
    assert space.label_counts["User"] == 1
    assert space.label_counts["Admin"] == 1

    u2 = space.create_node(labels=["User"], properties={"name": "Bob"})
    assert space.label_counts["User"] == 2
    assert space.label_counts["Admin"] == 1

    # 2. create_batch
    bulk_nodes = [
        {"labels": ["Device"], "properties": {"mac": "00:11"}},
        {"labels": ["Device", "Sensor"], "properties": {"mac": "00:22"}},
        {"labels": ["User"], "properties": {"name": "Charlie"}},
    ]
    space.create_batch(nodes=bulk_nodes, edges=[])
    assert space.label_counts["User"] == 3
    assert space.label_counts["Device"] == 2
    assert space.label_counts["Sensor"] == 1

    # 3. add_node_label / remove_node_label
    space.add_node_label(u2, "Manager")
    assert space.label_counts["Manager"] == 1
    space.remove_node_label(u2, "Manager")
    assert space.get_label_count("Manager") == 0

    # 4. delete_node
    space.delete_node(u1)
    assert space.label_counts["User"] == 2
    assert space.get_label_count("Admin") == 0


def test_cbo_1hop_backward_traversal(tmp_path):
    """Verify CBO executes backward traversal when candidate(target) < candidate(source) / 3."""
    db = SparkDB(storage_dir=str(tmp_path))
    project = db.select_project("cbo_1hop")

    # Create 30 Service nodes, but only 2 Database nodes
    services = []
    for i in range(30):
        nid = project._space.create_node(labels=["Service"], properties={"sid": i})
        services.append(nid)

    db1 = project._space.create_node(labels=["Database"], properties={"dbname": "Postgres", "cluster_id": 1})
    db2 = project._space.create_node(labels=["Database"], properties={"dbname": "Redis", "cluster_id": 2})

    # Services 0..4 connect to db1, Services 5..9 connect to db2
    for s in services[:5]:
        project._space.create_edge(src=s, rel="CONNECTS_TO", dst=db1)
    for s in services[5:10]:
        project._space.create_edge(src=s, rel="CONNECTS_TO", dst=db2)

    # Label counts
    assert project._space.label_counts["Service"] == 30
    assert project._space.label_counts["Database"] == 2

    # Query targeting db1 specifically: len(Database)=1 (filtered by cluster_id) vs len(Service)=30
    # CBO condition: len(Database candidates) < len(Service candidates) / 3 (1 < 10)
    res = project.query("MATCH (s:Service)-[:CONNECTS_TO]->(d:Database {cluster_id: 1}) RETURN s.sid, d.dbname")
    assert len(res.result_set) == 5
    sids = sorted([row[0] for row in res.result_set])
    assert sids == [0, 1, 2, 3, 4]
    for row in res.result_set:
        assert row[1] == "Postgres"


def test_cbo_2hop_backward_traversal(tmp_path):
    """Verify CBO executes 2-hop backward traversal when target candidate set is significantly smaller."""
    db = SparkDB(storage_dir=str(tmp_path))
    project = db.select_project("cbo_2hop")

    # 40 Clients -> 10 Proxies -> 1 Target Core
    clients = [project._space.create_node(labels=["Client"], properties={"cid": i}) for i in range(40)]
    proxies = [project._space.create_node(labels=["Proxy"], properties={"pid": i}) for i in range(10)]
    core1 = project._space.create_node(labels=["Core"], properties={"core_id": 99, "name": "MainCore"})
    core2 = project._space.create_node(labels=["Core"], properties={"core_id": 100, "name": "BackupCore"})

    # Wire 5 clients to proxy 0, which connects to core1
    for c in clients[:5]:
        project._space.create_edge(src=c, rel="ROUTES_TO", dst=proxies[0])
    project._space.create_edge(src=proxies[0], rel="UPSTREAM", dst=core1)

    # Wire other clients to other proxies
    for c in clients[5:10]:
        project._space.create_edge(src=c, rel="ROUTES_TO", dst=proxies[1])
    project._space.create_edge(src=proxies[1], rel="UPSTREAM", dst=core2)

    # 2-hop query targeting core_id: 99
    # Candidates for Core=1, Candidates for Client=40 => CBO backward traversal
    res = project.query("MATCH (c:Client)-[:ROUTES_TO]->(p:Proxy)-[:UPSTREAM]->(k:Core {core_id: 99}) RETURN c.cid, p.pid, k.name")
    assert len(res.result_set) == 5
    for row in res.result_set:
        assert row[1] == 0
        assert row[2] == "MainCore"
    assert sorted([r[0] for r in res.result_set]) == [0, 1, 2, 3, 4]


def test_cbo_empty_candidate_pruning(tmp_path):
    """Verify CBO immediately prunes search space when destination or intermediate candidates are empty."""
    db = SparkDB(storage_dir=str(tmp_path))
    project = db.select_project("cbo_prune")

    for i in range(50):
        project._space.create_node(labels=["NodeA"], properties={"val": i})

    # Destination label has 0 nodes
    res = project.query("MATCH (a:NodeA)-[:REL]->(b:NonExistentLabel) RETURN a, b")
    assert len(res.result_set) == 0

    # Intermediate label in 2-hop has 0 nodes
    res2 = project.query("MATCH (a:NodeA)-[:R1]->(m:NonExistentMiddle)-[:R2]->(b:NodeA) RETURN a, b")
    assert len(res2.result_set) == 0
