"""Tests for FalkorDB Advanced Parity Features in SparkDB."""
import shutil
import pytest
from sparkdb import SparkDB


@pytest.fixture
def parity_db(tmp_path):
    storage_dir = str(tmp_path / "sparkdb_parity")
    db = SparkDB(storage_dir=storage_dir)
    yield db
    shutil.rmtree(storage_dir, ignore_errors=True)


def test_parameterized_queries(parity_db):
    project = parity_db.select_project("test_params")
    project.query(
        "CREATE (n:Device {id: $id, vendor: $vendor, ports: $ports, active: $active})",
        params={"id": "dev_01", "vendor": "Cisco", "ports": 48, "active": True},
    )

    res = project.query(
        "MATCH (n:Device {id: $target_id}) RETURN n.vendor, n.ports, n.active",
        params={"target_id": "dev_01"},
    )
    assert len(res.result_set) == 1
    row = res.result_set[0]
    assert row == ["Cisco", 48, True]

    res2 = project.query(
        "MATCH (n:Device) WHERE n.ports >= $min_ports RETURN n.id",
        params={"min_ports": 24},
    )
    assert len(res2.result_set) == 1
    assert res2.result_set[0][0] == "dev_01"


def test_order_by_skip_limit(parity_db):
    project = parity_db.select_project("test_sort_page")
    project.query("""
    CREATE (a:Server {name: 'srv-01', load: 10}),
           (b:Server {name: 'srv-02', load: 85}),
           (c:Server {name: 'srv-03', load: 45}),
           (d:Server {name: 'srv-04', load: 95}),
           (e:Server {name: 'srv-05', load: 30})
    """)

    res_asc = project.query("MATCH (s:Server) RETURN s.name, s.load ORDER BY s.load ASC")
    loads_asc = [r[1] for r in res_asc.result_set]
    assert loads_asc == [10, 30, 45, 85, 95]

    res_desc = project.query("MATCH (s:Server) RETURN s.name, s.load ORDER BY s.load DESC")
    loads_desc = [r[1] for r in res_desc.result_set]
    assert loads_desc == [95, 85, 45, 30, 10]

    res_page = project.query("MATCH (s:Server) RETURN s.name, s.load ORDER BY s.load ASC SKIP 2 LIMIT 2")
    page_loads = [r[1] for r in res_page.result_set]
    assert page_loads == [45, 85]


def test_where_clause_operators(parity_db):
    project = parity_db.select_project("test_where_ops")
    project.query("""
    CREATE (u1:User {username: 'alice_admin', age: 32, department: 'DevOps', tags: 'core'}),
           (u2:User {username: 'bob_operator', age: 24, department: 'Support', tags: 'tier1'}),
           (u3:User {username: 'charlie_admin', age: 45, department: 'DevOps', tags: 'lead'}),
           (u4:User {username: 'david_guest', age: 19, department: 'External'})
    """)

    res1 = project.query("MATCH (u:User) WHERE u.age >= 30 AND u.department = 'DevOps' RETURN u.username")
    names1 = sorted([r[0] for r in res1.result_set])
    assert names1 == ["alice_admin", "charlie_admin"]

    res2 = project.query("MATCH (u:User) WHERE u.username CONTAINS 'admin' RETURN u.username")
    names2 = sorted([r[0] for r in res2.result_set])
    assert names2 == ["alice_admin", "charlie_admin"]

    res3 = project.query("MATCH (u:User) WHERE u.username STARTS WITH 'bob' RETURN u.username")
    assert [r[0] for r in res3.result_set] == ["bob_operator"]

    res4 = project.query("MATCH (u:User) WHERE u.username ENDS WITH 'guest' RETURN u.username")
    assert [r[0] for r in res4.result_set] == ["david_guest"]

    res5 = project.query("MATCH (u:User) WHERE u.department IN ['Support', 'External'] RETURN u.username")
    names5 = sorted([r[0] for r in res5.result_set])
    assert names5 == ["bob_operator", "david_guest"]

    res6 = project.query("MATCH (u:User) WHERE u.tags IS NULL RETURN u.username")
    assert [r[0] for r in res6.result_set] == ["david_guest"]

    res7 = project.query("MATCH (u:User) WHERE u.tags IS NOT NULL RETURN u.username")
    assert len(res7.result_set) == 3

    res8 = project.query("MATCH (u:User) WHERE u.age < 20 OR u.age > 40 RETURN u.username")
    names8 = sorted([r[0] for r in res8.result_set])
    assert names8 == ["charlie_admin", "david_guest"]


def test_analytical_aggregations(parity_db):
    project = parity_db.select_project("test_aggs")
    project.query("""
    CREATE (p1:Product {category: 'Compute', price: 100, stock: 10}),
           (p2:Product {category: 'Compute', price: 200, stock: 5}),
           (p3:Product {category: 'Storage', price: 50, stock: 50}),
           (p4:Product {category: 'Storage', price: 150, stock: 20})
    """)

    res_glob = project.query("MATCH (p:Product) RETURN count(p) AS cnt, sum(p.price) AS total_price, avg(p.price) AS avg_price, min(p.price) AS min_price, max(p.price) AS max_price, collect(p.price) AS prices")
    assert len(res_glob.result_set) == 1
    row = res_glob.result_set[0]
    assert row[0] == 4
    assert row[1] == 500.0
    assert row[2] == 125.0
    assert row[3] == 50.0
    assert row[4] == 200.0
    assert sorted(row[5]) == [50, 100, 150, 200]

    res_grp = project.query("MATCH (p:Product) RETURN p.category AS category, count(p) AS cnt, sum(p.stock) AS total_stock ORDER BY category ASC")
    assert len(res_grp.result_set) == 2
    cat_compute = res_grp.result_set[0]
    assert cat_compute[0] == "Compute"
    assert cat_compute[1] == 2
    assert cat_compute[2] == 15.0


def test_merge_upsert(parity_db):
    project = parity_db.select_project("test_merge")

    res1 = project.query("MERGE (c:Customer {email: 'customer@example.com'}) ON CREATE SET c.tier = 'PLATINUM', c.created_at = 100 RETURN c.email, c.tier")
    assert res1.nodes_created == 1
    assert res1.result_set[0] == ["customer@example.com", "PLATINUM"]

    res2 = project.query("MERGE (c:Customer {email: 'customer@example.com'}) ON CREATE SET c.tier = 'GOLD' ON MATCH SET c.login_count = 1 RETURN c.email, c.login_count")
    assert res2.nodes_created == 0
    assert res2.properties_set == 1
    assert res2.result_set[0] == ["customer@example.com", 1]

    verify_res = project.query("MATCH (c:Customer {email: 'customer@example.com'}) RETURN c.tier, c.login_count, c.created_at")
    assert verify_res.result_set[0] == ["PLATINUM", 1, 100]


def test_explain_and_profile(parity_db):
    project = parity_db.select_project("test_plan")
    project.query("CREATE (n:Service {name: 'Auth'})")

    exp_res = project.query("EXPLAIN MATCH (s:Service) WHERE s.name = 'Auth' RETURN s.name")
    assert exp_res.header == ["operator", "details"]
    assert len(exp_res.result_set) > 0

    prof_res = project.query("PROFILE MATCH (s:Service) WHERE s.name = 'Auth' RETURN s.name")
    assert prof_res.header == ["operator", "records", "execution_time", "details"]
    assert len(prof_res.result_set) > 0


def test_index_lifecycle_and_slowlog(parity_db):
    project = parity_db.select_project("test_idx_slowlog")
    project.query("CREATE (n:Asset {tag: 'A100', serial: 12345})")

    idx_res = project.query("CREATE INDEX FOR (a:Asset) ON (a.tag)")
    assert idx_res.indices_created == 1

    list_res = project.query("CALL db.indexes()")
    assert list_res.header == ["label", "properties", "type"]
    props = [r[1][0] for r in list_res.result_set if r[2] == "RANGE"]
    assert "tag" in props

    drop_res = project.query("DROP INDEX FOR (a:Asset) ON (a.tag)")
    assert drop_res.indices_deleted == 1

    slowlog_res = project.query("CALL db.slowlog()")
    assert slowlog_res.header == ["timestamp", "query", "duration_ms", "results_count"]
    assert len(slowlog_res.result_set) > 0
