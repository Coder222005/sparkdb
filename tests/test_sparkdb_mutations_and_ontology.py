"""Tests for SparkDB Mutations (SET, REMOVE, DELETE) and Ontology Metamodel Inspection."""
import os
import shutil
import tempfile
import pytest

from sparkdb import SparkDB


@pytest.fixture
def temp_db():
    temp_dir = tempfile.mkdtemp(prefix="sparkdb_mutations_test_")
    db = SparkDB(storage_dir=temp_dir)
    yield db
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_set_and_update_properties(temp_db):
    proj = temp_db.select_project("graph_mutations")

    # 1. CREATE node and edge
    proj.query("""
    CREATE (amf:NetworkFunction {name: 'AMF', role: 'ControlPlane', version: 1}),
           (smf:NetworkFunction {name: 'SMF', role: 'ControlPlane'}),
           (smf)-[:CONNECTS_TO {latency_ms: 10}]->(amf)
    """)

    # 2. SET update node property & add new property
    res = proj.query("MATCH (a:NetworkFunction {name: 'AMF'}) SET a.version = 2, a.status = 'active'")
    assert res.properties_set == 2

    # Check updated property
    check_res = proj.query("MATCH (a:NetworkFunction {name: 'AMF'}) RETURN a.version, a.status")
    assert check_res.result_set == [[2, "active"]]

    # 3. SET update edge property
    edge_set_res = proj.query("MATCH (s)-[r:CONNECTS_TO]->(d) SET r.latency_ms = 2.5, r.protocol = 'HTTP2'")
    assert edge_set_res.properties_set == 2

    edge_check = proj.query("MATCH (s)-[r:CONNECTS_TO]->(d) RETURN r.latency_ms, r.protocol")
    assert edge_check.result_set == [[2.5, "HTTP2"]]


def test_remove_property_and_label(temp_db):
    proj = temp_db.select_project("graph_removals")

    proj.query("CREATE (n:NF {name: 'UPF', ip: '10.0.0.1', temp_flag: true})")

    # REMOVE property
    proj.query("MATCH (n:NF {name: 'UPF'}) REMOVE n.temp_flag")

    check = proj.query("MATCH (n:NF {name: 'UPF'}) RETURN n.name, n.ip, n.temp_flag")
    assert check.result_set == [["UPF", "10.0.0.1", None]]


def test_delete_entity_and_relationship(temp_db):
    proj = temp_db.select_project("graph_deletions")

    proj.query("""
    CREATE (a:Service {name: 'Auth'}),
           (b:Service {name: 'Billing'}),
           (c:Service {name: 'Core'}),
           (a)-[r1:CALLS]->(b),
           (b)-[r2:CALLS]->(c)
    """)

    # 1. DELETE edge only
    del_rel_res = proj.query("MATCH (a:Service {name: 'Auth'})-[r:CALLS]->(b:Service {name: 'Billing'}) DELETE r")
    assert del_rel_res.relationships_deleted == 1

    # Verify edge deleted but nodes still exist
    check_nodes = proj.query("MATCH (n:Service) RETURN count(n) AS cnt")
    assert check_nodes.result_set == [[3]]
    check_edges = proj.query("MATCH (a)-[:CALLS]->(b) RETURN a.name, b.name")
    assert check_edges.result_set == [["Billing", "Core"]]

    # 2. DELETE node
    del_node_res = proj.query("MATCH (a:Service {name: 'Auth'}) DELETE a")
    assert del_node_res.nodes_deleted == 1
    assert proj.query("MATCH (n:Service) RETURN count(n)").result_set == [[2]]


def test_ontology_schema_extraction(temp_db):
    proj = temp_db.select_project("graph_ontology")

    # Insert sample instances
    proj.query("""
    CREATE (amf:NetworkFunction {name: 'AMF', role: 'ControlPlane', domain: '3GPP'}),
           (upf:NetworkFunction {name: 'UPF', role: 'UserPlane'}),
           (n4:StandardInterface {name: 'N4', protocol: 'PFCP'}),
           (amf)-[:CONNECTS_VIA {latency_ms: 1.0}]->(n4),
           (n4)-[:CONNECTS_TO]->(upf)
    """)

    # 1. Python SDK get_ontology()
    onto = proj.get_ontology()

    # Structural verification: No data instances, pure metamodel
    assert sorted(onto["node_labels"]) == ["NetworkFunction", "StandardInterface"]
    assert "name" in onto["property_keys_by_label"]["NetworkFunction"]
    assert "role" in onto["property_keys_by_label"]["NetworkFunction"]
    assert "protocol" in onto["property_keys_by_label"]["StandardInterface"]

    assert sorted(onto["relationship_types"]) == ["CONNECTS_TO", "CONNECTS_VIA"]
    rel_tuples = {(r["src_label"], r["rel_type"], r["dst_label"]) for r in onto["relationship_schema"]}
    assert ("NetworkFunction", "CONNECTS_VIA", "StandardInterface") in rel_tuples
    assert ("StandardInterface", "CONNECTS_TO", "NetworkFunction") in rel_tuples

    # 2. Cypher CALL db.labels()
    lbl_res = proj.query("CALL db.labels()")
    assert [row[0] for row in lbl_res.result_set] == ["NetworkFunction", "StandardInterface"]

    # 3. Cypher CALL db.relationshipTypes()
    rel_res = proj.query("CALL db.relationshipTypes()")
    assert [row[0] for row in rel_res.result_set] == ["CONNECTS_TO", "CONNECTS_VIA"]

    # 4. Cypher CALL db.schema()
    schema_res = proj.query("CALL db.schema()")
    assert schema_res.header == ["source_label", "relationship_type", "target_label"]
    assert len(schema_res.result_set) == 2


def test_drop_all_projects(temp_db):
    temp_db.select_project("p1").query("CREATE (n:Test)")
    temp_db.select_project("p2").query("CREATE (n:Test)")
    temp_db.select_project("p3").query("CREATE (n:Test)")

    assert len(temp_db.list_projects()) >= 3

    dropped = temp_db.drop_all_projects()
    assert "p1" in dropped and "p2" in dropped and "p3" in dropped
    assert temp_db.list_projects() == []
