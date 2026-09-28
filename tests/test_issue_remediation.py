"""Regression tests for storage, query, and rollback issue remediation."""
import json
import os

import pytest

from sparkdb.algorithms import pathfinding
from sparkdb.core.engine import GraphSpace
from sparkdb.core.matrix_store import MatrixStore
from sparkdb.core.property_store import DiskPropertyStore
from sparkdb.core.vector_store import HNSWLIB_AVAILABLE, VectorStore
from sparkdb.cypher.executor import CypherExecutor
from sparkdb.cypher.parser import substitute_params


@pytest.fixture(params=["memory", "hybrid"])
def graph_space(request, tmp_path):
    graph = GraphSpace(
        "regression",
        storage_dir=str(tmp_path),
        storage_mode=request.param,
    )
    yield graph
    graph.persistence.close()
    close = getattr(graph.property_store, "close", None)
    if close:
        close()


def test_disk_property_index_updates_remove_stale_values(tmp_path):
    store = DiskPropertyStore(os.path.join(tmp_path, "properties.db"))
    try:
        store.set_node_properties(1, {"status": "inactive"})
        store.set_node_properties(1, {"status": "active"})
        store.set_nodes_properties_batch({2: {"status": "inactive"}})
        store.set_nodes_properties_batch({2: {"status": "active"}})

        assert store.find_nodes_by_property("status", "inactive") == set()
        assert store.find_nodes_by_property("status", "active") == {1, 2}
        assert store._get_conn() is store._get_conn()
    finally:
        store.close()


def test_grouped_where_and_parameter_values_are_literal(graph_space):
    executor = CypherExecutor(graph_space)
    executor.execute(
        "CREATE (a:N {age: 40, role: 'User', status: 'active'}), "
        "(b:N {age: 20, role: 'Admin', status: 'inactive'})"
    )

    grouped = executor.execute(
        "MATCH (n:N) WHERE (n.age > 30 OR n.role = 'Admin') "
        "AND n.status = 'active' RETURN n.role"
    )
    assert grouped.result_set == [["User"]]

    injection_value = "nobody' OR true"
    parameterized = executor.execute(
        "MATCH (n:N) WHERE n.role = $role RETURN n.role",
        params={"role": injection_value},
    )
    assert parameterized.result_set == []
    trailing_slash_path = "C:" + chr(92) + "Program Files" + chr(92)
    assert substitute_params("RETURN $path", {"path": trailing_slash_path}) == (
        "RETURN " + json.dumps(trailing_slash_path)
    )


def test_transaction_rollback_preserves_sparse_ids_and_properties(graph_space):
    graph = graph_space
    first = graph.create_node(properties={"name": "first"})
    deleted = graph.create_node(properties={"name": "deleted"})
    last = graph.create_node(properties={"name": "last"})
    graph.create_edge(first, "LINK", last, properties={"weight": 1})
    graph.delete_node(deleted)

    with pytest.raises(RuntimeError, match="abort"):
        with graph.transaction():
            graph.set_node_properties(last, {"temporary": True})
            graph.create_node(properties={"temporary": True})
            raise RuntimeError("abort")

    assert graph.matrix_store.get_active_nodes() == {first, last}
    assert graph.matrix_store.get_csr("LINK").indices.tolist() == [last]
    assert graph.property_store.get_node_properties(last) == {"name": "last"}

    recycled_id = graph.create_node(properties={"name": "recycled"})
    assert recycled_id == deleted
    assert graph.property_store.get_node_properties(recycled_id) == {"name": "recycled"}


def test_checkpoint_preserves_sparse_node_ids(graph_space, tmp_path):
    graph = graph_space
    first = graph.create_node(labels=["N"], properties={"name": "first"})
    deleted = graph.create_node(labels=["N"], properties={"name": "deleted"})
    last = graph.create_node(labels=["N"], properties={"name": "last"})
    graph.create_edge(first, "LINK", last)
    graph.delete_node(deleted)

    snapshot_path = graph.checkpoint()
    with open(snapshot_path, encoding="utf-8") as snapshot_file:
        snapshot = json.load(snapshot_file)
    assert [node["id"] for node in snapshot["nodes"]] == [first, last]

    restored = GraphSpace(
        "regression",
        storage_dir=str(tmp_path),
        storage_mode=graph.storage_mode,
    )
    try:
        assert restored.matrix_store.get_active_nodes() == {first, last}
        assert restored.matrix_store.get_csr("LINK").indices.tolist() == [last]
        assert restored.matrix_store.add_node() == deleted
    finally:
        restored.persistence.close()
        close = getattr(restored.property_store, "close", None)
        if close:
            close()


def test_aof_replays_property_removals_labels_and_edge_property_cleanup(graph_space, tmp_path):
    graph = graph_space
    node = graph.create_node(labels=["Before"], properties={"remove_me": True})
    other = graph.create_node()
    graph.create_edge(node, "LINK", other, properties={"old": True})
    graph.remove_node_property(node, "remove_me")
    graph.add_node_label(node, "Added")
    graph.remove_node_label(node, "Before")
    graph.delete_edge(node, "LINK", other)
    graph.create_edge(node, "LINK", other)
    graph.persistence.close()

    restored = GraphSpace(
        "regression",
        storage_dir=str(tmp_path),
        storage_mode=graph.storage_mode,
    )
    try:
        assert "remove_me" not in restored.property_store.get_node_properties(node)
        assert restored.matrix_store.node_to_labels[node] == {"Added"}
        assert restored.property_store.get_edge_properties(node, "LINK", other) == {}
    finally:
        restored.persistence.close()
        close = getattr(restored.property_store, "close", None)
        if close:
            close()


def test_path_expansion_enforces_hop_and_path_budgets(monkeypatch):
    store = MatrixStore()
    nodes = [store.add_node() for _ in range(4)]
    for destination in nodes[1:]:
        store.add_edge(nodes[0], "R", destination)

    with pytest.raises(ValueError, match="maximum of 6 hops"):
        pathfinding.multi_hop_paths(store, [nodes[0]], ["R"] * 7)

    monkeypatch.setattr(pathfinding, "DEFAULT_MAX_PATHS", 2)
    with pytest.raises(ValueError, match="maximum of 2 paths"):
        pathfinding.multi_hop_paths(store, [nodes[0]], ["R"])
    assert len(pathfinding.multi_hop_paths(store, [nodes[0]], ["R"], max_paths=2)) == 2


@pytest.mark.skipif(not HNSWLIB_AVAILABLE, reason="hnswlib is not installed")
def test_hnsw_vector_store_can_reuse_deleted_node_id():
    store = VectorStore(dimension=2)
    store.add_node_vector(4, [1.0, 0.0])
    store.delete_node_vector(4)
    store.add_node_vector(4, [0.0, 1.0])

    assert store.query_nearest_nodes([0.0, 1.0], top_k=1)[0][0] == 4
    assert store.get_vector(4).tolist() == [0.0, 1.0]