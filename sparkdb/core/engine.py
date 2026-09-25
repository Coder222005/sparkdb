"""Core Engine Orchestrator for SparkDB.

Coordinates MatrixStore, PropertyStore / DiskPropertyStore, VectorStore,
FulltextStore, and PersistenceEngine for multi-tenant, durable, high-performance graph operations.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from .matrix_store import MatrixStore
from .property_store import PropertyStore, DiskPropertyStore
from .vector_store import VectorStore
from .fulltext_store import FulltextStore
from .persistence import PersistenceEngine

logger = logging.getLogger(__name__)


class GraphSpace:
    """An isolated graph/project workspace in SparkDB."""

    def __init__(
        self,
        name: str,
        vector_dim: int = 256,
        storage_dir: str = "./data/sparkdb",
        storage_mode: str = "memory",
        lru_cache_size: int = 10000,
    ):
        self._lock = threading.RLock()
        self.name = name
        self.vector_dim = vector_dim
        self.storage_dir = storage_dir
        self.storage_mode = storage_mode

        self.matrix_store = MatrixStore()
        if storage_mode == "hybrid":
            db_path = os.path.join(storage_dir, name, "properties.db")
            self.property_store = DiskPropertyStore(db_path=db_path, lru_cache_size=lru_cache_size)
        else:
            self.property_store = PropertyStore()

        self.vector_store = VectorStore(dimension=self.vector_dim)
        self.fulltext_store = FulltextStore()
        self.persistence = PersistenceEngine(graph_name=name, storage_dir=storage_dir)

        self._schema: Dict[str, Any] = {}
        self._slowlog: List[Dict[str, Any]] = []
        self.label_counts: Dict[str, int] = {}
        self._auto_restore()

    def _auto_restore(self) -> None:
        """Automatically restore state from snapshot and replay WAL if available."""
        snapshot = self.persistence.load_snapshot()
        if snapshot:
            self._restore_from_dict(snapshot)
            logger.info(f"Restored graph '{self.name}' from snapshot: {self.matrix_store.node_count} nodes")

        mutations = self.persistence.replay_aof()
        if mutations:
            for m in mutations:
                cmd = m.get("cmd")
                p = m.get("params", {})
                if cmd == "create_node":
                    self.create_node(p.get("labels"), p.get("properties"), p.get("embedding"), log_aof=False)
                elif cmd == "create_edge":
                    self.create_edge(p.get("src"), p.get("rel"), p.get("dst"), p.get("weight", 1.0), p.get("properties"), log_aof=False)
                elif cmd == "delete_node":
                    self.delete_node(p.get("node_id"), log_aof=False)
                elif cmd == "delete_edge":
                    self.delete_edge(p.get("src"), p.get("rel"), p.get("dst"), log_aof=False)
                elif cmd == "create_batch":
                    for n in p.get("nodes", []):
                        self.create_node(n.get("labels"), n.get("properties"), n.get("embedding"), log_aof=False)
                    for e in p.get("edges", []):
                        self.create_edge(e.get("src"), e.get("rel"), e.get("dst"), e.get("weight", 1.0), e.get("properties"), log_aof=False)
            logger.info(f"Replayed {len(mutations)} WAL mutations for graph '{self.name}'")

    def _restore_from_dict(self, data: Dict[str, Any]) -> None:
        """Populate stores from snapshot dictionary."""
        with self._lock:
            # Nodes
            for n_entry in data.get("nodes", []):
                lbls = n_entry.get("labels", [])
                nid = self.matrix_store.add_node(labels=lbls)
                for lbl in lbls:
                    self.label_counts[lbl] = self.label_counts.get(lbl, 0) + 1
                props = n_entry.get("properties", {})
                self.property_store.set_node_properties(nid, props)
                if "embedding" in n_entry and n_entry["embedding"] is not None:
                    self.vector_store.add_node_vector(nid, n_entry["embedding"])
                if "text" in props:
                    self.fulltext_store.index_node_text(nid, str(props["text"]))

            # Edges
            for e_entry in data.get("edges", []):
                src = e_entry["src"]
                rel = e_entry["rel"]
                dst = e_entry["dst"]
                w = e_entry.get("weight", 1.0)
                props = e_entry.get("properties", {})
                self.matrix_store.add_edge(src, rel, dst, weight=w)
                if props:
                    self.property_store.set_edge_properties(src, rel, dst, props)

    def batch_get_nodes_properties(self, node_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Batch fetch node properties via underlying property store."""
        return self.property_store.batch_get_nodes_properties(node_ids)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize complete state into dictionary for snapshotting."""
        with self._lock:
            nodes_data = []
            active_nodes = self.matrix_store.get_active_nodes()
            all_props = self.property_store.batch_get_nodes_properties(active_nodes)
            for nid in active_nodes:
                props = all_props.get(nid, {})
                lbls = list(self.matrix_store.node_to_labels.get(nid, set()))
                vec = self.vector_store._vectors.get(nid)
                nodes_data.append({
                    "id": nid,
                    "labels": lbls,
                    "properties": props,
                    "embedding": vec.tolist() if vec is not None else None,
                })

            edges_data = []
            for rel in self.matrix_store.get_relationship_types():
                mat = self.matrix_store._rel_matrices[rel]
                for (src, dst), weight in mat.items():
                    props = self.property_store.get_edge_properties(src, rel, dst)
                    edges_data.append({
                        "src": src,
                        "rel": rel,
                        "dst": dst,
                        "weight": float(weight),
                        "properties": props,
                    })

            return {
                "graph_name": self.name,
                "node_count": self.matrix_store.node_count,
                "nodes": nodes_data,
                "edges": edges_data,
            }

    def checkpoint(self) -> str:
        """Trigger an atomic disk snapshot."""
        state = self.to_dict()
        return self.persistence.save_snapshot(state)

    def load_schema(self, schema_dict_or_path: Dict[str, Any] | str) -> None:
        """Enforce ontology schema constraints."""
        with self._lock:
            if isinstance(schema_dict_or_path, str):
                with open(schema_dict_or_path, "r", encoding="utf-8") as f:
                    self._schema = json.load(f)
            else:
                self._schema = schema_dict_or_path

    def get_ontology(self) -> Dict[str, Any]:
        """Extract the structural ontology / metamodel schema of this project (without data instances)."""
        with self._lock:
            labels = sorted(list(self.matrix_store.labels.keys()))

            props_by_label: Dict[str, Set[str]] = {lbl: set() for lbl in labels}
            active_nodes = self.matrix_store.get_active_nodes()
            all_props = self.property_store.batch_get_nodes_properties(active_nodes)
            for nid in active_nodes:
                node_labels = self.matrix_store.node_to_labels.get(nid, set())
                node_props = all_props.get(nid, {})
                for lbl in node_labels:
                    props_by_label[lbl].update(node_props.keys())

            rel_types = sorted(self.matrix_store.get_relationship_types())
            rel_signatures = set()
            rel_props_by_type: Dict[str, Set[str]] = {rel: set() for rel in rel_types}

            for rel in rel_types:
                mat = self.matrix_store._rel_matrices.get(rel, {})
                for (src, dst) in mat.keys():
                    src_labels = sorted(list(self.matrix_store.node_to_labels.get(src, ["Entity"])))
                    dst_labels = sorted(list(self.matrix_store.node_to_labels.get(dst, ["Entity"])))
                    for sl in src_labels:
                        for dl in dst_labels:
                            rel_signatures.add((sl, rel, dl))

                    eprops = self.property_store.get_edge_properties(src, rel, dst)
                    rel_props_by_type[rel].update(eprops.keys())

            relationships_meta = [
                {"src_label": sl, "rel_type": rel, "dst_label": dl}
                for sl, rel, dl in sorted(list(rel_signatures))
            ]

            return {
                "project": self.name,
                "node_labels": labels,
                "property_keys_by_label": {k: sorted(list(v)) for k, v in props_by_label.items()},
                "relationship_types": rel_types,
                "relationship_schema": relationships_meta,
                "property_keys_by_relationship": {k: sorted(list(v)) for k, v in rel_props_by_type.items()},
            }

    def create_node(
        self,
        labels: Optional[List[str]] = None,
        properties: Optional[Dict[str, Any]] = None,
        embedding: Optional[List[float]] = None,
        log_aof: bool = True,
    ) -> int:
        """Create a node with labels, properties, and optional vector embedding."""
        with self._lock:
            nid = self.matrix_store.add_node(labels=labels)
            if labels:
                for lbl in labels:
                    self.label_counts[lbl] = self.label_counts.get(lbl, 0) + 1

            if properties:
                self.property_store.set_node_properties(nid, properties)
                for k in ["text", "description", "content"]:
                    if k in properties and isinstance(properties[k], str):
                        self.fulltext_store.index_node_text(nid, properties[k])

            if embedding is not None:
                self.vector_store.add_node_vector(nid, embedding)
                if properties is None:
                    properties = {}
                properties["has_embedding"] = True

            if log_aof:
                self.persistence.append_mutation("create_node", {
                    "labels": labels,
                    "properties": properties,
                    "embedding": embedding,
                })

            return nid

    def delete_node(self, node_id: int, log_aof: bool = True) -> bool:
        """Delete a node and its incident edges."""
        with self._lock:
            node_labels = list(self.matrix_store.node_to_labels.get(node_id, set()))
            success = self.matrix_store.delete_node(node_id)
            if success:
                for lbl in node_labels:
                    if lbl in self.label_counts:
                        self.label_counts[lbl] = max(0, self.label_counts[lbl] - 1)
                        if self.label_counts[lbl] == 0:
                            del self.label_counts[lbl]
                self.property_store.delete_node_properties(node_id)
                self.vector_store.delete_node_vector(node_id)
                if log_aof:
                    self.persistence.append_mutation("delete_node", {"node_id": node_id})
            return success

    def add_node_label(self, node_id: int, label: str) -> None:
        """Add a label to an active node and update cardinality statistics."""
        with self._lock:
            self.matrix_store.add_node_label(node_id, label)
            self.label_counts[label] = self.label_counts.get(label, 0) + 1

    def remove_node_label(self, node_id: int, label: str) -> None:
        """Remove a label from an active node and update cardinality statistics."""
        with self._lock:
            self.matrix_store.remove_node_label(node_id, label)
            if label in self.label_counts:
                self.label_counts[label] = max(0, self.label_counts[label] - 1)
                if self.label_counts[label] == 0:
                    del self.label_counts[label]

    def get_label_count(self, label: str) -> int:
        """Get the current count of nodes with the specified label."""
        with self._lock:
            return self.label_counts.get(label, 0)

    def get_reverse_adjacency(self, rel_type: str) -> Any:
        """Retrieve reverse (transposed) adjacency matrix for backward traversals."""
        return self.matrix_store.get_reverse_adjacency(rel_type)

    def create_batch(
        self,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        log_aof: bool = True,
    ) -> Tuple[List[int], int]:
        """High-performance bulk insertion for multiple nodes and edges in a single lock cycle."""
        with self._lock:
            node_ids = []
            node_props_batch = {}
            for n in nodes:
                lbls = n.get("labels", [])
                for lbl in lbls:
                    self.label_counts[lbl] = self.label_counts.get(lbl, 0) + 1
                props = n.get("properties", {})
                emb = props.pop("embedding", None) if isinstance(props, dict) else None
                nid = self.matrix_store.add_node(labels=lbls)
                node_ids.append(nid)
                if props:
                    node_props_batch[nid] = props
                if emb is not None:
                    self.vector_store.add_node_vector(nid, emb)
                for k in ["text", "description", "content"]:
                    if isinstance(props, dict) and k in props and isinstance(props[k], str):
                        self.fulltext_store.index_node_text(nid, props[k])

            if node_props_batch:
                if hasattr(self.property_store, "set_nodes_properties_batch"):
                    self.property_store.set_nodes_properties_batch(node_props_batch)
                else:
                    for nid, p in node_props_batch.items():
                        self.property_store.set_node_properties(nid, p)

            edge_props_batch = []
            for e in edges:
                src, rel, dst = e["src"], e["rel"], e["dst"]
                w = e.get("weight", 1.0)
                props = e.get("properties", {})
                self.matrix_store.add_edge(src, rel, dst, weight=w)
                if props:
                    edge_props_batch.append((src, rel, dst, props))

            if edge_props_batch:
                if hasattr(self.property_store, "set_edges_properties_batch"):
                    self.property_store.set_edges_properties_batch(edge_props_batch)
                else:
                    for src, rel, dst, p in edge_props_batch:
                        self.property_store.set_edge_properties(src, rel, dst, p)

            if log_aof:
                self.persistence.append_mutation("create_batch", {
                    "nodes": nodes,
                    "edges": edges,
                })

            return node_ids, len(edges)

    def create_edge(
        self,
        src: int,
        rel: str,
        dst: int,
        weight: float = 1.0,
        properties: Optional[Dict[str, Any]] = None,
        log_aof: bool = True,
    ) -> None:
        """Insert a directed edge into the sparse matrix."""
        with self._lock:
            self.matrix_store.add_edge(src, rel, dst, weight=weight)
            if properties:
                self.property_store.set_edge_properties(src, rel, dst, properties)

            if log_aof:
                self.persistence.append_mutation("create_edge", {
                    "src": src, "rel": rel, "dst": dst, "weight": weight, "properties": properties,
                })

    def delete_edge(self, src: int, rel: str, dst: int, log_aof: bool = True) -> bool:
        """Remove an edge."""
        with self._lock:
            success = self.matrix_store.delete_edge(src, rel, dst)
            if success:
                if hasattr(self.property_store, "edge_properties"):
                    self.property_store.edge_properties.pop((src, rel, dst), None)
                if log_aof:
                    self.persistence.append_mutation("delete_edge", {"src": src, "rel": rel, "dst": dst})
            return success

    def create_property_index(self, property_name: str) -> None:
        """Create a secondary property index."""
        self.property_store.create_node_property_index(property_name)

    def drop_property_index(self, property_name: str) -> bool:
        """Drop a secondary property index."""
        if hasattr(self.property_store, "drop_node_property_index"):
            return self.property_store.drop_node_property_index(property_name)
        return False

    def get_indexed_properties(self) -> List[str]:
        """List all indexed property names."""
        if hasattr(self.property_store, "get_indexed_properties"):
            return self.property_store.get_indexed_properties()
        return []

    def record_query_log(self, query: str, execution_time_ms: float, rows_count: int) -> None:
        """Record executed query to the slowlog / audit ring buffer."""
        with self._lock:
            self._slowlog.append({
                "query": query,
                "execution_time_ms": execution_time_ms,
                "timestamp": time.time(),
                "rows_count": rows_count,
            })
            if len(self._slowlog) > 200:
                self._slowlog.pop(0)

    def get_slowlog(self) -> List[Dict[str, Any]]:
        """Retrieve recent query execution logs."""
        with self._lock:
            return list(self._slowlog)


class SparkDB:
    """SparkDB Server Engine — Top-level database managing multi-tenant graph/project spaces."""

    def __init__(self, storage_dir: str = "./data/sparkdb", default_vector_dim: int = 256):
        self._lock = threading.RLock()
        self.storage_dir = os.path.abspath(storage_dir)
        os.makedirs(self.storage_dir, exist_ok=True)
        self.default_vector_dim = default_vector_dim
        self._graphs: Dict[str, GraphSpace] = {}

    def select_graph(
        self,
        graph_name: str,
        vector_dim: Optional[int] = None,
        storage_mode: str = "memory",
        lru_cache_size: int = 10000,
    ) -> GraphSpace:
        """Select an existing graph space or instantiate a new one."""
        with self._lock:
            if graph_name not in self._graphs:
                dim = vector_dim or self.default_vector_dim
                self._graphs[graph_name] = GraphSpace(
                    name=graph_name,
                    vector_dim=dim,
                    storage_dir=self.storage_dir,
                    storage_mode=storage_mode,
                    lru_cache_size=lru_cache_size,
                )
            return self._graphs[graph_name]

    select_project = select_graph

    def drop_graph(self, graph_name: str) -> bool:
        """Drop a graph space and delete its storage directory."""
        with self._lock:
            if graph_name in self._graphs:
                del self._graphs[graph_name]
            graph_path = os.path.join(self.storage_dir, graph_name)
            if os.path.exists(graph_path):
                shutil.rmtree(graph_path, ignore_errors=True)
                return True
            return False

    drop_project = drop_graph

    def drop_all_projects(self) -> List[str]:
        """Drop all active and persisted graph projects, cleaning disk storage."""
        with self._lock:
            dropped = list(self.list_graphs())
            self._graphs.clear()
            if os.path.exists(self.storage_dir):
                for item in os.listdir(self.storage_dir):
                    item_path = os.path.join(self.storage_dir, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
            return dropped

    def list_graphs(self) -> List[str]:
        """List all active and persisted graph names."""
        with self._lock:
            active = set(self._graphs.keys())
            if os.path.exists(self.storage_dir):
                for entry in os.listdir(self.storage_dir):
                    full_p = os.path.join(self.storage_dir, entry)
                    if os.path.isdir(full_p) and not entry.startswith("."):
                        active.add(entry)
            return sorted(list(active))

    list_projects = list_graphs

    def checkpoint_all(self) -> Dict[str, str]:
        """Atomically checkpoint all loaded graph spaces."""
        with self._lock:
            results = {}
            for name, g in self._graphs.items():
                results[name] = g.checkpoint()
            return results
