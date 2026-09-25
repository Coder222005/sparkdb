"""PropertyStore — Columnar and Key-Value Property Storage with Secondary Inverted Indexes.

Decoupled completely from topology to preserve L1/L2/L3 cache locality.
Supports exact matching, range scans, and deletion cleanup.
Includes DiskPropertyStore with SQLite WAL and LRU caching for zero-OOM scaling.
"""
from __future__ import annotations

from collections import OrderedDict
import json
import logging
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class PropertyStore:
    """Thread-safe in-memory property storage with secondary inverted indexes."""

    def __init__(self):
        self._lock = threading.RLock()
        self.node_properties: Dict[int, Dict[str, Any]] = {}
        self.edge_properties: Dict[Tuple[int, str, int], Dict[str, Any]] = {}
        self._property_indexes: Dict[str, Dict[Any, Set[int]]] = {}

    def set_node_properties(self, node_id: int, properties: Dict[str, Any]) -> None:
        """Assign or update properties for a node and update secondary indexes."""
        with self._lock:
            if node_id not in self.node_properties:
                self.node_properties[node_id] = {}

            current = self.node_properties[node_id]
            for key, val in properties.items():
                if key in self._property_indexes:
                    old_val = current.get(key)
                    if old_val is not None and old_val in self._property_indexes[key]:
                        self._property_indexes[key][old_val].discard(node_id)

                    if val not in self._property_indexes[key]:
                        self._property_indexes[key][val] = set()
                    self._property_indexes[key][val].add(node_id)

                current[key] = val

    def set_nodes_properties_batch(self, batch_props: Dict[int, Dict[str, Any]]) -> None:
        """Assign or update properties for multiple nodes in bulk."""
        with self._lock:
            for node_id, properties in batch_props.items():
                if node_id not in self.node_properties:
                    self.node_properties[node_id] = {}
                current = self.node_properties[node_id]
                for key, val in properties.items():
                    if key in self._property_indexes:
                        old_val = current.get(key)
                        if old_val is not None and old_val in self._property_indexes[key]:
                            self._property_indexes[key][old_val].discard(node_id)
                        if val not in self._property_indexes[key]:
                            self._property_indexes[key][val] = set()
                        self._property_indexes[key][val].add(node_id)
                    current[key] = val

    def set_edges_properties_batch(self, batch_edges: List[Tuple[int, str, int, Dict[str, Any]]]) -> None:
        """Assign properties for multiple edges in bulk."""
        with self._lock:
            for src, rel_type, dst, properties in batch_edges:
                key = (src, rel_type, dst)
                if key not in self.edge_properties:
                    self.edge_properties[key] = {}
                self.edge_properties[key].update(properties)

    def get_node_properties(self, node_id: int) -> Dict[str, Any]:
        """Retrieve properties for a node directly from cache without redundant copy allocation."""
        with self._lock:
            return self.node_properties.get(node_id, {})

    def batch_get_nodes_properties(self, node_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Fetches properties for a list of node IDs."""
        with self._lock:
            return {nid: self.node_properties.get(nid, {}) for nid in node_ids}

    def get_numeric_property_values(self, node_ids: List[int], property_name: str) -> List[float]:
        """Fast numeric property extraction for aggregations directly from the structured cache."""
        with self._lock:
            vals: List[float] = []
            for nid in node_ids:
                props = self.node_properties.get(nid)
                if props is not None and property_name in props:
                    val = props[property_name]
                    if isinstance(val, (int, float)) and not isinstance(val, bool):
                        vals.append(float(val))
            return vals

    def extract_numeric_property(self, node_ids: List[int], property_name: str) -> List[float]:
        """Extract numeric property values for given node IDs."""
        return self.get_numeric_property_values(node_ids, property_name)

    def aggregate_numeric_property(
        self, node_ids: List[int], property_name: str, agg: str
    ) -> Optional[float]:
        """Perform fast numeric aggregation (avg, min, max, sum, count) directly on structured cache."""
        vals = self.get_numeric_property_values(node_ids, property_name)
        if not vals:
            return 0.0 if agg.lower() in ("sum", "count") else None
        agg_lower = agg.lower()
        if agg_lower == "sum":
            return sum(vals)
        elif agg_lower == "avg":
            return sum(vals) / len(vals)
        elif agg_lower == "min":
            return min(vals)
        elif agg_lower == "max":
            return max(vals)
        elif agg_lower == "count":
            return float(len(vals))
        return None

    def delete_node_properties(self, node_id: int) -> None:
        """Clean up properties and index entries for a deleted node."""
        with self._lock:
            if node_id in self.node_properties:
                props = self.node_properties.pop(node_id)
                for key, val in props.items():
                    if key in self._property_indexes and val in self._property_indexes[key]:
                        self._property_indexes[key][val].discard(node_id)

    def set_edge_properties(self, src: int, rel_type: str, dst: int, properties: Dict[str, Any]) -> None:
        """Store properties for an edge."""
        with self._lock:
            key = (src, rel_type, dst)
            if key not in self.edge_properties:
                self.edge_properties[key] = {}
            self.edge_properties[key].update(properties)

    def get_edge_properties(self, src: int, rel_type: str, dst: int) -> Dict[str, Any]:
        """Retrieve properties for an edge."""
        with self._lock:
            return dict(self.edge_properties.get((src, rel_type, dst), {}))

    def create_node_property_index(self, property_name: str) -> None:
        """Build a secondary inverted index on a property."""
        with self._lock:
            if property_name in self._property_indexes:
                return

            idx: Dict[Any, Set[int]] = {}
            for nid, props in self.node_properties.items():
                val = props.get(property_name)
                if val is not None:
                    if val not in idx:
                        idx[val] = set()
                    idx[val].add(nid)

            self._property_indexes[property_name] = idx

    def drop_node_property_index(self, property_name: str) -> bool:
        """Drop a secondary inverted index on a property."""
        with self._lock:
            if property_name in self._property_indexes:
                del self._property_indexes[property_name]
                return True
            return False

    def get_indexed_properties(self) -> List[str]:
        """List all indexed property names."""
        with self._lock:
            return sorted(list(self._property_indexes.keys()))

    def find_nodes_by_property(self, property_name: str, value: Any) -> Set[int]:
        """Lookup nodes matching property value in O(1) via index, or linear fallback."""
        with self._lock:
            if property_name in self._property_indexes:
                return set(self._property_indexes[property_name].get(value, set()))

            matched = set()
            for nid, props in self.node_properties.items():
                if props.get(property_name) == value:
                    matched.add(nid)
            return matched


class DiskPropertyStore:
    """Tiered Disk-Backed Property Store using SQLite WAL mode and in-memory LRU Cache.

    Decouples large property payloads, texts, and metadata from RAM, preventing Out-Of-Memory (OOM)
    on massive enterprise graphs while keeping frequently accessed nodes at sub-microsecond latency.
    """

    def __init__(self, db_path: str, lru_cache_size: int = 100000):
        self._lock = threading.RLock()
        self.db_path = db_path
        self.lru_cache_size = lru_cache_size
        self._node_lru: OrderedDict[int, Dict[str, Any]] = OrderedDict()
        self._edge_lru: OrderedDict[Tuple[int, str, int], Dict[str, Any]] = OrderedDict()
        self._init_db()

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA mmap_size = 2147483648;")
        conn.execute("PRAGMA cache_size = -262144;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS node_properties (
                node_id INTEGER PRIMARY KEY,
                properties TEXT
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS edge_properties (
                src INTEGER,
                rel_type TEXT,
                dst INTEGER,
                properties TEXT,
                PRIMARY KEY (src, rel_type, dst)
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS property_index (
                prop_key TEXT,
                prop_val TEXT,
                node_id INTEGER,
                PRIMARY KEY (prop_key, prop_val, node_id)
            );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prop_lookup ON property_index(prop_key, prop_val);")
        conn.commit()
        conn.close()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA mmap_size = 2147483648;")
        conn.execute("PRAGMA cache_size = -262144;")
        return conn

    def set_node_properties(self, node_id: int, properties: Dict[str, Any]) -> None:
        with self._lock:
            existing = self.get_node_properties(node_id)
            current = dict(existing)
            current.update(properties)

            if node_id in self._node_lru:
                self._node_lru.move_to_end(node_id)
            self._node_lru[node_id] = current
            if len(self._node_lru) > self.lru_cache_size:
                self._node_lru.popitem(last=False)

            conn = self._get_conn()
            try:
                conn.execute(
                    "INSERT INTO node_properties (node_id, properties) VALUES (?, ?) "
                    "ON CONFLICT(node_id) DO UPDATE SET properties=excluded.properties",
                    (node_id, json.dumps(current)),
                )
                for k, v in properties.items():
                    conn.execute(
                        "INSERT OR IGNORE INTO property_index (prop_key, prop_val, node_id) VALUES (?, ?, ?)",
                        (k, str(v), node_id),
                    )
                conn.commit()
            finally:
                conn.close()

    def set_nodes_properties_batch(self, batch_props: Dict[int, Dict[str, Any]]) -> None:
        """Batch insert/update properties for multiple nodes in a single atomic transaction."""
        with self._lock:
            node_rows = []
            idx_rows = []
            for node_id, properties in batch_props.items():
                existing = self.get_node_properties(node_id)
                current = dict(existing)
                current.update(properties)
                if node_id in self._node_lru:
                    self._node_lru.move_to_end(node_id)
                self._node_lru[node_id] = current
                if len(self._node_lru) > self.lru_cache_size:
                    self._node_lru.popitem(last=False)

                node_rows.append((node_id, json.dumps(current)))
                for k, v in properties.items():
                    idx_rows.append((k, str(v), node_id))

            conn = self._get_conn()
            try:
                conn.executemany(
                    "INSERT INTO node_properties (node_id, properties) VALUES (?, ?) "
                    "ON CONFLICT(node_id) DO UPDATE SET properties=excluded.properties",
                    node_rows,
                )
                if idx_rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO property_index (prop_key, prop_val, node_id) VALUES (?, ?, ?)",
                        idx_rows,
                    )
                conn.commit()
            finally:
                conn.close()

    def set_edges_properties_batch(self, batch_edges: List[Tuple[int, str, int, Dict[str, Any]]]) -> None:
        """Batch insert/update properties for multiple edges in a single atomic transaction."""
        with self._lock:
            edge_rows = []
            for src, rel_type, dst, properties in batch_edges:
                key = (src, rel_type, dst)
                existing = self.get_edge_properties(src, rel_type, dst)
                current = dict(existing)
                current.update(properties)
                if key in self._edge_lru:
                    self._edge_lru.move_to_end(key)
                self._edge_lru[key] = current
                if len(self._edge_lru) > self.lru_cache_size:
                    self._edge_lru.popitem(last=False)
                edge_rows.append((src, rel_type, dst, json.dumps(current)))

            conn = self._get_conn()
            try:
                conn.executemany(
                    "INSERT INTO edge_properties (src, rel_type, dst, properties) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(src, rel_type, dst) DO UPDATE SET properties=excluded.properties",
                    edge_rows,
                )
                conn.commit()
            finally:
                conn.close()

    def get_node_properties(self, node_id: int) -> Dict[str, Any]:
        """Retrieve properties for a node, returning cached dict reference directly without allocating a new dict()."""
        with self._lock:
            if node_id in self._node_lru:
                self._node_lru.move_to_end(node_id)
                return self._node_lru[node_id]

            conn = self._get_conn()
            try:
                cursor = conn.execute("SELECT properties FROM node_properties WHERE node_id = ?", (node_id,))
                row = cursor.fetchone()
                if row:
                    props = json.loads(row[0])
                    self._node_lru[node_id] = props
                    if len(self._node_lru) > self.lru_cache_size:
                        self._node_lru.popitem(last=False)
                    return props
                return {}
            finally:
                conn.close()

    def batch_get_nodes_properties(self, node_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Fetches properties for a list of node IDs.
        First checks the in-memory LRU cache; for any cache misses, fetches them from SQLite
        using a single WHERE node_id IN (...) batch query in chunks of 500, populating the LRU cache.
        """
        with self._lock:
            result: Dict[int, Dict[str, Any]] = {}
            misses: List[int] = []
            seen_misses: Set[int] = set()

            for nid in node_ids:
                if nid in self._node_lru:
                    self._node_lru.move_to_end(nid)
                    result[nid] = self._node_lru[nid]
                else:
                    if nid not in seen_misses:
                        misses.append(nid)
                        seen_misses.add(nid)

            if misses:
                conn = self._get_conn()
                try:
                    for i in range(0, len(misses), 500):
                        chunk = misses[i : i + 500]
                        placeholders = ",".join("?" * len(chunk))
                        query = f"SELECT node_id, properties FROM node_properties WHERE node_id IN ({placeholders})"
                        cursor = conn.execute(query, chunk)
                        found: Set[int] = set()
                        for row in cursor.fetchall():
                            nid = row[0]
                            props = json.loads(row[1])
                            self._node_lru[nid] = props
                            if len(self._node_lru) > self.lru_cache_size:
                                self._node_lru.popitem(last=False)
                            result[nid] = props
                            found.add(nid)

                        for nid in chunk:
                            if nid not in found:
                                result[nid] = {}
                finally:
                    conn.close()

            return {nid: result.get(nid, {}) for nid in node_ids}

    def get_numeric_property_values(self, node_ids: List[int], property_name: str) -> List[float]:
        """Fast numeric property extraction for aggregations directly from structured cache to eliminate per-entity JSON parsing."""
        with self._lock:
            uncached = [nid for nid in node_ids if nid not in self._node_lru]
            if uncached:
                self.batch_get_nodes_properties(uncached)

            vals: List[float] = []
            for nid in node_ids:
                props = self._node_lru.get(nid)
                if props is not None and property_name in props:
                    val = props[property_name]
                    if isinstance(val, (int, float)) and not isinstance(val, bool):
                        vals.append(float(val))
            return vals

    def extract_numeric_property(self, node_ids: List[int], property_name: str) -> List[float]:
        """Extract numeric property values for given node IDs."""
        return self.get_numeric_property_values(node_ids, property_name)

    def aggregate_numeric_property(
        self, node_ids: List[int], property_name: str, agg: str
    ) -> Optional[float]:
        """Perform fast numeric aggregation (avg, min, max, sum, count) directly on structured cache."""
        vals = self.get_numeric_property_values(node_ids, property_name)
        if not vals:
            return 0.0 if agg.lower() in ("sum", "count") else None
        agg_lower = agg.lower()
        if agg_lower == "sum":
            return sum(vals)
        elif agg_lower == "avg":
            return sum(vals) / len(vals)
        elif agg_lower == "min":
            return min(vals)
        elif agg_lower == "max":
            return max(vals)
        elif agg_lower == "count":
            return float(len(vals))
        return None

    def delete_node_properties(self, node_id: int) -> None:
        with self._lock:
            self._node_lru.pop(node_id, None)
            conn = self._get_conn()
            try:
                conn.execute("DELETE FROM node_properties WHERE node_id = ?", (node_id,))
                conn.execute("DELETE FROM property_index WHERE node_id = ?", (node_id,))
                conn.commit()
            finally:
                conn.close()

    def set_edge_properties(self, src: int, rel_type: str, dst: int, properties: Dict[str, Any]) -> None:
        with self._lock:
            current = self.get_edge_properties(src, rel_type, dst)
            current.update(properties)
            key = (src, rel_type, dst)
            if key in self._edge_lru:
                self._edge_lru.move_to_end(key)
            self._edge_lru[key] = current
            if len(self._edge_lru) > self.lru_cache_size:
                self._edge_lru.popitem(last=False)

            conn = self._get_conn()
            try:
                conn.execute(
                    "INSERT INTO edge_properties (src, rel_type, dst, properties) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(src, rel_type, dst) DO UPDATE SET properties=excluded.properties",
                    (src, rel_type, dst, json.dumps(current)),
                )
                conn.commit()
            finally:
                conn.close()

    def get_edge_properties(self, src: int, rel_type: str, dst: int) -> Dict[str, Any]:
        with self._lock:
            key = (src, rel_type, dst)
            if key in self._edge_lru:
                self._edge_lru.move_to_end(key)
                return self._edge_lru[key]

            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    "SELECT properties FROM edge_properties WHERE src = ? AND rel_type = ? AND dst = ?",
                    (src, rel_type, dst),
                )
                row = cursor.fetchone()
                if row:
                    props = json.loads(row[0])
                    self._edge_lru[key] = props
                    if len(self._edge_lru) > self.lru_cache_size:
                        self._edge_lru.popitem(last=False)
                    return props
                return {}
            finally:
                conn.close()

    def create_node_property_index(self, property_name: str) -> None:
        pass

    def drop_node_property_index(self, property_name: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("DELETE FROM property_index WHERE prop_key = ?", (property_name,))
                conn.commit()
                return True
            finally:
                conn.close()

    def get_indexed_properties(self) -> List[str]:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute("SELECT DISTINCT prop_key FROM property_index")
                return sorted([r[0] for r in cursor.fetchall()])
            finally:
                conn.close()

    def find_nodes_by_property(self, property_name: str, value: Any) -> Set[int]:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    "SELECT node_id FROM property_index WHERE prop_key = ? AND prop_val = ?",
                    (property_name, str(value)),
                )
                return {row[0] for row in cursor.fetchall()}
            finally:
                conn.close()
