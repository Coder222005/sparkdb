"""MatrixStore — High-Performance Sparse Linear Algebra Graph Substrate.

Implements GraphBLAS mathematical foundations:
  - Compressed Sparse Row (CSR) & Compressed Sparse Column (CSC) matrices per relation
  - Dynamic dimension expansion and free-list node ID recycling
  - Boolean and Tropical semiring vector-matrix multiplication
  - Cached unified adjacency matrices
  - Thread-safe mutation locking
"""
from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional, Set, Tuple
import numpy as np
import scipy.sparse as sp

logger = logging.getLogger(__name__)


class MatrixStore:
    """Thread-safe sparse matrix topology engine with GraphBLAS duality."""

    def __init__(self, initial_capacity: int = 256):
        self._lock = threading.RLock()
        self.node_capacity = max(16, initial_capacity)
        self.node_count = 0
        self._free_node_ids: List[int] = []
        self._active_nodes: Set[int] = set()

        self.labels: Dict[str, Set[int]] = {}
        self.node_to_labels: Dict[int, Set[str]] = {}

        # High-performance edge storage: rel_type -> {(src, dst): weight}
        self._rel_matrices: Dict[str, Dict[Tuple[int, int], float]] = {}
        self._csr_cache: Dict[str, sp.csr_matrix] = {}
        self._csc_cache: Dict[str, sp.csc_matrix] = {}
        self._dirty_matrices: Set[str] = set()
        self._rev_csr_cache: Dict[str, sp.csr_matrix] = {}
        self._unified_csr: Optional[sp.csr_matrix] = None
        self._unified_dirty: bool = True

    def add_node(self, labels: Optional[List[str]] = None) -> int:
        """Allocate a node ID (recycles deleted IDs if available) and assign labels."""
        with self._lock:
            if self._free_node_ids:
                node_id = self._free_node_ids.pop()
            else:
                node_id = self.node_count
                self.node_count += 1
                if self.node_count > self.node_capacity:
                    self.node_capacity *= 2

            self._active_nodes.add(node_id)
            self.node_to_labels[node_id] = set()
            self._dirty_matrices.update(self._rel_matrices.keys())
            self._unified_dirty = True
            if labels:
                for lbl in labels:
                    self.add_node_label(node_id, lbl)

            return node_id

    def delete_node(self, node_id: int) -> bool:
        """Mark a node as deleted, clear its edges, and add to free-list."""
        with self._lock:
            if node_id not in self._active_nodes:
                return False

            self._active_nodes.remove(node_id)
            self._free_node_ids.append(node_id)

            # Remove from label sets
            for lbl in self.node_to_labels.get(node_id, set()):
                if lbl in self.labels:
                    self.labels[lbl].discard(node_id)
            self.node_to_labels.pop(node_id, None)

            # Clear all incident edges in matrices
            for rel, mat in self._rel_matrices.items():
                keys_to_delete = [k for k in mat.keys() if k[0] == node_id or k[1] == node_id]
                for k in keys_to_delete:
                    del mat[k]
                if keys_to_delete:
                    self._dirty_matrices.add(rel)
                    self._unified_dirty = True

            return True

    def add_node_label(self, node_id: int, label: str) -> None:
        """Attach a categorical label to a node ID."""
        with self._lock:
            if node_id not in self._active_nodes:
                raise ValueError(f"Cannot label non-existent or deleted node {node_id}")
            if label not in self.labels:
                self.labels[label] = set()
            self.labels[label].add(node_id)
            self.node_to_labels[node_id].add(label)

    def remove_node_label(self, node_id: int, label: str) -> None:
        """Remove a categorical label from a node ID."""
        with self._lock:
            if label in self.labels:
                self.labels[label].discard(node_id)
            if node_id in self.node_to_labels:
                self.node_to_labels[node_id].discard(label)

    def add_edge(self, src: int, rel_type: str, dst: int, weight: float = 1.0) -> None:
        """Insert a directed edge into the relation's sparse adjacency store."""
        with self._lock:
            if src not in self._active_nodes or dst not in self._active_nodes:
                raise ValueError(f"Invalid edge endpoints ({src}, {dst}) - node not active")

            if rel_type not in self._rel_matrices:
                self._rel_matrices[rel_type] = {}

            self._rel_matrices[rel_type][(src, dst)] = float(weight)
            self._dirty_matrices.add(rel_type)
            self._unified_dirty = True

    def delete_edge(self, src: int, rel_type: str, dst: int) -> bool:
        """Remove a directed edge."""
        with self._lock:
            if rel_type in self._rel_matrices and (src, dst) in self._rel_matrices[rel_type]:
                del self._rel_matrices[rel_type][(src, dst)]
                self._dirty_matrices.add(rel_type)
                self._unified_dirty = True
                return True
            return False

    def get_csr(self, rel_type: str) -> sp.csr_matrix:
        """Retrieve cached or compiled Compressed Sparse Row matrix."""
        with self._lock:
            if rel_type not in self._rel_matrices:
                return sp.csr_matrix((self.node_count, self.node_count), dtype=np.float32)

            if rel_type in self._dirty_matrices or rel_type not in self._csr_cache:
                mat = self._rel_matrices[rel_type]
                if not mat:
                    self._csr_cache[rel_type] = sp.csr_matrix((self.node_count, self.node_count), dtype=np.float32)
                else:
                    rows = np.fromiter((k[0] for k in mat.keys()), dtype=np.int32, count=len(mat))
                    cols = np.fromiter((k[1] for k in mat.keys()), dtype=np.int32, count=len(mat))
                    vals = np.fromiter(mat.values(), dtype=np.float32, count=len(mat))
                    self._csr_cache[rel_type] = sp.csr_matrix((vals, (rows, cols)), shape=(self.node_count, self.node_count))
                self._dirty_matrices.discard(rel_type)

            return self._csr_cache[rel_type]

    def get_csc(self, rel_type: str) -> sp.csc_matrix:
        """Retrieve cached Compressed Sparse Column matrix for reverse traversals."""
        with self._lock:
            if rel_type not in self._rel_matrices:
                return sp.csr_matrix((self.node_count, self.node_count), dtype=np.float32).tocsc()

            if rel_type not in self._csc_cache or rel_type in self._dirty_matrices:
                csr = self.get_csr(rel_type)
                self._csc_cache[rel_type] = csr.tocsc()

            return self._csc_cache[rel_type]

    def get_reverse_adjacency(self, rel_type: str) -> sp.csr_matrix:
        """Retrieve cached or compiled reverse (transposed) adjacency matrix for backward traversals."""
        with self._lock:
            if rel_type not in self._rel_matrices:
                return sp.csr_matrix((self.node_count, self.node_count), dtype=np.float32)

            if rel_type not in self._rev_csr_cache or rel_type in self._dirty_matrices:
                csr = self.get_csr(rel_type)
                self._rev_csr_cache[rel_type] = csr.transpose().tocsr()

            return self._rev_csr_cache[rel_type]

    def get_unified_csr(self) -> sp.csr_matrix:
        """Retrieve or build the combined adjacency CSR across all relationship types."""
        with self._lock:
            if self._unified_csr is not None and not self._unified_dirty:
                return self._unified_csr

            total_edges = sum(len(m) for m in self._rel_matrices.values())
            if total_edges == 0:
                self._unified_csr = sp.csr_matrix((self.node_count, self.node_count), dtype=np.float32)
            else:
                rows = np.empty(total_edges, dtype=np.int32)
                cols = np.empty(total_edges, dtype=np.int32)
                vals = np.empty(total_edges, dtype=np.float32)
                offset = 0
                for mat in self._rel_matrices.values():
                    m_len = len(mat)
                    if m_len == 0:
                        continue
                    rows[offset:offset + m_len] = [k[0] for k in mat.keys()]
                    cols[offset:offset + m_len] = [k[1] for k in mat.keys()]
                    vals[offset:offset + m_len] = list(mat.values())
                    offset += m_len
                self._unified_csr = sp.csr_matrix((vals, (rows, cols)), shape=(self.node_count, self.node_count))
            self._unified_dirty = False
            return self._unified_csr

    def traverse_boolean_step(self, frontier: np.ndarray, rel_type: str) -> np.ndarray:
        """Compute single-hop reachability over Boolean semiring via vector-matrix dot product."""
        if frontier.shape[0] != self.node_count:
            padded = np.zeros(self.node_count, dtype=bool)
            padded[:min(frontier.shape[0], self.node_count)] = frontier[:min(frontier.shape[0], self.node_count)]
            frontier = padded

        csr = self.get_csr(rel_type)
        if csr.nnz == 0:
            return np.zeros(self.node_count, dtype=bool)

        v_sparse = sp.csr_matrix(frontier.astype(np.float32))
        res_sparse = v_sparse.dot(csr)
        res_dense = np.asarray(res_sparse.todense()).flatten()
        return res_dense > 0.0

    def get_active_nodes(self) -> Set[int]:
        """Return all live node IDs."""
        with self._lock:
            return set(self._active_nodes)

    def get_nodes_with_label(self, label: str) -> Set[int]:
        """Return set of active node IDs with the given label."""
        with self._lock:
            return set(self.labels.get(label, set())).intersection(self._active_nodes)

    def get_relationship_types(self) -> List[str]:
        """Return all relationship types."""
        with self._lock:
            return list(self._rel_matrices.keys())
