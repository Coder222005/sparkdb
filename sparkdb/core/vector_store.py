"""VectorStore — High-Performance HNSW Vector Search Engine for Node Embeddings.

Supports Cosine, L2 (Euclidean), and Inner Product distance spaces using hnswlib (Apache 2.0).
Provides seamless fallback to NumPy SIMD vectorization.
"""
from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional, Tuple, Any
import numpy as np

logger = logging.getLogger(__name__)

try:
    import hnswlib
    HNSWLIB_AVAILABLE = True
except ImportError:
    HNSWLIB_AVAILABLE = False


class VectorStore:
    """Manages multi-index HNSW vector spaces."""

    def __init__(self, dimension: int = 256, space: str = "cosine", max_elements: int = 20000):
        self._lock = threading.RLock()
        self.dimension = dimension
        self.space = space
        self.max_elements = max_elements
        self._index: Optional[Any] = None
        self._vectors: Dict[int, np.ndarray] = {}

        if HNSWLIB_AVAILABLE:
            self._index = hnswlib.Index(space=self.space, dim=self.dimension)
            self._index.init_index(max_elements=self.max_elements, ef_construction=200, M=16)
            self._index.set_ef(50)

    def add_node_vector(self, node_id: int, vector: List[float] | np.ndarray) -> None:
        """Add or update an embedding vector for a node."""
        with self._lock:
            vec = np.asarray(vector, dtype=np.float32)
            if vec.shape[0] != self.dimension:
                raise ValueError(f"Vector dim {vec.shape[0]} does not match index dim {self.dimension}")

            self._vectors[node_id] = vec
            if self._index is not None:
                if len(self._vectors) > self.max_elements:
                    self.max_elements *= 2
                    self._index.resize_index(self.max_elements)
                self._index.add_items([vec], [node_id])

    def delete_node_vector(self, node_id: int) -> None:
        """Remove a vector for a node."""
        with self._lock:
            if node_id in self._vectors:
                del self._vectors[node_id]
                if self._index is not None:
                    try:
                        self._index.mark_deleted(node_id)
                    except Exception:
                        pass

    def query_nearest_nodes(self, query_vector: List[float] | np.ndarray, top_k: int = 5) -> List[Tuple[int, float]]:
        """Query top-k nearest nodes with their similarity / distance scores."""
        with self._lock:
            if not self._vectors:
                return []

            q_vec = np.asarray(query_vector, dtype=np.float32)
            k = min(top_k, len(self._vectors))

            if self._index is not None:
                try:
                    labels, distances = self._index.knn_query([q_vec], k=k)
                    return list(zip([int(x) for x in labels[0]], [float(x) for x in distances[0]]))
                except Exception as e:
                    logger.warning(f"HNSW query failed, falling back to NumPy: {e}")

            # NumPy fallback
            results = []
            q_norm = np.linalg.norm(q_vec)
            for nid, vec in self._vectors.items():
                denom = q_norm * np.linalg.norm(vec)
                sim = 1.0 - (np.dot(q_vec, vec) / denom if denom > 0 else 0.0)
                results.append((nid, float(sim)))

            results.sort(key=lambda x: x[1])
            return results[:k]
