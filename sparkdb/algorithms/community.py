"""Community detection algorithms for SparkDB.

Implements:
  - Weakly Connected Components (WCC)
  - Strongly Connected Components (SCC via Tarjan/Kosaraju)
  - Label Propagation Algorithm (LPA)
"""
from __future__ import annotations

import collections
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
import numpy as np
import scipy.sparse as sp
from scipy.sparse.csgraph import connected_components

logger = logging.getLogger(__name__)


def weakly_connected_components(matrix_store: Any) -> Dict[int, int]:
    """Compute Weakly Connected Components (WCC) mapping node_id -> component_id."""
    n = matrix_store.node_count
    if n == 0:
        return {}

    if hasattr(matrix_store, "get_unified_csr"):
        combined = matrix_store.get_unified_csr()
    else:
        relations = matrix_store.get_relationship_types()
        if not relations:
            return {i: i for i in range(n)}
        combined = matrix_store.get_csr(relations[0])
        for r in relations[1:]:
            combined = combined + matrix_store.get_csr(r)

    # Scipy connected_components with directed=False evaluates the undirected graph directly
    n_components, labels = connected_components(csgraph=combined, directed=False)
    return dict(enumerate(labels.tolist()))


def strongly_connected_components(matrix_store: Any) -> Dict[int, int]:
    """Compute Strongly Connected Components (SCC) mapping node_id -> component_id."""
    n = matrix_store.node_count
    if n == 0:
        return {}

    if hasattr(matrix_store, "get_unified_csr"):
        combined = matrix_store.get_unified_csr()
    else:
        relations = matrix_store.get_relationship_types()
        if not relations:
            return {i: i for i in range(n)}
        combined = matrix_store.get_csr(relations[0])
        for r in relations[1:]:
            combined = combined + matrix_store.get_csr(r)

    n_components, labels = connected_components(csgraph=combined, directed=True, connection="strong")
    return dict(enumerate(labels.tolist()))


def label_propagation(matrix_store: Any, max_iter: int = 30) -> Dict[int, int]:
    """Community detection via semi-synchronous Label Propagation."""
    n = matrix_store.node_count
    if n == 0:
        return {}

    if hasattr(matrix_store, "get_unified_csr"):
        combined = matrix_store.get_unified_csr()
    else:
        relations = matrix_store.get_relationship_types()
        if not relations:
            return {i: i for i in range(n)}
        combined = matrix_store.get_csr(relations[0])
        for r in relations[1:]:
            combined = combined + matrix_store.get_csr(r)

    symmetric = (combined + combined.transpose()).tocsr()

    # Initial community labels: each node is its own community
    labels = np.arange(n, dtype=int)

    for _ in range(max_iter):
        changed = False
        nodes = np.random.permutation(n)
        for u in nodes:
            r_start = symmetric.indptr[u]
            r_end = symmetric.indptr[u + 1]
            nbrs = symmetric.indices[r_start:r_end]
            if len(nbrs) == 0:
                continue

            nbr_labels = labels[nbrs]
            counts = collections.Counter(nbr_labels)
            max_freq = max(counts.values())
            best_labels = [lbl for lbl, c in counts.items() if c == max_freq]
            chosen = int(np.random.choice(best_labels))

            if chosen != labels[u]:
                labels[u] = chosen
                changed = True

        if not changed:
            break

    return dict(enumerate(labels.tolist()))
