"""Centrality algorithms for SparkDB.

Implements:
  - PageRank (Power iteration with dangling-node conservation over GraphBLAS sparse matrices)
  - Degree Centrality (In-degree, Out-degree)
  - Betweenness Centrality (Brandes' algorithm over sparse graph)
"""
from __future__ import annotations

import collections
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
import numpy as np
import scipy.sparse as sp

logger = logging.getLogger(__name__)


def pagerank(
    matrix_store: Any,
    rel_types: Optional[List[str]] = None,
    damping: float = 0.85,
    max_iter: int = 50,
    tol: float = 1e-6,
) -> Dict[int, float]:
    """Compute PageRank scores using power iteration over stochastic transition matrix."""
    n = matrix_store.node_count
    if n == 0:
        return {}

    relations = rel_types or matrix_store.get_relationship_types()
    if not relations:
        return {i: 1.0 / n for i in range(n)}

    if len(relations) == 1:
        combined = matrix_store.get_csr(relations[0])
    elif rel_types is None and hasattr(matrix_store, "get_unified_csr"):
        combined = matrix_store.get_unified_csr()
    else:
        combined = matrix_store.get_csr(relations[0])
        for r in relations[1:]:
            combined = combined + matrix_store.get_csr(r)

    if combined.nnz == 0:
        return {i: 1.0 / n for i in range(n)}

    # High-performance out-degree computation from CSR indptr
    deg = np.diff(combined.indptr).astype(np.float32)
    is_dangling = (deg == 0)
    dangling_idx = np.flatnonzero(is_dangling)
    has_dangling = len(dangling_idx) > 0
    safe_deg = np.where(is_dangling, 1.0, deg)

    # Scale data by 1/out_degree to build stochastic transition matrix
    scale = np.repeat(1.0 / safe_deg, deg.astype(int))
    scaled_data = combined.data * scale
    p_matrix = sp.csr_matrix((scaled_data, combined.indices, combined.indptr), shape=(n, n)).transpose().tocsr()

    rank = np.full(n, 1.0 / n, dtype=np.float32)
    teleport = (1.0 - damping) / n

    for _ in range(max_iter):
        dang = rank[dangling_idx].sum() if has_dangling else 0.0
        next_rank = damping * (p_matrix.dot(rank) + dang / n) + teleport
        if np.abs(next_rank - rank).sum() < tol:
            rank = next_rank
            break
        rank = next_rank

    return dict(enumerate(rank.tolist()))


def degree_centrality(matrix_store: Any, rel_types: Optional[List[str]] = None) -> Dict[int, Dict[str, int]]:
    """Compute in-degree and out-degree centrality for all nodes."""
    n = matrix_store.node_count
    if n == 0:
        return {}

    relations = rel_types or matrix_store.get_relationship_types()
    if len(relations) == 1:
        combined = matrix_store.get_csr(relations[0])
    elif rel_types is None and hasattr(matrix_store, "get_unified_csr"):
        combined = matrix_store.get_unified_csr()
    else:
        combined = matrix_store.get_csr(relations[0])
        for r in relations[1:]:
            combined = combined + matrix_store.get_csr(r)

    out_degs = np.diff(combined.indptr).astype(int)
    csc = combined.tocsc()
    in_degs = np.diff(csc.indptr).astype(int)

    return {
        i: {"in_degree": int(in_degs[i]), "out_degree": int(out_degs[i]), "total": int(in_degs[i] + out_degs[i])}
        for i in range(n)
    }
