"""Structural graph metrics: Triangle Counting and Clustering Coefficient."""
from __future__ import annotations

import logging
from typing import Dict, List, Optional
import numpy as np
import scipy.sparse as sp

logger = logging.getLogger(__name__)


def triangle_count(matrix_store: Any) -> int:
    """Compute total number of triangles in the graph using matrix trace: Trace(A^3) / 6."""
    n = matrix_store.node_count
    if n < 3:
        return 0

    combined = sp.csr_matrix((n, n), dtype=np.float32)
    for r in matrix_store.get_relationship_types():
        combined = combined + matrix_store.get_csr(r)

    # Undirected adjacency with zero diagonal
    adj = ((combined + combined.transpose()) > 0).astype(np.float32)
    adj.setdiag(0)
    adj.eliminate_zeros()

    # A^2
    a2 = adj.dot(adj)
    # Elementwise product with A and sum gives Trace(A^3)
    trace_a3 = adj.multiply(a2).sum()
    return int(round(trace_a3 / 6.0))
