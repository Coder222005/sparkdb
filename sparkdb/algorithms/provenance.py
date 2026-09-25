"""Bidirectional Provenance tracing connecting entities to source document chunks."""
from __future__ import annotations

from typing import Any, Dict, List


def trace_provenance(matrix_store: Any, property_store: Any, entity_node_id: int) -> List[Dict[str, Any]]:
    """Trace all Chunk nodes linked via [:MENTIONED_IN] edges."""
    csr = matrix_store.get_csr("MENTIONED_IN")
    if csr.nnz == 0 or entity_node_id >= csr.shape[0]:
        return []

    r_start = csr.indptr[entity_node_id]
    r_end = csr.indptr[entity_node_id + 1]
    chunk_node_ids = csr.indices[r_start:r_end]

    results = []
    for cid in chunk_node_ids:
        props = property_store.get_node_properties(int(cid))
        props["_id"] = int(cid)
        props["_labels"] = list(matrix_store.node_to_labels.get(int(cid), set()))
        results.append(props)

    return results
