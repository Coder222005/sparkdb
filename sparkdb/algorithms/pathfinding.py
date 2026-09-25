"""Pathfinding algorithms for SparkDB.

Implements:
  - Multi-Hop Traversal (Boolean semiring row-slicing / matrix-vector dot products)
  - Directed Shortest Path (BFS with early exit and multi-relation cache)
  - Weighted Shortest Path (Dijkstra over Min-Plus semiring)
"""
from __future__ import annotations

import collections
import heapq
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
import numpy as np
import scipy.sparse as sp

logger = logging.getLogger(__name__)


def multi_hop_paths(
    matrix_store: Any,
    start_node_ids: List[int],
    rel_path: List[str],
    target_label: Optional[str] = None,
    max_paths: Optional[int] = None,
) -> List[List[int]]:
    """Compute all directed paths following a relation chain with optional early limit exit."""
    if not rel_path:
        return [[nid] for nid in (start_node_ids[:max_paths] if max_paths else start_node_ids)]

    current_paths = [[nid] for nid in start_node_ids]

    for rel in rel_path:
        csr = matrix_store.get_csr(rel)
        if csr.nnz == 0:
            return []

        next_paths = []
        for path in current_paths:
            curr = path[-1]
            if curr >= csr.shape[0]:
                continue
            r_start = csr.indptr[curr]
            r_end = csr.indptr[curr + 1]
            for nbr in csr.indices[r_start:r_end]:
                next_paths.append(path + [int(nbr)])
                if max_paths and len(next_paths) >= max_paths:
                    break
            if max_paths and len(next_paths) >= max_paths:
                break

        current_paths = next_paths
        if not current_paths:
            break

    if target_label:
        valid_targets = matrix_store.get_nodes_with_label(target_label)
        current_paths = [p for p in current_paths if p[-1] in valid_targets]

    if max_paths:
        return current_paths[:max_paths]

    return current_paths


def shortest_path(
    matrix_store: Any,
    source_id: int,
    target_id: int,
    rel_types: Optional[List[str]] = None,
) -> Optional[List[int]]:
    """Compute unweighted shortest path using fast BFS with immediate target early exit."""
    if source_id == target_id:
        return [source_id]

    relations = rel_types or matrix_store.get_relationship_types()
    if not relations:
        return None

    if len(relations) == 1:
        csrs = [matrix_store.get_csr(relations[0])]
    elif rel_types is None and hasattr(matrix_store, "get_unified_csr"):
        csrs = [matrix_store.get_unified_csr()]
    else:
        csrs = [matrix_store.get_csr(r) for r in relations]

    visited = {source_id}
    queue = collections.deque([source_id])
    parent: Dict[int, Optional[int]] = {source_id: None}

    found = False
    while queue:
        curr = queue.popleft()
        for csr in csrs:
            if curr >= csr.shape[0]:
                continue
            r_start = csr.indptr[curr]
            r_end = csr.indptr[curr + 1]
            for nbr in csr.indices[r_start:r_end]:
                nbr = int(nbr)
                if nbr not in visited:
                    visited.add(nbr)
                    parent[nbr] = curr
                    if nbr == target_id:
                        found = True
                        break
                    queue.append(nbr)
            if found:
                break
        if found:
            break

    if not found:
        return None

    path = []
    curr_node: Optional[int] = target_id
    while curr_node is not None:
        path.append(curr_node)
        curr_node = parent[curr_node]
    path.reverse()
    return path


def dijkstra_shortest_path(
    matrix_store: Any,
    source_id: int,
    target_id: int,
    rel_types: Optional[List[str]] = None,
) -> Tuple[Optional[List[int]], float]:
    """Compute weighted shortest path over the Min-Plus semiring using Dijkstra's algorithm."""
    if source_id == target_id:
        return [source_id], 0.0

    relations = rel_types or matrix_store.get_relationship_types()
    if not relations:
        return None, float("inf")

    if len(relations) == 1:
        combined = matrix_store.get_csr(relations[0])
    elif rel_types is None and hasattr(matrix_store, "get_unified_csr"):
        combined = matrix_store.get_unified_csr()
    else:
        combined = matrix_store.get_csr(relations[0])
        for r in relations[1:]:
            combined = combined + matrix_store.get_csr(r)

    distances = {source_id: 0.0}
    parent: Dict[int, Optional[int]] = {source_id: None}
    pq = [(0.0, source_id)]

    while pq:
        dist, curr = heapq.heappop(pq)
        if curr == target_id:
            break
        if dist > distances.get(curr, float("inf")):
            continue

        r_start = combined.indptr[curr]
        r_end = combined.indptr[curr + 1]
        for idx in range(r_start, r_end):
            nbr = int(combined.indices[idx])
            weight = float(combined.data[idx])
            new_dist = dist + weight
            if new_dist < distances.get(nbr, float("inf")):
                distances[nbr] = new_dist
                parent[nbr] = curr
                heapq.heappush(pq, (new_dist, nbr))

    if target_id not in parent:
        return None, float("inf")

    path = []
    curr_node = target_id
    while curr_node is not None:
        path.append(curr_node)
        curr_node = parent[curr_node]
    path.reverse()
    return path, distances[target_id]
