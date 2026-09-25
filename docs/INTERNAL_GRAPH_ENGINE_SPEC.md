# Internal High-Performance Graph Database Engine (Clean-Room Specification)

## 1. Scope & Objective
This specification defines the clean-room implementation of our in-house, high-performance graph database engine (`app/graph_engine/`). Designed to replace FalkorDB for internal business operations, it eliminates Server Side Public License (SSPL) risks while retaining FalkorDB's linear algebraic performance advantages.

---

## 2. Core Functional Requirements

1. **Topology Engine**:
   - Monotonic 0-indexed integer Node ID allocation.
   - Independent Compressed Sparse Row (CSR) and Compressed Sparse Column (CSC) matrices per relationship type.
   - Dynamic dimension growth with amortized constant-time expansion.

2. **Graph Algorithms**:
   - **Multi-Hop Traversal**: Vectorized matrix multiplication over the Boolean semiring.
   - **Shortest Path**: Breadth-First Search (BFS) and Bellman-Ford/Dijkstra over tropical semirings.
   - **PageRank**: Power iteration over sparse transition matrices.
   - **Weakly Connected Components (WCC)**: Sparse label propagation.

3. **Property & Vector Storage**:
   - Segregated property dictionaries per node and edge.
   - Integrated HNSW vector index (`hnswlib`) for 256-dimensional node embeddings.

4. **Provenance & Auditability**:
   - Native `[:MENTIONED_IN]` edge indexing providing $O(1)$ mapping from any entity to its originating text chunks.

5. **Ontology Contract Compliance**:
   - Directly loads and enforces entity and relationship types defined in `ontology.v0.json` and `ontology.v1.json`.

---

## 3. Module Hierarchy (`app/graph_engine/`)

```
app/graph_engine/
├── __init__.py           # Package exports
├── matrix_store.py       # Sparse CSR/CSC adjacency matrices & semiring operators
├── property_store.py     # Segregated columnar & dictionary property store
├── vector_store.py       # HNSWlib vector indexing for node embeddings
├── algorithms.py         # Multi-hop, BFS, ShortestPath, PageRank, WCC
├── query_engine.py       # Pattern matching & query compilation
└── engine.py             # High-level database manager & multi-tenant graph spaces
```
