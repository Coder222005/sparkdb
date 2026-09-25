# SparkDB v1.1.0 — Full Internal Technical Reference

> **Audience**: Engine engineers, contributors, and advanced integrators. Every internal subsystem, data structure, and algorithm is documented here in full technical depth. You should be able to understand and re-implement SparkDB from this document alone.

---

## Section 1: Repository & Package Structure

### 1.1 Annotated Directory Tree

```
sparkdb/                             ← project root (pyproject.toml lives here)
│
├── pyproject.toml                   ← PEP 621 build manifest (setuptools, entry-points)
├── README.md                        ← High-level project overview
│
├── sparkdb/                         ← Installable Python package root
│   ├── __init__.py                  ← Public surface: exports SparkDB, GraphClient, GraphSpace, QueryResult, __version__
│   │
│   ├── cli.py                       ← sparkdb-cli entry-point (interactive REPL / admin commands)
│   │
│   ├── core/                        ← Storage and traversal engines
│   │   ├── engine.py                ← GraphSpace (tenant graph) + SparkDB (cluster) orchestration
│   │   ├── matrix_store.py          ← GraphBLAS sparse topology: CSR/CSC coordinate dicts + dirty cache
│   │   ├── property_store.py        ← In-memory PropertyStore + hybrid DiskPropertyStore (SQLite WAL)
│   │   ├── vector_store.py          ← HNSW vector index (hnswlib) with NumPy fallback
│   │   ├── fulltext_store.py        ← Inverted index + BM25 scorer
│   │   └── persistence.py           ← Append-Only File (AOF) WAL + JSON snapshot engine
│   │
│   ├── cypher/                      ← Query language subsystem
│   │   ├── parser.py                ← Regex-based recursive descent Cypher parser + two-tier LRU plan cache
│   │   └── executor.py              ← Cypher AST → GraphBLAS / SQLite execution engine; QueryResult emitter
│   │
│   ├── algorithms/                  ← Graph algorithms
│   │   ├── __init__.py              ← Public exports: multi_hop_paths, shortest_path, pagerank, etc.
│   │   ├── pathfinding.py           ← BFS shortest path, Dijkstra, multi-hop CSR row-slice traversal
│   │   ├── centrality.py            ← PageRank (power iteration) + degree centrality
│   │   ├── community.py             ← WCC / SCC (scipy connected_components) + Label Propagation
│   │   ├── structural.py            ← Triangle count via Trace(A³)/6
│   │   └── provenance.py            ← MENTIONED_IN edge path lineage tracing
│   │
│   ├── client/                      ← Python SDK layer
│   │   ├── __init__.py              ← SparkDBClient (embedded + remote), GraphClient (in-process graph wrapper)
│   │   └── remote_client.py         ← RemoteGraphClient: HTTP + MessagePack binary protocol client
│   │
│   └── server/                      ← HTTP server subsystem
│       ├── app.py                   ← SparkDBRequestHandler + run_server (ThreadingHTTPServer)
│       └── server.py                ← Re-export shim + __main__ entry point
│
├── tests/                           ← pytest test suite
│   ├── test_sparkdb_complete.py
│   ├── test_sparkdb_hybrid_storage.py
│   ├── test_sparkdb_mutations_and_ontology.py
│   ├── test_falkordb_advanced_parity.py
│   ├── test_sparkdb_remote_client.py
│   ├── test_cbo_and_ast_cache.py
│   └── test_storage_optimization_and_binary_protocol.py
│
├── scripts/                         ← Benchmarking and stress-test utilities
│   ├── benchmark_10m_complex.py
│   ├── benchmark_query_retrieval.py
│   └── stress_test.py
│
├── docker/                          ← Container build artifacts
│   ├── Dockerfile.sparkdb           ← Production Docker image (node:20-bookworm base)
│   ├── docker-compose.yml           ← Compose service definition (port 7379, /data/sparkdb volume)
│   └── sparkdb_deps/                ← Pre-installed native wheel copies (numpy, scipy, hnswlib)
│
└── docs/                            ← Documentation (this file + architecture + pipeline + user guide)
    ├── SPARKDB_FULL_INTERNALS.md    ← ← ← THIS FILE
    ├── SPARKDB_FULL_PIPELINE.md
    ├── SPARKDB_ARCHITECTURE_AND_DECISIONS.md
    ├── SPARKDB_USER_GUIDE.md
    └── SPARKDB_END_USER_GUIDE.md
```

### 1.2 How the Package is Built (`pyproject.toml`)

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "sparkdb"
version = "1.1.0"
description = "Fast GraphBLAS Sparse Linear Algebra Graph Database (Clean-Room / Zero-SSPL)"
license = { text = "BSD-3-Clause" }
dependencies = []        # ← zero hard deps; numpy/scipy/hnswlib are [server] extras

[project.optional-dependencies]
server = ["numpy>=1.24.0", "scipy>=1.10.0", "hnswlib>=0.8.0"]

[project.scripts]
sparkdb-cli    = "sparkdb.cli:main"
sparkdb-server = "sparkdb.server.server:run_server"

[tool.setuptools.packages.find]
include = ["sparkdb*"]   # ← discovers sparkdb, sparkdb.core, sparkdb.cypher, etc.
```

Running `pip install -e .[server]` installs the project in editable mode with all runtime dependencies. The final wheel is a pure-Python wheel (no compiled extensions of its own) that vendors numpy/scipy indirectly via the Docker image's `sparkdb_deps/` directory.

### 1.3 Docker Container: Copy & Restart Flow

The production `Dockerfile.sparkdb`:
1. **Base image**: `node:20-bookworm` — a Debian Bookworm image that ships with Python 3.11 already installed (via the NodeJS base).
2. **Working directory**: `/app`
3. **Environment variables set at build time**:
   - `PYTHONUNBUFFERED=1` — disables Python output buffering so logs appear immediately.
   - `SPARKDB_PORT=7379` — default server port.
   - `SPARKDB_STORAGE=/data/sparkdb` — default on-disk persistence directory.
   - `PYTHONPATH=/app/deps:/app` — makes pre-bundled wheels (`/app/deps/`) and SparkDB itself importable without `pip install`.
4. **COPY steps**:
   - `COPY docker/sparkdb_deps /app/deps` — copies pre-bundled numpy, scipy, hnswlib wheels (already extracted) so the container works with no network access.
   - `COPY sparkdb /app/sparkdb` — copies the entire `sparkdb/` source tree.
5. **Volume** `/data/sparkdb` — bind-mount for persistent snapshot and AOF files.
6. **EXPOSE 7379** — declares HTTP port.
7. **HEALTHCHECK**: Polls `http://localhost:7379/health` every 10 s; 3 retries allowed.
8. **ENTRYPOINT**: `python3 -m sparkdb.server.server 0.0.0.0 7379 /data/sparkdb` — starts the `ThreadingHTTPServer`.

To update and restart during development:
```bash
docker cp sparkdb/ sparkdb_container:/app/sparkdb
docker restart sparkdb_container
```
This copies changed Python files into the running container and restarts the process, picking up new code immediately without rebuilding the image.

---

## Section 2: SparkDB Entry Point & Multi-Tenancy

### 2.1 Package Entry Point (`sparkdb/__init__.py`)

```python
from sparkdb.client import SparkDBClient as SparkDB, GraphClient
from sparkdb.core.engine import GraphSpace
from sparkdb.cypher.executor import QueryResult

__version__ = "1.1.0"
```

`SparkDB` as imported by end users is actually `SparkDBClient` from `sparkdb/client/__init__.py`. This class provides the unified API for both **embedded** (in-process) and **remote** (Docker/HTTP) operating modes.

### 2.2 `SparkDB` Cluster-Level Class (`core/engine.py`, class `SparkDB`)

`SparkDB` is the top-level cluster manager. It maintains a dict of named `GraphSpace` instances and a single shared `storage_dir` on disk.

```
SparkDB
├── _lock: threading.RLock       ← cluster-level lock
├── storage_dir: str             ← absolute path to shared storage root
├── default_vector_dim: int      ← default HNSW index dimension (256)
└── _graphs: Dict[str, GraphSpace]  ← in-memory registry of active graph spaces
```

**`select_graph(graph_name, vector_dim, storage_mode, lru_cache_size) → GraphSpace`**

1. Acquires `_lock`.
2. If `graph_name` not in `_graphs`, instantiates `GraphSpace(name, vector_dim, storage_dir, storage_mode, lru_cache_size)`.
3. The new `GraphSpace.__init__()` triggers `_auto_restore()` which loads any existing snapshot + WAL.
4. Stores the `GraphSpace` in `_graphs[graph_name]`.
5. Returns the `GraphSpace`.

`select_project` is an alias for `select_graph`.

**`list_graphs() → List[str]`**

Merges in-memory `_graphs.keys()` with the list of subdirectories found in `storage_dir` (excluding hidden directories). This means graphs that were persisted in previous processes are listed even if not yet loaded into memory.

**`drop_graph(graph_name) → bool`**

1. Removes from `_graphs` if present.
2. Calls `shutil.rmtree()` on `storage_dir/graph_name/`.
3. Returns `True` if the directory existed.

**`drop_all_projects() → List[str]`**

1. Captures all currently known graph names via `list_graphs()`.
2. Clears `_graphs`.
3. Iterates `storage_dir` and `shutil.rmtree`s every subdirectory.
4. Returns list of dropped names.

**`checkpoint_all() → Dict[str, str]`**

Iterates all loaded `GraphSpace` instances, calls `g.checkpoint()` on each, and returns a dict of `{graph_name: snapshot_file_path}`.

### 2.3 `GraphSpace` Initialization — All 7 Subsystems

`GraphSpace.__init__(name, vector_dim, storage_dir, storage_mode, lru_cache_size)`:

```python
self._lock = threading.RLock()          # Subsystem 1: Thread safety lock
self.matrix_store = MatrixStore()        # Subsystem 2: Graph topology
if storage_mode == "hybrid":
    db_path = os.path.join(storage_dir, name, "properties.db")
    self.property_store = DiskPropertyStore(db_path, lru_cache_size)  # Subsystem 3a
else:
    self.property_store = PropertyStore()  # Subsystem 3b
self.vector_store = VectorStore(dimension=vector_dim)  # Subsystem 4
self.fulltext_store = FulltextStore()    # Subsystem 5
self.persistence = PersistenceEngine(name, storage_dir)  # Subsystem 6
self._schema: Dict[str, Any] = {}        # Subsystem 7: Ontology schema
self._slowlog: List[Dict] = []           # Query audit ring buffer (max 200 entries)
self.label_counts: Dict[str, int] = {}   # CBO cardinality statistics
self._auto_restore()                     # Crash recovery
```

**Multi-tenancy model**: Each `GraphSpace` uses its own subdirectory `storage_dir/name/`:
- `properties.db` — SQLite database for `DiskPropertyStore` (hybrid mode only)
- `snapshot.json` — full serialized graph state
- `mutations.aof` — append-only WAL file

All 7 subsystems are fully isolated per `GraphSpace`. Cross-graph queries are impossible by design.

### 2.4 `create_batch()` Bulk Creation Pipeline

`GraphSpace.create_batch(nodes, edges, log_aof=True)` is the highest-throughput insertion method:

1. **Acquire `_lock`** — single lock cycle for entire batch.
2. **Iterate nodes list**:
   - For each `n`: extract `labels`, `properties`, and optionally pop `embedding` from properties dict.
   - Increment `label_counts[lbl]` for each label.
   - Call `matrix_store.add_node(labels=lbls)` → gets back `nid`.
   - Accumulate `node_props_batch[nid] = props`.
   - If `emb` is present, call `vector_store.add_node_vector(nid, emb)`.
   - If `properties` has `text`, `description`, or `content` key, call `fulltext_store.index_node_text(nid, text)`.
   - Append `nid` to `node_ids`.
3. **Batch-write node properties**:
   - If `property_store` has `set_nodes_properties_batch()` (DiskPropertyStore), calls it — single `executemany` transaction.
   - Otherwise falls back to individual `set_node_properties()` calls.
4. **Iterate edges list**:
   - For each `e`: call `matrix_store.add_edge(src, rel, dst, weight)`.
   - If edge has `properties`, accumulate in `edge_props_batch`.
5. **Batch-write edge properties**: same fallback pattern as nodes.
6. **AOF log**: If `log_aof=True`, writes a single `create_batch` mutation record to the WAL.
7. **Returns** `(node_ids: List[int], len(edges))`.

### 2.5 `_auto_restore()` — Crash Recovery on Startup

```python
def _auto_restore(self) -> None:
    snapshot = self.persistence.load_snapshot()
    if snapshot:
        self._restore_from_dict(snapshot)

    mutations = self.persistence.replay_aof()
    if mutations:
        for m in mutations:
            cmd = m.get("cmd")
            p = m.get("params", {})
            if cmd == "create_node":
                self.create_node(p.get("labels"), p.get("properties"), ..., log_aof=False)
            elif cmd == "create_edge":
                self.create_edge(p.get("src"), p.get("rel"), p.get("dst"), ..., log_aof=False)
            elif cmd == "delete_node":
                self.delete_node(p.get("node_id"), log_aof=False)
            elif cmd == "delete_edge":
                self.delete_edge(p.get("src"), p.get("rel"), p.get("dst"), log_aof=False)
            elif cmd == "create_batch":
                for n in p.get("nodes", []):
                    self.create_node(n.get("labels"), n.get("properties"), ..., log_aof=False)
                for e in p.get("edges", []):
                    self.create_edge(e.get("src"), e.get("rel"), ..., log_aof=False)
```

Critical: all `log_aof=False` prevents WAL-replayed mutations from being written back to WAL.

`_restore_from_dict(data)`: Rebuilds from snapshot:
- For each node entry: calls `matrix_store.add_node(labels)`, then `property_store.set_node_properties()`, optionally `vector_store.add_node_vector()`, optionally `fulltext_store.index_node_text()`. Also updates `label_counts`.
- For each edge entry: calls `matrix_store.add_edge()` and optionally `property_store.set_edge_properties()`.

---

## Section 3: MatrixStore — The GraphBLAS Linear Algebra Core

File: `sparkdb/core/matrix_store.py`

### 3.1 Internal Data Structures

```python
class MatrixStore:
    _lock: threading.RLock
    node_capacity: int                              # doubly-expanded when needed
    node_count: int                                 # total nodes ever allocated (monotonic counter)
    _free_node_ids: List[int]                       # free-list for ID reuse after deletion
    _active_nodes: Set[int]                         # currently live node IDs

    labels: Dict[str, Set[int]]                     # label → set of node IDs
    node_to_labels: Dict[int, Set[str]]             # node ID → set of labels

    # Primary edge storage: coordinate-list format
    _rel_matrices: Dict[str, Dict[Tuple[int, int], float]]
    #               rel_type →  {(src_id, dst_id) → edge_weight}

    _csr_cache: Dict[str, sp.csr_matrix]            # compiled CSR per rel_type
    _csc_cache: Dict[str, sp.csc_matrix]            # compiled CSC per rel_type
    _rev_csr_cache: Dict[str, sp.csr_matrix]        # transposed CSR per rel_type
    _unified_csr: Optional[sp.csr_matrix]           # merged adjacency across all rel types
    _dirty_matrices: Set[str]                       # rel_types whose CSR/CSC caches are stale
    _unified_dirty: bool                            # whether unified CSR is stale
```

**Why coordinate dict, not scipy DOK/COO?** The Python dict `{(row, col): val}` allows O(1) single-edge insertion and deletion by key without any scipy overhead or re-materialization. CSR is only compiled from this dict when a traversal query actually needs it (lazy materialization).

### 3.2 Node Lifecycle

**`add_node(labels) → int`**:
1. If `_free_node_ids` is non-empty, pops the last ID (recycled from deleted node). Otherwise uses `node_count` as new ID and increments `node_count`.
2. If `node_count > node_capacity`, doubles `node_capacity` (capacity tracking for future matrix shapes).
3. Adds `node_id` to `_active_nodes`.
4. Initializes `node_to_labels[node_id] = set()`.
5. Marks all existing rel matrices dirty (because node count may change matrix shape): `_dirty_matrices.update(_rel_matrices.keys())`.
6. Sets `_unified_dirty = True`.
7. For each label, calls `add_node_label(node_id, lbl)`.
8. Returns `node_id`.

**`delete_node(node_id) → bool`**:
1. Checks `node_id in _active_nodes`; returns `False` if not found.
2. Removes from `_active_nodes`, appends to `_free_node_ids` (ID recycled for future `add_node` calls).
3. For each label in `node_to_labels[node_id]`: removes from `labels[lbl]`.
4. Deletes `node_to_labels[node_id]`.
5. **Incident edge cleanup**: For every `_rel_matrices[rel]`, collects all keys `(src, dst)` where `src == node_id OR dst == node_id`, deletes them, marks that `rel` dirty.
6. Returns `True`.

**Free-list ID reuse**: When a node is deleted, its ID goes onto `_free_node_ids`. The next `add_node()` call pops from this list, reusing the ID. This prevents unbounded growth of `node_count` and keeps matrices compact.

### 3.3 Label Sets

```python
labels: Dict[str, Set[int]]        # inverted: label → {node_id, ...}
node_to_labels: Dict[int, Set[str]] # forward:  node_id → {label, ...}
```

`add_node_label(node_id, label)`:
- Creates `labels[label]` set if not present.
- Adds `node_id` to `labels[label]`.
- Adds `label` to `node_to_labels[node_id]`.

`remove_node_label(node_id, label)`:
- Discards `node_id` from `labels[label]`.
- Discards `label` from `node_to_labels[node_id]`.

`get_nodes_with_label(label) → Set[int]`:
- Returns `labels.get(label, set()).intersection(_active_nodes)` — filters by active nodes to exclude deleted IDs still in label sets from a batch operation.

### 3.4 Edge Storage

```python
_rel_matrices[rel_type][(src_id, dst_id)] = float(weight)
```

`add_edge(src, rel_type, dst, weight=1.0)`:
1. Validates both `src` and `dst` are in `_active_nodes`.
2. Creates `_rel_matrices[rel_type]` dict if rel_type is new.
3. Stores `_rel_matrices[rel_type][(src, dst)] = float(weight)`.
4. Adds `rel_type` to `_dirty_matrices`, sets `_unified_dirty = True`.

`delete_edge(src, rel_type, dst) → bool`:
1. Checks existence: `rel_type in _rel_matrices and (src, dst) in _rel_matrices[rel_type]`.
2. Deletes the key.
3. Marks dirty.

### 3.5 Lazy CSR Materialization

**`get_csr(rel_type) → sp.csr_matrix`**:

```python
if rel_type not in _rel_matrices:
    return sp.csr_matrix((node_count, node_count), dtype=float32)  # empty

if rel_type in _dirty_matrices or rel_type not in _csr_cache:
    mat = _rel_matrices[rel_type]
    if not mat:
        _csr_cache[rel_type] = sp.csr_matrix((node_count, node_count), dtype=float32)
    else:
        rows = np.fromiter((k[0] for k in mat.keys()), dtype=int32, count=len(mat))
        cols = np.fromiter((k[1] for k in mat.keys()), dtype=int32, count=len(mat))
        vals = np.fromiter(mat.values(), dtype=float32, count=len(mat))
        _csr_cache[rel_type] = sp.csr_matrix((vals, (rows, cols)), shape=(node_count, node_count))
    _dirty_matrices.discard(rel_type)

return _csr_cache[rel_type]
```

The `_dirty_matrices` set is the invalidation flag. Only when a rel_type is marked dirty (after edge addition, deletion, or node changes) does `get_csr()` re-compile from the coordinate dict. Subsequent calls return the cached `csr_matrix` object — O(1) until the next mutation.

`np.fromiter` with `count=len(mat)` is used instead of `np.array(list(...))` because it avoids creating intermediate lists, making it ~30% faster for large coordinate dicts.

### 3.6 CSC and Reverse Adjacency

**`get_csc(rel_type) → sp.csc_matrix`**:
1. Returns empty if rel_type unknown.
2. If dirty or not cached: calls `get_csr(rel_type)` then `.tocsc()` (scipy's O(nnz) conversion).
3. Caches in `_csc_cache[rel_type]`.

**`get_reverse_adjacency(rel_type) → sp.csr_matrix`** (used by the CBO backward traversal):
1. Returns empty if rel_type unknown.
2. If dirty or not cached: calls `get_csr(rel_type)` then `.transpose().tocsr()`.
   - `.transpose()` returns a `csr_matrix` with rows/cols swapped (effectively the CSC transposed to CSR layout).
   - `.tocsr()` ensures the canonical CSR format with sorted column indices.
3. Caches in `_rev_csr_cache[rel_type]`.

The reverse adjacency matrix represents the graph with all edge directions flipped. If original has edge A→B, the reverse has B→A. Used by the CBO optimizer for backward traversal from the smaller candidate set.

### 3.7 `traverse_boolean_step()` — Single-Hop Boolean Semiring

```python
def traverse_boolean_step(self, frontier: np.ndarray, rel_type: str) -> np.ndarray:
    if frontier.shape[0] != self.node_count:
        # Pad frontier to match current node_count
        padded = np.zeros(self.node_count, dtype=bool)
        padded[:min(frontier.shape[0], self.node_count)] = frontier[:...]
        frontier = padded

    csr = self.get_csr(rel_type)
    if csr.nnz == 0:
        return np.zeros(self.node_count, dtype=bool)

    v_sparse = sp.csr_matrix(frontier.astype(np.float32))   # shape: (1, node_count)
    res_sparse = v_sparse.dot(csr)                            # (1, node_count) × (node_count, node_count)
    res_dense = np.asarray(res_sparse.todense()).flatten()    # flatten to 1D
    return res_dense > 0.0                                    # boolean mask: reachable nodes
```

**Mathematical semantics**: This implements a single step of BFS in the Boolean semiring (addition = OR, multiplication = AND). The frontier vector `v` has `v[i] = 1` for nodes in the current wavefront. After `v × A`, the result `r[j] = OR_i(v[i] AND A[i,j])`, meaning `r[j]` is `True` if any frontier node has an edge to `j`.

This is faster than Python-level adjacency list iteration because the `dot()` call dispatches to BLAS (compiled C with SIMD).

### 3.8 `get_unified_csr()` — All-Relationship Merged Adjacency

```python
def get_unified_csr(self) -> sp.csr_matrix:
    if self._unified_csr is not None and not self._unified_dirty:
        return self._unified_csr

    total_edges = sum(len(m) for m in _rel_matrices.values())
    if total_edges == 0:
        self._unified_csr = sp.csr_matrix((node_count, node_count), dtype=float32)
    else:
        rows = np.empty(total_edges, dtype=int32)
        cols = np.empty(total_edges, dtype=int32)
        vals = np.empty(total_edges, dtype=float32)
        offset = 0
        for mat in _rel_matrices.values():
            m_len = len(mat)
            rows[offset:offset+m_len] = [k[0] for k in mat.keys()]
            cols[offset:offset+m_len] = [k[1] for k in mat.keys()]
            vals[offset:offset+m_len] = list(mat.values())
            offset += m_len
        self._unified_csr = sp.csr_matrix((vals, (rows, cols)), shape=(node_count, node_count))
    self._unified_dirty = False
    return self._unified_csr
```

Used by BFS shortest path and PageRank when traversal is across all relationship types (no `rel_types` filter). The pre-allocated numpy arrays avoid repeated list concatenation.

---

## Section 4: PropertyStore & DiskPropertyStore

File: `sparkdb/core/property_store.py`

### 4.1 `PropertyStore` (Memory-Only)

```python
class PropertyStore:
    _lock: threading.RLock
    node_properties: Dict[int, Dict[str, Any]]              # node_id → {prop: val}
    edge_properties: Dict[Tuple[int,str,int], Dict[str,Any]] # (src,rel,dst) → {prop: val}
    _property_indexes: Dict[str, Dict[Any, Set[int]]]        # prop_key → {prop_val → {node_ids}}
```

**`set_node_properties(node_id, properties)`**:
1. Creates `node_properties[node_id] = {}` if not present.
2. For each `(key, val)` in `properties`:
   - If `key` is indexed: retrieves old value from current dict; discards `node_id` from `_property_indexes[key][old_val]`; inserts `node_id` into `_property_indexes[key][val]`.
   - Updates `current[key] = val`.

**`set_nodes_properties_batch(batch_props: Dict[int, Dict])`**:
Loops over `batch_props.items()` and applies the same incremental index update logic for each node. No difference in semantics, just a bulk convenience that avoids repeated lock acquisitions (single `with self._lock:` wraps the entire loop).

**`batch_get_nodes_properties(node_ids)`**:
```python
return {nid: self.node_properties.get(nid, {}) for nid in node_ids}
```
Pure dict comprehension — O(n) where n = len(node_ids). Returns direct dict references, not copies.

**`get_node_properties(node_id)`**:
```python
return self.node_properties.get(node_id, {})
```
Returns the stored dict reference directly — no copy allocation (zero-copy). Callers must not mutate the returned dict unless they intend to modify the stored state.

**`get_numeric_property_values(node_ids, property_name)`**:
Iterates `node_ids`, accesses `node_properties[nid][property_name]` directly (no JSON parsing), checks `isinstance(val, (int, float)) and not isinstance(val, bool)`, appends to result list. This bypasses any serialization overhead.

**`aggregate_numeric_property(node_ids, property_name, agg)`**:
```python
vals = self.get_numeric_property_values(node_ids, property_name)
if not vals:
    return 0.0 if agg in ("sum", "count") else None
# dispatch to sum(), avg(), min(), max(), count()
```
Operates entirely on Python native floats — no numpy, no JSON deserialization.

**Secondary inverted index creation** (`create_node_property_index(property_name)`):
Scans all `node_properties`, builds `idx[val].add(nid)` for every node that has the property, stores in `_property_indexes[property_name]`.

### 4.2 `DiskPropertyStore` (Hybrid SQLite + LRU)

```python
class DiskPropertyStore:
    _lock: threading.RLock
    db_path: str
    lru_cache_size: int                                    # default: 100_000
    _node_lru: OrderedDict[int, Dict[str, Any]]           # LRU node property cache
    _edge_lru: OrderedDict[Tuple[int,str,int], Dict]      # LRU edge property cache
```

#### SQLite Schema

```sql
CREATE TABLE node_properties (
    node_id INTEGER PRIMARY KEY,
    properties TEXT           -- JSON-encoded property dict
);

CREATE TABLE edge_properties (
    src INTEGER,
    rel_type TEXT,
    dst INTEGER,
    properties TEXT,          -- JSON-encoded property dict
    PRIMARY KEY (src, rel_type, dst)
);

CREATE TABLE property_index (
    prop_key TEXT,
    prop_val TEXT,            -- always stored as string (str(val))
    node_id INTEGER,
    PRIMARY KEY (prop_key, prop_val, node_id)
);
CREATE INDEX idx_prop_lookup ON property_index(prop_key, prop_val);
```

#### SQLite PRAGMAs — Explained

Set at `_init_db()` time (first connection):

| PRAGMA | Value | Purpose |
|---|---|---|
| `journal_mode` | `WAL` | Write-Ahead Logging: readers never block writers; writers never block readers. WAL file accumulates writes, checkpointed to main DB periodically. |
| `synchronous` | `NORMAL` | Flush to kernel buffer on each transaction; OS crash may lose last transaction, but SQLite crash doesn't. Avoids `fsync()` on every write (significant speedup). |
| `cache_size` | `-262144` | Negative = kilobytes → 256 MB in-memory page cache. Hot pages served without disk I/O. |
| `mmap_size` | `2147483648` | 2 GB memory-mapped I/O: the OS kernel maps database file pages directly into process address space. Reads become memory accesses, not `read()` syscalls. |
| `temp_store` | `MEMORY` | Temporary tables and indexes (used for sorting, GROUP BY, etc.) reside in RAM, not `/tmp`. |
| `busy_timeout` | `5000` | If another thread holds a write lock, retry for 5000 ms before raising `OperationalError`. Handles multi-thread contention. |
| `foreign_keys` | `OFF` (not set, default) | Foreign key enforcement is off by default in SQLite; not needed here since integrity is managed by MatrixStore. |

#### LRU Cache Mechanics

`_node_lru: OrderedDict` acts as a doubly-linked hash map. The oldest entry is at position `last=False` (front). Recent access moves a key to `last=True` (back). When `len > lru_cache_size`, `popitem(last=False)` evicts the least-recently-used entry.

**`get_node_properties(node_id)`** — read path:
1. If `node_id in _node_lru`: calls `_node_lru.move_to_end(node_id)` (marks recent), returns cached dict directly.
2. Otherwise: opens new SQLite connection, `SELECT properties FROM node_properties WHERE node_id = ?`, `json.loads(row[0])`, stores in `_node_lru`, evicts if overflow. Returns loaded dict.

**`batch_get_nodes_properties(node_ids)`** — batched read path:
1. Separates `node_ids` into LRU hits and misses (dedup via `seen_misses` set).
2. For misses: chunks into groups of 500, executes `SELECT node_id, properties FROM node_properties WHERE node_id IN (?,?,...)` per chunk.
3. `json.loads()` each row, populates `_node_lru` and `result` dict.
4. Returns `{nid: result.get(nid, {}) for nid in node_ids}`.

**`set_nodes_properties_batch(batch_props)`** — batched write path:
1. For each `(node_id, properties)`: reads current state, merges, updates LRU.
2. Builds `node_rows` list of `(node_id, json.dumps(current))`.
3. Builds `idx_rows` list of `(prop_key, str(val), node_id)` for all new properties.
4. Single SQLite connection: `executemany(INSERT ... ON CONFLICT DO UPDATE ...)` for node rows, then `executemany(INSERT OR IGNORE ...)` for index rows. Single `commit()`.

---

## Section 5: VectorStore — HNSW Vector Index

File: `sparkdb/core/vector_store.py`

### 5.1 What HNSW Is

**HNSW (Hierarchical Navigable Small World)** is a graph-based approximate nearest neighbor (ANN) index. It builds a multi-layer proximity graph:
- Layer 0: All nodes, densely connected to local neighbors.
- Layer i (i > 0): Exponentially sparser subset of nodes, forming "highways" for long-range navigation.
- Search: Starts at top layer, greedily descends toward the query, refining at each layer.

Construction parameters in SparkDB:
- `M=16`: each node has up to 16 bidirectional connections in the graph.
- `ef_construction=200`: size of the dynamic candidate list during construction (higher = better index quality, slower build).
- `ef=50`: query-time beam width (higher = more accurate results, slower search).
- `space="cosine"`: Cosine similarity metric (suitable for text/semantic embeddings).

**Time complexity**: ANN query is O(log n) amortized. Exact NN would be O(n).

### 5.2 Internal Storage

```python
_index: Optional[hnswlib.Index]       # HNSW index (hnswlib Apache 2.0 library)
_vectors: Dict[int, np.ndarray]       # node_id → float32 embedding vector (also used as fallback)
dimension: int                        # fixed at GraphSpace creation (default 256)
max_elements: int                     # initial capacity (20,000); doubles when exceeded
```

### 5.3 `add_node_vector(node_id, vector)`

1. Converts `vector` to `np.asarray(dtype=float32)`.
2. Validates dimension: raises `ValueError` if `vec.shape[0] != self.dimension`.
3. Stores `_vectors[node_id] = vec` (for fallback and snapshotting).
4. If `_index` is initialized and `len(_vectors) > max_elements`: doubles `max_elements`, calls `_index.resize_index(max_elements)`.
5. Calls `_index.add_items([vec], [node_id])`.

### 5.4 `query_nearest_nodes(query_vector, top_k=5)`

1. If `_vectors` is empty, returns `[]`.
2. Converts query to `float32`, clamps `k = min(top_k, len(_vectors))`.
3. **HNSW path** (primary): `labels, distances = _index.knn_query([q_vec], k=k)` → returns arrays of shape `(1, k)`. Zips into `[(node_id, distance), ...]`.
4. **NumPy fallback** (if HNSW unavailable or fails): Iterates all `_vectors`, computes cosine similarity `1 - dot(q,v) / (|q| * |v|)` for each, sorts ascending (lower = closer in cosine distance), returns top `k`.

**Why RAM**: HNSW requires the full graph in memory because search involves pointer chasing through the layer graph. numpy SIMD operations on float32 arrays run at peak CPU throughput (AVX2/AVX512 on modern hardware).

---

## Section 6: FulltextStore — BM25 Text Search

File: `sparkdb/core/fulltext_store.py`

### 6.1 What BM25 Is

**BM25 (Best Match 25)** is a probabilistic IR ranking function. For a term `t` in document `d`:

```
score(t, d) = IDF(t) × TF(t, d) × (k1 + 1) / (TF(t, d) + k1 × (1 - b + b × dl/avgdl))
```

Where:
- `IDF(t) = log((N - df + 0.5) / (df + 0.5) + 1)` — penalizes common terms
- `TF(t, d)` — term frequency in document
- `dl` — document length (token count)
- `avgdl` — average document length across corpus
- `k1=1.5`, `b=0.75` (SparkDB defaults) — tuning parameters

### 6.2 `index_node_text(node_id, text)`

1. `_tokenize(text)`: `re.findall(r'\b\w+\b', text.lower())` — lowercase word-boundary tokenization.
2. `_doc_lengths[node_id] = len(tokens)`.
3. `tf_map = Counter(tokens)`.
4. For each `(term, freq)`: `_inverted_index[term][node_id] = freq`.
5. Updates `_avg_doc_len = sum(_doc_lengths.values()) / len(_doc_lengths)`.

### 6.3 `search(query, top_k=10)`

1. Tokenizes query.
2. For each query term:
   - Computes `IDF` from `_inverted_index[term]` document frequency.
   - For each `(doc_id, tf)` in the posting list: computes BM25 score and accumulates into `scores[doc_id]`.
3. Sorts `scores.items()` descending by score.
4. Returns top `top_k` as `[(node_id, score), ...]`.

---

## Section 7: PersistenceEngine — Durability Layer

File: `sparkdb/core/persistence.py`

### 7.1 AOF (Append-Only File) WAL

```python
def append_mutation(self, command: str, params: Dict[str, Any]) -> None:
    with self._lock:
        record = json.dumps({"cmd": command, "params": params}, default=str)
        handle = self._get_aof_handle()
        handle.write(record + "\n")
        handle.flush()               # ← immediately flushes to kernel buffer
```

Key points:
- The file handle is opened once in append mode (`"a"`) with 64 KB write buffer (`buffering=65536`).
- `handle.flush()` is called after every write — this pushes the Python buffer to the OS kernel. Combined with `synchronous=NORMAL` SQLite, this means mutations survive a Python crash (but not a hard power failure).
- `_get_aof_handle()` reopens the file if it was closed (e.g., after a snapshot).

### 7.2 `replay_aof() → List[Dict]`

```python
def replay_aof(self) -> List[Dict]:
    with self._lock:
        if self._aof_handle:
            self._aof_handle.flush()     # flush in-flight writes
        mutations = []
        with open(self.aof_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        mutations.append(json.loads(line))
                    except Exception:
                        pass            # corrupted line is silently skipped
        return mutations
```

Returns all mutations since the last snapshot reset (the snapshot clears the AOF).

### 7.3 `save_snapshot(state_dict) → str`

```python
def save_snapshot(self, state_dict: Dict) -> str:
    with self._lock:
        # 1. Close AOF handle to release file lock
        if self._aof_handle:
            self._aof_handle.close()
            self._aof_handle = None

        # 2. Write to temporary file (atomic write)
        temp_fd, temp_path = tempfile.mkstemp(dir=graph_dir, prefix="snap_", suffix=".tmp")
        with os.fdopen(temp_fd, "w") as f:
            json.dump(state_dict, f, indent=2, default=str)

        # 3. Atomic rename (POSIX rename is atomic)
        shutil.move(temp_path, self.snapshot_file)

        # 4. Reset AOF
        with open(self.aof_file, "w") as f:
            f.truncate(0)

        return self.snapshot_file
```

`tempfile.mkstemp` + `shutil.move` ensures the snapshot file is never partially written — if the process crashes during write, the `snap_*.tmp` file exists but the `snapshot.json` remains at its last valid state.

### 7.4 `load_snapshot() → Optional[Dict]`

```python
def load_snapshot(self) -> Optional[Dict]:
    if not os.path.exists(self.snapshot_file):
        return None
    try:
        with open(self.snapshot_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(...)
        return None
```

Returns `None` on file-not-found or JSON parse error (corrupted snapshot). `_auto_restore` handles `None` gracefully.

### 7.5 Crash Recovery Flow

```
Process restart
     │
     ▼
GraphSpace.__init__()
     │
     ▼
_auto_restore()
     │
     ├─► PersistenceEngine.load_snapshot()
     │       └─► json.load(snapshot.json) → state_dict (or None)
     │
     ├─► if state_dict: _restore_from_dict(state_dict)
     │       └─► rebuilds matrix_store, property_store, vector_store from snapshot
     │
     └─► PersistenceEngine.replay_aof()
             └─► reads mutations.aof line by line
             └─► re-applies each mutation (create_node, create_edge, etc.)
                  with log_aof=False to avoid double-logging
```

---

## Section 8: Cypher Parser — Lexer & Recursive Descent

File: `sparkdb/cypher/parser.py`

### 8.1 `LRUCache` Class

```python
class LRUCache:
    capacity: int                    # 2048 (enforced minimum 2000)
    _cache: OrderedDict[str, Any]    # key → value, insertion order = LRU order
    _lock: threading.RLock
    _hits: int
    _misses: int
```

Operations:
- **`get(key)`**: If key exists, moves to end (recently used), increments `_hits`. Otherwise `_misses++`, returns `default`.
- **`set(key, value)`**: If key exists, moves to end. Stores value. If `len > capacity`, calls `popitem(last=False)` to evict LRU entry.
- **`stats()`**: Returns `{size, capacity, hits, misses}`.

Thread safety: every operation holds `_lock` (RLock allows re-entrant acquisition from the same thread).

### 8.2 Two-Tier Plan Cache Architecture

```python
_COMPILED_PLAN_CACHE: LRUCache = LRUCache(capacity=2048)   # resolved query → CypherStatement
_TEMPLATE_PLAN_CACHE: LRUCache = LRUCache(capacity=2048)   # template query → CypherStatement
_PARSED_CACHE = _COMPILED_PLAN_CACHE                        # alias for backward compat
```

- **Tier 1 (`_COMPILED_PLAN_CACHE`)**: Caches the fully resolved (post-parameter-substitution) query string mapped to its parsed `CypherStatement`. O(1) lookup by exact string.
- **Tier 2 (`_TEMPLATE_PLAN_CACHE`)**: Caches parameterized template forms (with literal values extracted to `$__p0`, `$__p1`, etc.) mapped to a template `CypherStatement`. Allows reuse of parse results across queries that differ only in literal values.

### 8.3 `extract_template(query, params) → (template_str, extracted_params)`

This method canonicalizes a query with embedded literals into a generic template:

**Input**: `"MATCH (u:User {name: 'Alice', age: 30}) RETURN u"`
**Output template**: `"MATCH (u:User {name: $__p0, age: $__p1}) RETURN u"`
**Output params**: `{"__p0": "Alice", "__p1": 30}`

The implementation:
1. Uses `re.sub(r'\{([^{}]*:[^{}]*)\}', repl_prop_map, query.strip())` — finds all property map `{...}` blocks.
2. For each block, uses a nested regex to find `key: value` pairs where `value` matches string/numeric/bool/null literals or existing `$param` references.
3. Each matched literal is replaced with `$__pN` (auto-incremented `param_idx`), and the literal value is stored in `extracted: Dict`.
4. Returns the template string and extracted param dict.
5. If `params` dict was provided, any user params not extracted (because they were already `$param` references) are merged into `extracted`.

`vecf32(...)` embedding syntax is exempt from template extraction (contains arrays that should not be parameterized).

### 8.4 `_bind_params_to_ast(obj, params) → obj`

Deep recursive clone of an AST structure (dict/list/str/primitive), substituting parameter references:

```python
if isinstance(obj, str) and obj.startswith("$") and obj[1:] in params:
    return params[obj[1:]]        # substitute: "$__p0" → "Alice"
if isinstance(obj, dict):
    # Special case: "where" and "raw_query" string values get substitute_params() applied
    # All other values are recursively processed
if isinstance(obj, list):
    return [_bind_params_to_ast(elem, params) for elem in obj]
```

This allows the template AST (which has `$__p0` strings inside) to have those references replaced with concrete values, producing a fully-bound AST without re-parsing.

### 8.5 `_raw_parse(q) → CypherStatement`

The primary dispatch table (order matters, checked from top to bottom):

| Priority | Pattern | Handler |
|---|---|---|
| 0 | `q_upper.startswith("EXPLAIN")` | Strip prefix, recursively parse inner, return `EXPLAIN` stmt |
| 0 | `q_upper.startswith("PROFILE")` | Strip prefix, recursively parse inner, return `PROFILE` stmt |
| 1 | `q_upper.startswith("CALL")` | `_parse_call(q)` |
| 2 | `re.match(r"^DROP\s+(VECTOR\s+)?INDEX", q, IGNORECASE)` | `_parse_drop_index(q)` |
| 3 | `re.match(r"^CREATE\s+(VECTOR\s+)?INDEX", q, IGNORECASE)` | `_parse_create_index(q)` |
| 4 | `q_upper.startswith("MERGE")` | `_parse_merge(q)` |
| 5 | `q_upper.startswith("CREATE")` | `_parse_create(q)` |
| 6 | `q_upper.startswith("MATCH")` | `_parse_match(q)` |
| 7 | `q_upper.startswith("RETURN")` | `CypherStatement("RETURN_EXPR", ...)` |

Each parser uses `re.search()` and `re.match()` with `IGNORECASE` and `DOTALL` flags. No hand-written tokenizer — the regex patterns form the "lexer" implicitly.

### 8.6 `parse(query, params) → CypherStatement` — Three-Tier Resolution

```
CypherParser.parse(query, params)
│
├── Step 1: substitute_params(q_strip, params)  → resolved_q
│
├── Tier 1: _COMPILED_PLAN_CACHE.get(resolved_q)
│   └── HIT: return cached CypherStatement immediately (O(1))
│
├── Tier 2: extract_template(q_strip, params) → (template_q, extracted_params)
│   └── if template_q in _TEMPLATE_PLAN_CACHE:
│       ├── template_stmt = _TEMPLATE_PLAN_CACHE.get(template_q)
│       ├── bound_details = _bind_params_to_ast(template_stmt.details, extracted_params)
│       ├── stmt = CypherStatement(template_stmt.type, bound_details)
│       ├── _COMPILED_PLAN_CACHE.set(resolved_q, stmt)  # also cache exact form
│       └── return stmt
│
└── Tier 3: Raw parse
    ├── if template_q != resolved_q:
    │   ├── template_stmt = _raw_parse(template_q)
    │   ├── _TEMPLATE_PLAN_CACHE.set(template_q, template_stmt)
    │   ├── bound_details = _bind_params_to_ast(template_stmt.details, extracted_params)
    │   ├── stmt = CypherStatement(template_stmt.type, bound_details)
    │   └── _COMPILED_PLAN_CACHE.set(resolved_q, stmt)
    └── else:
        ├── stmt = _raw_parse(resolved_q)
        └── _COMPILED_PLAN_CACHE.set(resolved_q, stmt)
```

**All returned `CypherStatement` objects are immutable after creation** (details is a plain dict, not a live reference into the cache). The template AST stored in `_TEMPLATE_PLAN_CACHE` is never mutated — `_bind_params_to_ast` always produces a new dict.

### 8.7 All Statement Types

| Type | Details dict keys |
|---|---|
| `CREATE` | `nodes`, `edges`, `return` |
| `MATCH` | `nodes`, `rels`, `rel_info`, `where`, `set`, `remove`, `delete`, `detach`, `return`, `order_by`, `skip`, `limit` |
| `MERGE` | `var`, `label`, `properties`, `on_create_set`, `on_match_set`, `return`, `raw_query` |
| `CALL` | `procedure`, `args`, `yield`, `chained_query` |
| `CREATE_INDEX` | `is_vector`, `label`, `property`, `options` |
| `DROP_INDEX` | `is_vector`, `label`, `property` |
| `SHORTEST_PATH` | `src_var`, `dst_var`, `raw_query` |
| `EXPLAIN` | `inner` (CypherStatement), `raw_query` |
| `PROFILE` | `inner` (CypherStatement), `raw_query` |
| `RETURN_EXPR` | `expr` |

---

## Section 9: Cypher Executor — Query Execution Engine

File: `sparkdb/cypher/executor.py`

### 9.1 `QueryResult` Class

```python
class QueryResult:
    header: List[str]                 # column names
    result_set: List[List[Any]]       # rows × columns
    execution_time_ms: float          # wall-clock time of execute()
    nodes_created: int
    nodes_deleted: int
    relationships_created: int
    relationships_deleted: int
    properties_set: int
    indices_created: int
    indices_deleted: int
```

Matches FalkorDB's client API (`falkordb.QueryResult`), enabling drop-in compatibility.

### 9.2 `execute()` — Statement Dispatch Table

```python
def execute(self, query: str, params=None) -> QueryResult:
    t0 = time.perf_counter()
    stmt = CypherParser.parse(query, params=params)

    # dispatch by stmt.type:
    EXPLAIN       → _execute_explain()
    PROFILE       → _execute_profile()
    MERGE         → _execute_merge()
    CREATE_INDEX  → _execute_create_index()
    DROP_INDEX    → _execute_drop_index()
    CALL          → _execute_call()
    SHORTEST_PATH → _execute_shortest_path()
    CREATE        → _execute_create()
    MATCH         → _execute_match()

    exec_time = (time.perf_counter() - t0) * 1000
    res.execution_time_ms = exec_time
    self.graph.record_query_log(query, exec_time, len(res.result_set))
    return res
```

### 9.3 `_execute_create()` — Bulk Node + Edge Creation

1. Builds `bulk_nodes` list from `details["nodes"]`.
2. Calls `self.graph.create_batch(nodes=bulk_nodes, edges=[], log_aof=False)` → creates nodes, gets `node_ids`.
3. Maps variable names to IDs: `var_to_id[n["var"]] = node_ids[i]`.
4. Builds `bulk_edges` using `var_to_id` to resolve `src_var` and `dst_var` to concrete IDs.
5. If edges present: calls `self.graph.create_batch(nodes=[], edges=bulk_edges, log_aof=False)`.
6. **Manual AOF write**: `self.graph.persistence.append_mutation("create_batch", {...})` — single WAL entry for entire CREATE statement.
7. If `RETURN` clause present: resolves projections for created nodes.

### 9.4 `_find_candidate_node_ids(label, properties) → List[int]`

This is the **central hot-path function** for all MATCH and MERGE queries.

```python
def _find_candidate_node_ids(self, label, properties) -> List[int]:
    # Fast-path: property filter exists
    if properties:
        candidates: Optional[Set[int]] = None
        for k, v in properties.items():
            matched = self.graph.property_store.find_nodes_by_property(k, v)
            # Intersect: AND semantics across multiple properties
            if candidates is None:
                candidates = set(matched)
            else:
                candidates = candidates.intersection(matched)
            if not candidates:
                return []             # early exit if intersection is empty

        # Filter by active + label
        active = self.graph.matrix_store.get_active_nodes()
        result = [nid for nid in candidates if nid in active
                  and (label is None or label in node_to_labels.get(nid, set()))]
        return sorted(result)

    # Label-only path
    if label:
        return sorted(list(self.graph.matrix_store.get_nodes_with_label(label)))

    # No filter: return all active nodes
    return sorted(list(self.graph.matrix_store.get_active_nodes()))
```

**For indexed properties** (`PropertyStore._property_indexes[k][v]` or `DiskPropertyStore.find_nodes_by_property()`): The property index lookup is O(1) hash access, returning a pre-computed set of matching node IDs. This eliminates full label-set scans for filtered queries.

### 9.5 `_execute_match()` Full Pipeline

```
1. Parse all pattern nodes from details["nodes"]
2. _find_candidate_node_ids(start_label, start_props) → start_ids
3. If start_ids empty → return empty QueryResult early

4. Cost-Based Optimizer (CBO) — applies when len(rels) ∈ {1, 2} and pattern is a chain:
   a. _find_candidate_node_ids(end_label, end_props) → end_ids
   b. If end_ids empty → prune immediately (early exit)
   c. For 2-hop: also check mid_ids
   d. If can_reverse AND len(end_ids) < len(start_ids) / 3:
      → Execute backward traversal: _execute_backward_traversal(nodes, rels, start_ids, end_ids, fetch_limit)
        - Gets reverse CSR for each rel via get_reverse_adjacency()
        - For 1-hop: iterates end_ids, looks up CSR.indptr[dst:dst+1] for src neighbors
        - For 2-hop: double reverse CSR traversal
   e. Otherwise fall through to forward traversal

5. Forward traversal (if CBO chose forward or no rels):
   algos.multi_hop_paths(matrix_store, start_ids, rels, target_label, max_paths=fetch_limit)

6. Filter valid paths:
   For each path p:
     a. Check inline property constraints (nodes[i].properties vs actual node props)
     b. If WHERE clause: build var_map, evaluate _evaluate_where(where_str, var_map)
     c. Append to valid_paths if passes all filters

7. Mutating operations (SET, REMOVE, DELETE):
   For each valid path, apply set_ops, remove_ops, delete_targets

8. Projection:
   If has_aggregation:
     → _compute_aggregations(projections, valid_paths, ...)
       - Groups by non-aggregate columns
       - For each group, calls _eval_aggregate_fn()
       - Fast path for sum/avg/min/max: calls aggregate_numeric_property() if node_idx found
   Else:
     → For each path: _build_var_map(p, nodes, rels, rel_info) → var_to_node
       → For each (expr, alias) in projections: _resolve_projection_value(expr, var_to_node, p)

9. ORDER BY:
   For each sort_spec (reversed list for stable multi-key sort):
     Resolves col_idx from projections, sorts rows in-place

10. SKIP / LIMIT:
    rows = rows[skip:]
    rows = rows[:limit]

11. Return QueryResult(header, result_set, ...)
```

### 9.6 `_execute_merge()` — Upsert

1. `_find_candidate_node_ids(label, properties)` → candidates.
2. If candidates non-empty: uses `candidates[0]` as existing node; applies `ON MATCH SET` operations.
3. If empty: calls `graph.create_node(labels=[label], properties=props, embedding=emb)` → new node; applies `ON CREATE SET` operations.
4. If `RETURN` clause: resolves and returns projected row.

### 9.7 `_execute_call()` — Procedure Dispatch

| Procedure (case-insensitive, `.lower()` matched) | Action |
|---|---|
| `algo.pagerank` | Calls `algos.pagerank(matrix_store, rel_types)` → power iteration |
| `algo.wcc` | Calls `algos.weakly_connected_components(matrix_store)` |
| `algo.trianglecount` | Calls `algos.triangle_count(matrix_store)` |
| `db.idx.vector.querynodes` | Calls `vector_store.query_nearest_nodes(vec, top_k)` |
| `db.idx.fulltext.querynodes` | Calls `fulltext_store.search(q_text, top_k)` |
| `db.indexes` | Returns list of indexed properties + vector index |
| `db.slowlog` | Returns `graph.get_slowlog()` ring buffer |
| `db.labels` | Returns sorted label names |
| `db.relationshiptypes` | Returns sorted relationship type names |
| `db.propertykeys` | Scans all active nodes and collects property key names |
| `db.schema` / `db.ontology` | Calls `graph.get_ontology()` |

---

## Section 10: Algorithms

### 10.1 `pathfinding.py`

**`multi_hop_paths(matrix_store, start_node_ids, rel_path, target_label, max_paths)`**:

```python
current_paths = [[nid] for nid in start_node_ids]

for rel in rel_path:
    csr = matrix_store.get_csr(rel)
    if csr.nnz == 0:
        return []
    next_paths = []
    for path in current_paths:
        curr = path[-1]
        r_start = csr.indptr[curr]
        r_end   = csr.indptr[curr + 1]
        for nbr in csr.indices[r_start:r_end]:    # ← CSR row slice: O(out-degree)
            next_paths.append(path + [int(nbr)])
            if max_paths and len(next_paths) >= max_paths:
                break                               # ← early exit
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
```

The CSR row slice `csr.indices[r_start:r_end]` is a numpy array slice — zero-copy view into the CSR indices array. Iterating it is much faster than a Python list because numpy arrays have denser memory layout.

**`shortest_path(matrix_store, source_id, target_id, rel_types)`**:
BFS using `collections.deque`. If only one rel_type: uses `get_csr()`. If no rel_types specified: uses `get_unified_csr()`. Tracks `parent` dict for path reconstruction. Returns first path found (shortest by hop count).

**`dijkstra_shortest_path(matrix_store, source_id, target_id, rel_types)`**:
Min-heap BFS (`heapq`) over edge weights. Uses CSR data array for weights. Returns `(path, total_distance)`.

### 10.2 `centrality.py`

**`pagerank(matrix_store, rel_types, damping=0.85, max_iter=50, tol=1e-6)`**:

1. Build combined CSR across all rel_types (or use `get_unified_csr()`).
2. Compute out-degree from `np.diff(combined.indptr)`.
3. Identify dangling nodes (out-degree = 0).
4. Build **column-stochastic transition matrix**: scale each row by `1/out_degree`, then transpose to CSR for efficient matrix-vector product.
5. Power iteration: `next_rank = damping * (P.dot(rank) + dangling_sum/n) + (1-damping)/n`
6. Converge when `|next_rank - rank|.sum() < tol`.
7. Returns `{node_id: score}` dict.

**`degree_centrality(matrix_store, rel_types)`**:
Uses `np.diff(combined.indptr)` for out-degrees (CSR row lengths) and `np.diff(combined.tocsc().indptr)` for in-degrees (CSC column lengths).

### 10.3 `community.py`

**`weakly_connected_components(matrix_store)`**:
Uses `scipy.sparse.csgraph.connected_components(combined, directed=False)` — treats directed graph as undirected for WCC.

**`strongly_connected_components(matrix_store)`**:
Uses `scipy.sparse.csgraph.connected_components(combined, directed=True, connection="strong")` — Tarjan/Kosaraju internally.

**`label_propagation(matrix_store, max_iter=30)`**:
1. Symmetrizes: `symmetric = (combined + combined.T).tocsr()`.
2. Initializes each node as its own community: `labels = np.arange(n)`.
3. Per iteration: random node order (`np.random.permutation`); for each node, take the majority label among its neighbors; if changed, mark `changed=True`.
4. Terminates when no labels change or `max_iter` reached.

### 10.4 `structural.py`

**`triangle_count(matrix_store)`**:
Uses the identity: triangles = Trace(A³) / 6 for an undirected unweighted adjacency matrix A.
1. Build undirected: `adj = ((combined + combined.T) > 0).astype(float32)`.
2. Remove self-loops: `adj.setdiag(0)`.
3. `a2 = adj.dot(adj)` — A squared.
4. `trace_a3 = adj.multiply(a2).sum()` — elementwise A * A² summed = Trace(A³).
5. Return `int(round(trace_a3 / 6.0))`.

### 10.5 `provenance.py`

**`trace_provenance(matrix_store, property_store, entity_node_id)`**:
Looks up `MENTIONED_IN` relationship CSR. Reads `csr.indices[csr.indptr[entity_node_id]:csr.indptr[entity_node_id+1]]` — all chunk node IDs that the entity is mentioned in. Returns their properties.

---

## Section 11: HTTP Server

Files: `sparkdb/server/app.py`, `sparkdb/server/server.py`

### 11.1 `ThreadingHTTPServer`

```python
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

server = ThreadingHTTPServer(("0.0.0.0", 7379), SparkDBRequestHandler)
server.serve_forever()
```

`ThreadingHTTPServer` inherits from both `ThreadingMixIn` and `HTTPServer`. `ThreadingMixIn` overrides `process_request()` to spawn a new `threading.Thread` for each incoming connection. Each thread runs `finish_request()` → `setup()` → `handle()` → `finish()`.

There is **no thread pool** — every request gets a new thread. For SparkDB's use case (graph queries that take milliseconds), this is acceptable. Thread creation overhead (~0.1 ms) is negligible vs query time.

### 11.2 `address_string()` Override — Critical Performance Fix

```python
def address_string(self) -> str:
    return self.client_address[0]    # return raw IP, not resolved hostname
```

The default `BaseHTTPRequestHandler.address_string()` performs a DNS reverse lookup (`socket.getfqdn(host)`) which can block for 10–30 seconds on private VPN/LAN IPs with no PTR records. This override bypasses the DNS lookup entirely.

### 11.3 `_send_payload(status, payload)` — MessagePack / JSON Content Negotiation

```python
def _send_payload(self, status, payload):
    accept = self.headers.get("Accept", "")
    qs = parse_qs(urlparse(self.path).query)
    format_param = qs.get("format", [""])[0].lower()

    use_msgpack = HAS_MSGPACK and (
        "application/msgpack" in accept or format_param == "msgpack"
    )
    if use_msgpack:
        body = msgpack.packb(payload, default=str)
        content_type = "application/msgpack"
    else:
        body = json.dumps(payload, default=str).encode("utf-8")
        content_type = "application/json"

    self.send_response(status)
    self.send_header("Content-Type", content_type)
    self.send_header("Content-Length", str(len(body)))
    self.send_header("Connection", "close")
    self.end_headers()
    self.wfile.write(body)
```

Format selection priority:
1. `Accept: application/msgpack` header
2. `?format=msgpack` query parameter
3. Default: JSON

### 11.4 All REST Endpoints

| Method | Path | Description | Request Body | Response |
|---|---|---|---|---|
| `GET` | `/health` | Server status | — | `{status, engine, host_ip, port, active_projects, active_graphs}` |
| `GET` | `/graphs` | List all graphs | — | `{projects: [...], graphs: [...]}` |
| `GET` | `/projects` | Alias for `/graphs` | — | same |
| `GET` | `/ontology?project=<name>` | Schema/metamodel | — | `{project, node_labels, property_keys_by_label, relationship_types, relationship_schema, property_keys_by_relationship}` |
| `GET` | `/schema?project=<name>` | Alias for `/ontology` | — | same |
| `POST` | `/query` | Execute Cypher | `{project, query, params?}` | `{header, result_set, execution_time_ms, nodes_created, ...}` |
| `POST` | `/checkpoint` | Save all snapshots | — | `{status: "ok", snapshots: {graph: path, ...}}` |
| `POST` | `/drop` | Drop one graph | `{project}` | `{status: "ok"|"not_found", project}` |
| `POST` | `/drop_all` | Drop all graphs | — | `{status: "ok", dropped: [...]}` |

Error responses: `{error: "message"}` with HTTP 400 (bad request) or 500 (server error).

---

## Section 12: Remote Client

Files: `sparkdb/client/remote_client.py`, `sparkdb/client/__init__.py`

### 12.1 `SparkDBClient` — Mode Selection

```python
class SparkDBClient:
    # Mode resolution logic (simplified):
    if url:                          → remote mode, base_url = url
    elif mode == "remote":           → remote mode
    elif mode == "embedded":         → embedded mode
    elif host is not None:           → remote mode
    elif storage_dir is not None:    → embedded mode
    else:                            → embedded mode (default)

    if embedded:
        self._engine = EngineSparkDB(storage_dir=..., default_vector_dim=...)
    else:
        self._engine = None          # no local engine
```

`SparkDB = SparkDBClient` (alias at module level).

### 12.2 `RemoteGraphClient` — HTTP + MessagePack Client

```python
class RemoteGraphClient:
    name: str           # graph/project name
    base_url: str       # e.g., "http://10.164.241.54:7379"
```

**`_get_headers()`**:
```python
headers = {"Content-Type": "application/json"}
if HAS_MSGPACK:
    headers["Accept"] = "application/msgpack, application/json"
return headers
```

If `msgpack` is installed, the client always requests binary format. The server responds with `Content-Type: application/msgpack` which the client detects in `_decode_response()`.

**`_post(endpoint, payload)`**:
1. `json.dumps(payload).encode("utf-8")` — request body is always JSON (response may be msgpack).
2. `urllib.request.Request(url, data, headers, method="POST")`.
3. `urllib.request.urlopen(req, timeout=30)` — blocking HTTP call.
4. Wraps response in `HttpResponseWrapper` (reads `.content` bytes).
5. Calls `_decode_response(response)`.

**`_decode_response(response)`**:
```python
content_type = response.headers.get("Content-Type", "")
if "application/msgpack" in content_type and HAS_MSGPACK:
    return msgpack.unpackb(response.content, raw=False)
else:
    return json.loads(response.content.decode("utf-8"))
```

**`query(cypher, params) → QueryResult`**:
1. Builds payload `{project, graph, query, params}`.
2. Calls `_post("/query", payload)`.
3. Constructs `QueryResult` from response dict.

### 12.3 `GraphClient` — Embedded In-Process Graph Wrapper

Wraps a `GraphSpace` and a dedicated `CypherExecutor`:

```python
class GraphClient:
    _space: GraphSpace
    _executor: CypherExecutor
    name: str
```

`query(cypher, params)` → `self._executor.execute(cypher, params=params)` → `QueryResult`

All operations (create index, drop index, checkpoint, etc.) delegate directly to `_space` without any network hop.

---

## Section 13: Test Suite

### `test_sparkdb_complete.py` — 6 Core Engine Tests

1. **`test_cypher_create_and_match`**: Creates 5 nodes + 4 edges via Cypher, then 2-hop MATCH traversal.
2. **`test_cypher_shortest_path`**: Creates 3-node chain, queries `shortestPath((a)-[*]->(c))`.
3. **`test_cypher_call_algorithms`**: Creates a cycle, verifies PageRank (~1/3 per node), WCC (1 component), TriangleCount (1).
4. **`test_vector_and_fulltext_search`**: Creates chunks with BM25 text and HNSW embeddings, verifies both search paths.
5. **`test_persistence_snapshot_and_aof`**: Creates graph, checkpoints, adds more nodes, restarts with new `SparkDB` instance, verifies all 3 nodes present.
6. **`test_high_concurrency`**: 8 threads × 50 writes, verifies final count.

### `test_sparkdb_hybrid_storage.py` — 5 DiskPropertyStore + WAL Tests

Tests DiskPropertyStore SQLite WAL mode, LRU cache behavior, batch writes, and persistence across restarts.

### `test_sparkdb_mutations_and_ontology.py` — 5 Mutation + Ontology Tests

Tests SET, REMOVE, DELETE via MATCH, and `get_ontology()` schema extraction.

### `test_falkordb_advanced_parity.py` — 7 FalkorDB API Parity Tests

Runs against a live FalkorDB container on port 6379. Verifies SparkDB's API surface matches FalkorDB's Python client (`falkordb` package): `select_graph()`, `query()`, `ro_query()`, `create_node_range_index()`, etc.

### `test_sparkdb_remote_client.py` — 2 Remote HTTP Client Tests

Runs against a live SparkDB Docker container on port 7379. Tests `SparkDBClient(host=..., port=...)` remote mode for `query()` and `list_projects()`.

### `test_cbo_and_ast_cache.py` — 7 CBO + AST Plan Cache Tests

Verifies:
- Tier 1 exact cache hits (same query twice → second is cache hit)
- Tier 2 template cache (two queries with different literal values → share template AST)
- CBO backward traversal triggers when `len(end_ids) < len(start_ids) / 3`
- CBO prunes when destination candidate set is empty
- `get_cache_stats()` reports correct hits/misses

### `test_storage_optimization_and_binary_protocol.py` — 7 Batch + MessagePack Tests

Verifies:
- `set_nodes_properties_batch()` bulk write correctness
- `aggregate_numeric_property()` fast path results
- MessagePack binary protocol encoding/decoding in server round-trip
- `batch_get_nodes_properties()` LRU hit and miss paths
