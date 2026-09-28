# SparkDB Changelog & Migration Guide

All notable changes, new capabilities, and migration steps for SparkDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.1] - 2026-09-28

### 🚀 Highlights & Performance Breakthroughs

* **Resolved ISSUE-12 (Vectorized Property Pre-fetching)**:
  * Eliminated the **N+1 query problem** during Cypher pattern matching and path traversal.
  * In previous releases, queries with candidate paths executed individual SQLite round-trips for every node in every path during property filtering, `WHERE` clauses, aggregations, and `RETURN` projections.
  * In v1.1.1, all unique candidate node IDs (`all_nids`) are aggregated and pre-fetched in a single batched SQLite call (`batch_get_nodes_properties()`). Lookups throughout query execution now run against an in-memory cache with $O(1)$ complexity.
  * **Result**: Latency drops by up to **98%** on dense path traversals and high-volume pattern queries.

* **Vector & Full-Text Search Procedure Optimization**:
  * Optimized `CALL db.idx.vector.querynodes()` and `CALL db.idx.fulltext.querynodes()`.
  * Pre-fetches properties for all top-$K$ matched nodes in a single batch, avoiding iterative property retrieval.

* **Automated CI/CD & GitHub Releases**:
  * Added automated GitHub Actions workflow (`.github/workflows/release.yml`) for building wheels (`.whl`), source distributions (`.tar.gz`), running tests, and publishing GitHub Release assets on version tags.

* **Architecture Masterclass & Issues Registry**:
  * Added `docs/SPARKDB_SYSTEM_MASTERCLASS.md`: Comprehensive engineering documentation covering GraphBLAS sparse linear algebra matrices (CSR/CSC), hybrid storage architecture, mathematical formulations for graph algorithms with step-by-step traces, and data redundancy analysis.
  * Added `issues.md`: Centralized issue registry and architectural roadmap detailing severity, impact, and remediation plans for upcoming releases.

---

### 🛡️ Backward Compatibility & Data Safety

* **100% Backward Compatible**:
  * **Zero database format changes**: Disk property stores (`properties.db`), checkpoints (`snapshot.json`), append-only logs (`mutations.aof`), and HNSW vector indices are 100% compatible.
  * Upgrading to v1.1.1 requires **no data migration, no schema conversion, and no export/import**.
* **Zero Cypher Syntax Changes**:
  * All existing Cypher queries (`MATCH`, `CREATE`, `WHERE`, `RETURN`, variable-hop patterns `*1..k`, aggregations) continue to function identically with no code modifications.

---

### 📦 How to Upgrade & Migrate

#### Option 1: Install from GitHub Releases (Direct Wheel)
```bash
pip install --upgrade https://github.com/Coder222005/sparkdb/releases/download/v1.1.1/sparkdb-1.1.1-py3-none-any.whl
```

#### Option 2: Install from GitHub Source
```bash
pip install --upgrade git+https://github.com/Coder222005/sparkdb.git@v1.1.1
```

#### Option 3: Install from PyPI (Once Synced)
```bash
pip install --upgrade sparkdb
```

#### Option 4: Local Repository / Developer Setup
```bash
cd sparkdb
git pull origin main
pip install -e .
```

---

## [1.1.0] - 2026-09-25

### Added
* **Hybrid Storage Engine**: In-memory GraphBLAS sparse matrices paired with an ACID SQLite Write-Ahead Logging (WAL) store for node/edge properties.
* **Vector Similarity Indexing**: Native HNSW vector indexing (`VectorStore`) supporting Cosine, Euclidean (L2), and Inner Product distance metrics.
* **Full-Text Inverted Index**: Built-in full-text search store with BM25-style scoring.
* **Graph Algorithms**: Built-in PageRank, Shortest Path (BFS & Dijkstra), Betweenness Centrality, Weakly Connected Components (WCC), Triangle Counting, and Lineage/Provenance tracing.
* **Client/Server Daemon**: Multi-threaded HTTP/REST server daemon (`sparkdb.server`) and zero-dependency remote client driver (`sparkdb.client.SparkDBClient`).
* **Multi-Tenant Graph Spaces**: Isolated graph namespaces within a single database instance.
