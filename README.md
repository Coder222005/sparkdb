# SparkDB

[![PyPI version](https://img.shields.io/pypi/v/sparkdb.svg?color=blue)](https://pypi.org/project/sparkdb/)
[![Python versions](https://img.shields.io/pypi/pyversions/sparkdb.svg)](https://pypi.org/project/sparkdb/)
[![License: BSD-3-Clause](https://img.shields.io/badge/License-BSD--3--Clause-green.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Tests](https://img.shields.io/badge/tests-39%20passed%20(100%25)-brightgreen.svg)]()

**SparkDB** is an enterprise-grade, clean-room graph database engine designed as a **100% permissively licensed (BSD 3-Clause)** drop-in replacement for **FalkorDB** and **RedisGraph**.

Built from the ground up using **GraphBLAS sparse linear algebra**, SparkDB models graph topologies as sparse adjacency matrices (CSR/CSC). It pairs vectorized matrix multiplications with an ACID SQLite Write-Ahead Logging (WAL) property engine, an openCypher query executor with Cost-Based Optimization (CBO), and built-in HNSW vector search.

---

## Key Features

* **Permissive Clean-Room (Zero-SSPL)**: Licensed under BSD-3-Clause. 100% free of copyleft restrictions, commercial hosting prohibitions, or vendor lock-in.
* **GraphBLAS Linear Algebra Core**: Graph traversals and path finding are computed using sparse matrix multiplications ($C = A \cdot B$) over CSR/CSC representations, achieving microsecond execution times.
* **Vectorized Cypher Engine**: Full openCypher dialect support (`MATCH`, `CREATE`, `WHERE`, `RETURN`, `DELETE`, `SET`, `REMOVE`, `ORDER BY`, `LIMIT`, `SKIP`, variable-length relationships `*min..max`).
* **Cost-Based Optimizer (CBO) & 2-Tier LRU Plan Cache**: AST and execution plans are parsed once, parameterized, and cached for sub-millisecond query evaluation.
* **Hybrid Storage Architecture**: In-memory sparse adjacency matrices for hyper-fast traversals paired with an ACID SQLite Write-Ahead Logging (WAL) store for persistent properties and JSON/AOF snapshot recovery.
* **Integrated Vector Search & HNSW Indexing**: Native cosine, Euclidean (L2), and inner product similarity indexing with automatic NumPy SIMD fallback.
* **Graph Algorithms Library**: Built-in support for PageRank, Shortest Path (BFS and Dijkstra), Betweenness Centrality, Weakly/Strongly Connected Components (WCC/SCC), Triangle Counting, and Lineage/Provenance tracing.
* **Dual Deployment Modes**: Run as an **embedded in-process Python library** (zero network overhead, zero C-compiler requirements) OR as a **standalone multi-threaded HTTP/REST daemon** and Docker container.

---

## Table of Contents

1. [Installation](#installation)
2. [Quickstart: Embedded In-Process Engine](#quickstart-embedded-in-process-engine)
3. [Quickstart: Standalone Server & Remote Client](#quickstart-standalone-server--remote-client)
4. [Cypher Query Guide](#cypher-query-guide)
5. [Graph Algorithms](#graph-algorithms)
6. [Vector Similarity Search](#vector-similarity-search)
7. [Multi-Tenant Project & Graph Management](#multi-tenant-project--graph-management)
8. [Interactive Command-Line Interface (CLI)](#interactive-command-line-interface-cli)
9. [Running with Docker](#running-with-docker)
10. [FalkorDB & RedisGraph Migration](#falkordb--redisgraph-migration)
11. [Running the Test Suite](#running-the-test-suite)
12. [Repository Structure](#repository-structure)
13. [Contributors & License](#contributors--license)

---

## Installation

### Method 1: Install from PyPI (Recommended)

Install the lightweight, zero-dependency client and embedded engine directly:

```bash
pip install sparkdb
```

For full server acceleration (NumPy, SciPy linear algebra, and HNSW vector search):

```bash
pip install "sparkdb[server]"
```

### Method 2: Install from Source

```bash
git clone https://github.com/Coder222005/sparkdb.git
cd sparkdb
pip install -e .
```

---

## Quickstart: Embedded In-Process Engine

SparkDB can be embedded directly within any Python application with zero external daemons or services:

```python
from sparkdb import SparkDB

# Initialize embedded engine (persistent storage directory is optional)
db = SparkDB(storage_dir="./data/sparkdb")

# Select or create an isolated graph space
graph = db.select_graph("social_network")

# 1. Create nodes and relationships using Cypher
graph.query("""
CREATE (alice:Person {name: 'Alice', age: 30}),
       (bob:Person {name: 'Bob', age: 28}),
       (charlie:Person {name: 'Charlie', age: 35}),
       (alice)-[:KNOWS {since: 2021}]->(bob),
       (bob)-[:KNOWS {since: 2023}]->(charlie)
""")

# 2. Query graph patterns and multi-hop traversals
result = graph.query("""
MATCH (src:Person {name: 'Alice'})-[:KNOWS*1..2]->(friend:Person)
RETURN src.name AS person, friend.name AS friend, friend.age AS age
""")

# 3. Read tabular results
print("Columns:", result.header)
for row in result.result_set:
    print(f"{row[0]} -> {row[1]} (Age: {row[2]})")

# 4. Save an atomic snapshot
snapshot_path = graph.checkpoint()
print(f"Graph snapshot persisted to: {snapshot_path}")
```

---

## Quickstart: Standalone Server & Remote Client

### 1. Start the SparkDB Server

Launch the multi-threaded HTTP server daemon:

```bash
# Using the installed CLI entry point
sparkdb-server 0.0.0.0 7379 ./data/sparkdb

# Or using the Python module directly
python -m sparkdb.server.server 0.0.0.0 7379 ./data/sparkdb
```

The server binds to port `7379` by default and exposes both JSON REST and MessagePack binary protocol endpoints.

### 2. Connect from Any Remote Python Client

The remote SDK is lightweight and requires zero C-extensions:

```python
from sparkdb import SparkDB

# Connect to the remote SparkDB server
client = SparkDB(host="localhost", port=7379)

# Select or provision a tenant workspace
project = client.select_project("finance_graph")

# Execute Cypher
res = project.query("CREATE (c:Company {ticker: 'SPRK', name: 'Spark Corp'}) RETURN c.ticker")
print("Created:", res.result_set)
```

---

## Cypher Query Guide

SparkDB supports standard openCypher query syntax:

### Node & Edge Creation
```cypher
CREATE (srv:Server {hostname: 'web-01', ip: '10.0.0.1'}),
       (db:Database {engine: 'PostgreSQL', port: 5432}),
       (srv)-[:DEPENDS_ON {protocol: 'TCP'}]->(db)
```

### Pattern Matching with Filters
```cypher
MATCH (srv:Server)-[r:DEPENDS_ON]->(db:Database)
WHERE srv.hostname = 'web-01'
RETURN srv.ip, db.engine, r.protocol
```

### Variable-Length Path Traversal
```cypher
MATCH (start:Node {id: 1})-[*1..4]->(target:Node)
RETURN DISTINCT target.id
```

### Deletions and Mutations
```cypher
MATCH (srv:Server {hostname: 'web-01'})
DELETE srv
```

### Query Plan Inspection & Profiling
```python
# Explain physical plan without execution
plan = graph.explain("MATCH (n:Person) RETURN n.name")
print(plan.result_set)

# Profile execution with latency breakdown
profile = graph.profile("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name")
print(profile.result_set)
```

---

## Graph Algorithms

SparkDB provides built-in GraphBLAS algorithmic routines that execute directly on sparse adjacency matrices:

### PageRank
```python
from sparkdb.algorithms import pagerank

# Calculate PageRank scores for all nodes in the graph
scores = pagerank(graph._space, damping=0.85, max_iter=100)
for node_id, rank in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]:
    props = graph._space.property_store.get_node_properties(node_id)
    print(f"Node: {props.get('name', node_id)} - Score: {rank:.4f}")
```

### Shortest Path (BFS & Dijkstra)
```python
from sparkdb.algorithms import shortest_path

# Find shortest path between two nodes
path = shortest_path(graph._space, src_node_id=0, dst_node_id=4)
print(f"Shortest path hops: {path}")
```

### Connected Components & Centrality
```python
from sparkdb.algorithms import weakly_connected_components, degree_centrality

components = weakly_connected_components(graph._space)
centralities = degree_centrality(graph._space)
```

---

## Vector Similarity Search

SparkDB natively integrates vector embeddings with graph topology. Each graph space includes an HNSW vector index:

```python
# Insert nodes with dense embedding vectors
graph.query("""
CREATE (doc1:Document {title: 'GraphBLAS Algorithms', embedding: [0.12, 0.88, 0.45, 0.02]}),
       (doc2:Document {title: 'Vector Databases', embedding: [0.15, 0.82, 0.41, 0.08]}),
       (doc3:Document {title: 'Cooking Recipes', embedding: [0.95, 0.05, 0.10, 0.89]})
""")

# Retrieve top-k nearest neighbors by cosine similarity
results = graph.vector_search(
    vector=[0.14, 0.85, 0.43, 0.05],
    k=2,
    space="cosine"
)
for doc_id, score in results:
    props = graph._space.property_store.get_node_properties(doc_id)
    print(f"Match: {props['title']} (Similarity: {score:.4f})")
```

---

## Multi-Tenant Project & Graph Management

SparkDB supports isolated multiple tenants (workspaces/graphs) out of the box:

```python
from sparkdb import SparkDB

db = SparkDB()

# List all active graph spaces
print("Active graphs:", db.list_graphs())

# Select or switch between graphs
g1 = db.select_graph("tenant_alpha")
g2 = db.select_graph("tenant_beta")

# Inspect schema & ontology without loading raw data
schema = g1.get_schema()
print("Node labels:", schema.get("labels"))
print("Relationships:", schema.get("relationship_types"))

# Delete a specific workspace
db.drop_graph("tenant_beta")

# Purge all workspaces
db.drop_all_projects()
```

---

## Interactive Command-Line Interface (CLI)

SparkDB provides an interactive REPL shell:

```bash
sparkdb-cli
```

### Example Interactive Session

```text
Welcome to the SparkDB Interactive Shell. Type :help or ? to list commands. Type 'exit' to quit.

sparkdb[default]> CREATE (a:User {name: 'Alice'})-[:FOLLOWS]->(b:User {name: 'Bob'})
Nodes created: 2, Relationships created: 1

sparkdb[default]> MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name

a.name | b.name
-----------------
Alice  | Bob

sparkdb[default]> USE analytics_graph
Switched to graph 'analytics_graph'

sparkdb[analytics_graph]> CHECKPOINT
Checkpoint saved to ./data/sparkdb/analytics_graph_snapshot.json

sparkdb[analytics_graph]> exit
```

---

## Running with Docker

### One-Command Setup with Docker Compose

```bash
# Clone repository
git clone https://github.com/Coder222005/sparkdb.git
cd sparkdb

# Start SparkDB container in detached mode
docker compose -f docker/docker-compose.yml up -d
```

### Build & Run Manually

```bash
# Build Docker image
docker build -f docker/Dockerfile.sparkdb -t sparkdb:latest .

# Run container with persistent data volume
docker run -d \
  --name sparkdb_container \
  -p 7379:7379 \
  -v sparkdb_data:/data/sparkdb \
  --restart unless-stopped \
  sparkdb:latest
```

---

## FalkorDB & RedisGraph Migration

SparkDB provides complete drop-in compatibility with codebases written for FalkorDB or RedisGraph:

| Feature | FalkorDB / RedisGraph | SparkDB |
| :--- | :--- | :--- |
| **License** | SSPL / Proprietary | **BSD-3-Clause (Permissive Clean-Room)** |
| **Query Dialect** | openCypher | **openCypher** |
| **Execution Model** | GraphBLAS Sparse Matrices | **GraphBLAS Sparse Matrices (CSR/CSC)** |
| **Client Initialization** | `FalkorDB(host=..., port=...)` | `SparkDB(host=..., port=...)` or `SparkDB()` (Embedded) |
| **Query Invocation** | `graph.query(cypher, params)` | `graph.query(cypher, params)` |
| **Read-Only Queries** | `graph.ro_query(...)` | `graph.ro_query(...)` |
| **Execution Profiling** | `graph.profile(...)` | `graph.profile(...)` |
| **Plan Explanation** | `graph.explain(...)` | `graph.explain(...)` |
| **Embedded Mode** | No (Requires C-server/daemon) | **Yes (Pure-Python Embedded Available)** |
| **Multi-Tenancy** | `select_graph(name)` | `select_graph(name)` / `select_project(name)` |

### Instant Migration Code Example

```python
# Before (FalkorDB):
# from falkordb import FalkorDB
# db = FalkorDB(host="localhost", port=6379)

# After (SparkDB) - 100% Compatible:
from sparkdb import SparkDB
db = SparkDB(host="localhost", port=7379)

graph = db.select_graph("my_graph")
res = graph.query("MATCH (n:Person) RETURN n.name LIMIT 10")
```

---

## Running the Test Suite

SparkDB includes an extensive test suite verifying Cypher compliance, GraphBLAS linear algebra routines, HNSW indexing, persistence WAL replay, and thread safety:

```bash
# Run pytest across the complete test suite
pytest tests/
```

Expected output:
```text
tests/test_cbo_and_ast_cache.py .......                                  [ 17%]
tests/test_falkordb_advanced_parity.py .......                           [ 35%]
tests/test_sparkdb_complete.py ......                                    [ 51%]
tests/test_sparkdb_hybrid_storage.py .....                               [ 64%]
tests/test_sparkdb_mutations_and_ontology.py .....                       [ 76%]
tests/test_sparkdb_remote_client.py ..                                   [ 82%]
tests/test_storage_optimization_and_binary_protocol.py .......           [100%]

============================= 39 passed in 19.76s =============================
```

---

## Repository Structure

```
sparkdb/
├── sparkdb/                 # Core engine & Python SDK package
│   ├── algorithms/          # Graph algorithms (pathfinding, centrality, community, etc.)
│   ├── client/              # In-process SDK client and remote HTTP/MessagePack client
│   ├── core/                # GraphBLAS MatrixStore (CSR/CSC), PropertyStore, VectorStore
│   ├── cypher/              # openCypher AST parser, 2-tier LRU cache, CBO executor
│   ├── server/              # Multi-threaded HTTP daemon & REST API
│   ├── cli.py               # Interactive CLI REPL (sparkdb-cli)
│   └── __init__.py          # Public API exports
├── tests/                   # Complete test suite (39 unit & integration tests)
├── docker/                  # Dockerfile and docker-compose configurations
├── docs/                    # Architecture, internals, and user reference guides
├── scripts/                 # Stress tests and benchmark suites
├── pyproject.toml           # PEP 517/621 build configuration
├── LICENSE                  # BSD-3-Clause License
└── README.md                # Project documentation
```

---

## Contributors & License

* **Lead Author & Major Contributor**: Bommireddy Venkata Dheeraj Reddy ([bommireddyvenkatadheerajreddy@gmail.com](mailto:bommireddyvenkatadheerajreddy@gmail.com))
* **Maintainer & Contributor**: Ruthwik ([ruthwik4566@gmail.com](mailto:ruthwik4566@gmail.com))

SparkDB is open-sourced under the **BSD-3-Clause License**. See the [LICENSE](LICENSE) file for complete details.
