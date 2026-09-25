# SparkDB Development Workspace (`sparkdb_dev`)

**SparkDB** is an enterprise-grade, clean-room graph database engine designed as a 100% permissively licensed (BSD 3-Clause / Apache 2.0) drop-in replacement for **FalkorDB** and **RedisGraph**.

This directory (`sparkdb_dev`) consolidates all source code, tests, Docker configurations, build artifacts, and documentation for SparkDB.

---

## 1. Directory Structure

```
sparkdb_dev/
├── sparkdb/                 # Core engine package
│   ├── algorithms/          # Graph algorithms (pathfinding, centrality, community, etc.)
│   ├── core/                # GraphBLAS linear algebra engine, CSR/CSC matrices, hybrid SQLite WAL store, HNSW vectors
│   ├── cypher/              # openCypher lexer, parser, and vectorized execution engine
│   ├── server/              # High-throughput ThreadingHTTPServer daemon (REST API)
│   ├── client.py            # Drop-in replacement Python SDK (zero external dependencies)
│   └── cli.py               # Interactive CLI client
├── tests/                   # Complete SparkDB test suite (Parity, Mutations, Hybrid, Remote)
│   ├── test_falkordb_advanced_parity.py
│   ├── test_sparkdb_mutations_and_ontology.py
│   ├── test_sparkdb_complete.py
│   ├── test_sparkdb_hybrid_storage.py
│   ├── test_sparkdb_remote_client.py
│   └── test_internal_graph_engine.py
├── docker/                  # Production Docker configuration
│   ├── Dockerfile.sparkdb   # Production multi-stage Docker build
│   ├── docker-compose.yml   # One-command compose setup
│   └── sparkdb_deps/        # Pre-packaged native dependencies
├── docs/                    # Technical & User documentation
│   ├── SPARKDB_END_USER_GUIDE.md    # Developer integration guide & SDK reference
│   ├── SPARKDB_USER_GUIDE.md        # Architecture, linear algebra & hybrid storage spec
│   └── INTERNAL_GRAPH_ENGINE_SPEC.md # GraphBLAS & matrix implementation spec
├── dist/                    # Built distribution wheels
│   └── sparkdb-1.1.0-py3-none-any.whl (Zero-dependency client wheel)
├── pyproject.toml           # Build system specification (flit / pip)
└── README.md                # This reference file
```

---

## 2. Running Tests

Run all SparkDB tests using `pytest`:

```bash
# From sparkdb_dev directory:
pytest tests/
```

---

## 3. Building the Universal Wheel

Build the zero-dependency client distribution wheel:

```bash
pip wheel --no-build-isolation --no-deps -w dist .
```

---

## 4. Building & Running the Docker Container

Build and launch the SparkDB server container:

```bash
# Build Docker image
docker build -f docker/Dockerfile.sparkdb -t sparkdb:latest .

# Run container
docker run -d \
  --name sparkdb_container \
  -p 7379:7379 \
  -v /home/ebomven/poc/data/sparkdb:/data/sparkdb \
  --restart unless-stopped \
  sparkdb:latest
```

Or using Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up -d
```

---

## 5. Connecting with the Python Client

```python
from sparkdb import SparkDB

# Connect to SparkDB instance
db = SparkDB(host="localhost", port=7379)

# Select or create project
project = db.select_project("production_graph")

# Execute Cypher
res = project.query("CREATE (n:Device {name: 'Switch-01'}) RETURN n.name")
print(res.result_set)
```

---

## 6. Contributors

* **Lead Author & Major Contributor**: Bommireddy Venkata Dheeraj Reddy ([bommireddyvenkatadheerajreddy@gmail.com](mailto:bommireddyvenkatadheerajreddy@gmail.com))
* **Maintainer & Contributor**: Ruthwik ([ruthwik4566@gmail.com](mailto:ruthwik4566@gmail.com))

---

## 7. License

SparkDB is open-sourced under the **BSD-3-Clause License**. See the `LICENSE` file for full terms.
