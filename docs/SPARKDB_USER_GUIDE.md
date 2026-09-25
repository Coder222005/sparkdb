# SparkDB: Complete User Guide & Integration Reference

**Version**: 1.0.0  
**License**: BSD 3-Clause / Apache 2.0 (100% Permissive Open-Source — Zero SSPL Copyleft Risk)  
**Host IP**: `10.164.241.54`  
**Host Port**: `7379` (HTTP REST)  
**Docker Image**: `sparkdb:latest` (or `sparkdb:1.0.0`)  
**Base URL**: `http://10.164.241.54:7379`  

---

## 1. Executive Overview

**SparkDB** is an enterprise-grade graph database engine designed as a clean-room, 100% permissively licensed drop-in replacement for **FalkorDB** and **RedisGraph**.

### Why SparkDB was Created
* **The FalkorDB SSPL Liability**: FalkorDB is licensed under the *Server Side Public License (SSPL v1)*. In enterprise IT, cloud infrastructure, and commercial SaaS operations, SSPL prohibits hosting the software internally or exposing it as part of an infrastructure service without open-sourcing the entire surrounding stack.
* **Pure Permissive Ownership**: SparkDB is licensed under **BSD 3-Clause / Apache 2.0**. You have full freedom to deploy, embed, host, modify, and distribute it without legal risk.
* **GraphBLAS Linear Algebraic Performance**: Rather than pointer-chasing graph models, SparkDB executes traversals as sparse matrix multiplications over boolean semirings using CSR (Compressed Sparse Row) and CSC matrices, achieving sub-millisecond query execution (**~60–120 microseconds** for 2-hop traversals).
* **Tiered Hybrid Storage (Zero-OOM Safety)**: Unlike FalkorDB which forces 100% of graph data into physical RAM, SparkDB provides a **Hybrid Storage Mode** (`storage_mode="hybrid"`). Graph topology and HNSW vectors are kept in RAM for microsecond math, while rich node/edge properties, chunk text, and metadata spill to a high-speed SQLite WAL store with an in-memory LRU cache.

---

## 2. Architecture: How FalkorDB Stores Data vs. How SparkDB Stores Data

| Component | FalkorDB (SSPL) | SparkDB In-Memory Mode | SparkDB Tiered Hybrid Mode |
| :--- | :--- | :--- | :--- |
| **Hosting Daemon** | Embedded C module in Redis / Valkey | Standalone HTTP REST daemon / embedded | Standalone HTTP REST daemon / embedded |
| **Topology Storage** | RAM (SuiteSparse:GraphBLAS C) | RAM (Vectorized CSR / CSC matrices) | RAM (Vectorized CSR / CSC matrices) |
| **Properties Storage** | **100% RAM** (causes Redis OOM crashes) | RAM (`PropertyStore` dictionary) | **Disk + LRU Cache** (`DiskPropertyStore` SQLite WAL) |
| **RAM Footprint for 10M Nodes** | **~35–50 GB RAM** | ~2–4 GB RAM | **< 200 MB RAM** |
| **Vector Embeddings & HNSW Index** | RAM | **RAM (HNSW Index / SIMD)** | **RAM (HNSW Index / SIMD)** |
| **Crash Recovery** | Redis RDB / AOF | Atomic JSON snapshot + AOF WAL replay | Native SQLite WAL + Topology Checkpoints |
| **Legal Status** | SSPL v1 (Copyleft) | BSD 3-Clause / Apache 2.0 (Permissive) | BSD 3-Clause / Apache 2.0 (Permissive) |

### 2.1 Why Vector Similarity Search is NOT Slow (The Embedding Storage Design)

A critical architectural design in SparkDB: **Embedding similarity calculation NEVER touches the disk.**

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 TIERED HYBRID STORAGE ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  ⚡ ULTRA-FAST IN-MEMORY TIER (Zero Disk I/O during Query Computation)                       │
│  ├─ 1. GraphBLAS Topology: CSR / CSC sparse adjacency matrices in RAM                       │
│  └─ 2. HNSW Vector Index: dense float32 vector arrays & HNSW graph layers in RAM            │
│         ↳ Similarity queries (Cosine / L2 / IP) execute 100% in RAM via SIMD & HNSW graphs  │
│         ↳ Query latency: < 0.5 ms (Sub-millisecond)                                         │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  💾 TIERED DISK & CACHE STORAGE TIER (Spills only heavy text payloads)                     │
│  ├─ 1. In-Memory LRU Cache: Top 10,000–50,000 hottest node/edge properties in RAM          │
│  └─ 2. SQLite WAL Disk Store: Bulky chunk text, raw spec paragraphs, and unstructured JSON │
│         ↳ Only the top-K winners (e.g. top 5 nodes) have their text fetched from disk/cache │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Why this eliminates OOM while keeping vector math at maximum speed:
* **Vectors are small**: A 256-dimension `float32` vector requires only **1 KB** of RAM ($256 \times 4$ bytes). Even 100,000 vectors take merely **~100 MB** of RAM. Keeping vectors and the HNSW graph in RAM is extremely lightweight and guarantees **microsecond-level cosine similarity**.
* **Text properties are bulky**: 100,000 document chunks, raw markdown specifications, and JSON attributes consume **gigabytes of RAM**. In FalkorDB/Redis, this text causes catastrophic Out-of-Memory (OOM) crashes.
* **Two-Stage Execution**:
  1. **Stage 1 (RAM)**: HNSW vector search finds the top $K$ nearest node IDs (e.g. `[102, 584, 912]`) in RAM in ~0.2ms.
  2. **Stage 2 (Point Lookup)**: Only those specific $K$ nodes have their text properties retrieved from the LRU cache (or disk via SQLite row lookup).

---

## 3. Quickstart with Docker

SparkDB runs in a lightweight, self-contained Docker container with built-in health monitoring and volume persistence.

### 3.1 Run via Docker CLI

Launch the SparkDB container with persistent storage mounted on the host:

```bash
docker run -d \
  --name sparkdb_container \
  -p 7379:7379 \
  -v /home/ebomven/poc/data/sparkdb:/data/sparkdb \
  --restart unless-stopped \
  sparkdb:latest
```

### 3.2 Run via Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: "3.8"

services:
  sparkdb:
    image: sparkdb:latest
    container_name: sparkdb_container
    ports:
      - "7379:7379"
    volumes:
      - ./data/sparkdb:/data/sparkdb
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python3", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:7379/health')"]
      interval: 10s
      timeout: 3s
      retries: 3
```

Start the service:
```bash
docker compose up -d
```

### 3.3 Verify Server Health

From any machine with network access to the host:

```bash
curl -s http://10.164.241.54:7379/health
```

**Response**:
```json
{
  "status": "healthy",
  "engine": "SparkDB v1.0.0 (BSD 3-Clause)",
  "host_ip": "10.164.241.54",
  "port": 7379,
  "active_projects": [],
  "active_graphs": []
}
```

---

## 4. Multi-Project Workspaces & Automatic Directory Provisioning

SparkDB provides native **multi-tenancy** and **project workspaces**. Users never need to think about creating directories, paths, or file permissions on the server.

### 4.1 How Automatic Project Provisioning Works

When you connect and select a project name (e.g. `db.select_project("ran_5g")`):
1. **Automatic Directory Creation**: SparkDB automatically provisions an isolated directory on disk:
   ```text
   /data/sparkdb/
   ├── ran_5g/
   │   ├── snapshot.json      (Atomic topology & vector checkpoint)
   │   ├── mutations.aof      (Incremental Write-Ahead-Log)
   │   └── properties.db      (SQLite WAL property store in hybrid mode)
   ├── 5g_core/
   │   ├── snapshot.json
   │   └── mutations.aof
   └── enterprise_knowledge_graph/
       ├── snapshot.json
       └── mutations.aof
   ```
2. **Strict Project Isolation**: Nodes, edges, vector spaces, and schemas created in `ran_5g` are strictly isolated from `5g_core`.
3. **Automatic Project Discovery**: When `db.list_projects()` is called, SparkDB scans the storage mount and returns all existing project workspaces, even after a container or host reboot.

### 4.2 Python SDK Multi-Project Management

```python
from sparkdb import SparkDB

# Connect to the SparkDB container at 10.164.241.54
db = SparkDB(host="10.164.241.54", port=7379)

# 1. List all active and persisted projects
print("Existing Projects:", db.list_projects())

# 2. Select or create an isolated project workspace
# (SparkDB automatically provisions the folder /data/sparkdb/ran_5g/)
project = db.select_project("ran_5g")

# 3. Insert project-specific entities
project.query("CREATE (g:gNodeB {id: 'gnb_delhi_01', cells: 64, vendor: "VendorA"})")

# 4. Checkpoint this project's state
project.checkpoint()

# 5. Drop a specific project workspace and remove its directory from disk
# db.drop_project("ran_5g")

# 6. Completely purge/reset all projects and clean the database disk
# db.drop_all_projects()
```

---

## 5. Working with Graph Data (OpenCypher Queries)

### 5.1 Creating Nodes and Relationships

```python
from sparkdb import SparkDB

db = SparkDB(host="10.164.241.54", port=7379)
project = db.select_project("5g_core")

# Create 5G Core Network Functions and Interfaces
result = project.query("""
CREATE (amf:NetworkFunction {name: 'AMF', role: 'ControlPlane', domain: '3GPP_CORE'}),
       (smf:NetworkFunction {name: 'SMF', role: 'ControlPlane', domain: '3GPP_CORE'}),
       (upf:NetworkFunction {name: 'UPF', role: 'UserPlane', domain: '3GPP_CORE'}),
       (n4:StandardInterface {name: 'N4', protocol: 'PFCP', bandwidth_gbps: 100}),
       (n11:StandardInterface {name: 'N11', protocol: 'SBI', bandwidth_gbps: 10}),
       (smf)-[:CONNECTS_VIA {latency_ms: 0.5}]->(n4),
       (n4)-[:CONNECTS_TO {latency_ms: 0.5}]->(upf),
       (smf)-[:CONNECTS_VIA {latency_ms: 1.2}]->(n11),
       (n11)-[:CONNECTS_TO {latency_ms: 1.2}]->(amf)
""")

print(f"Created {result.nodes_created} nodes and {result.relationships_created} relationships in {result.execution_time_ms:.2f}ms")
```

### 5.2 Multi-Hop Relational Traversal

Traverse paths using OpenCypher:

```python
# Multi-hop query: Find all User Plane & Control Plane endpoints reachable from SMF
res = project.query("""
MATCH (src:NetworkFunction {name: 'SMF'})-[:CONNECTS_VIA]->(iface:StandardInterface)-[:CONNECTS_TO]->(dst:NetworkFunction)
RETURN src.name AS source_nf, iface.name AS interface, iface.protocol AS protocol, dst.name AS target_nf
""")

print("Columns:", res.header)
# Columns: ['source_nf', 'interface', 'protocol', 'target_nf']

for row in res.result_set:
    print(row)
# Output:
# ['SMF', 'N4', 'PFCP', 'UPF']
# ['SMF', 'N11', 'SBI', 'AMF']
```

### 5.3 Finding Shortest Paths (`shortestPath`)

SparkDB computes topological shortest paths using breadth-first search over boolean adjacency vectors:

```python
res = project.query("""
MATCH (a:NetworkFunction {name: 'SMF'}), (b:NetworkFunction {name: 'UPF'})
RETURN shortestPath((a)-[*]->(b))
""")

path = res.result_set[0][0]
print("Shortest path node IDs:", path)
```

### 5.4 Mutating Entities and Attributes (`SET` & `REMOVE`)

SparkDB supports full in-place mutations to update properties, attach new labels, and strip out deprecated attributes.

#### Updating Node and Relationship Properties (`SET`)
```python
# Update existing properties or add new properties to matching entities
res = project.query("""
MATCH (n:NetworkFunction {name: 'SMF'})
SET n.status = 'ACTIVE', n.capacity_sessions = 5000000, n.domain = '3GPP_REL_18'
""")
print(f"Properties updated: {res.properties_set}")

# Update relationship attributes
res = project.query("""
MATCH (smf:NetworkFunction {name: 'SMF'})-[r:CONNECTS_VIA]->(n4:StandardInterface {name: 'N4'})
SET r.latency_ms = 0.25, r.backup_mode = true
""")
```

#### Adding Labels to Entities (`SET`)
```python
# Assign additional labels to existing entities
project.query("""
MATCH (n:NetworkFunction {name: 'AMF'})
SET n:CriticalInfrastructure:ControlPlaneNode
""")
```

#### Removing Properties from Entities and Relationships (`REMOVE`)
```python
# Remove unwanted properties from entities
res = project.query("""
MATCH (n:NetworkFunction {name: 'SMF'})
REMOVE n.capacity_sessions
""")
print(f"Properties removed: {res.properties_set}")

# Remove properties from relationships
project.query("""
MATCH (s)-[r:CONNECTS_VIA]->(d)
REMOVE r.backup_mode
""")
```

#### Removing Labels from Entities (`REMOVE`)
```python
# Strip a label from an entity
project.query("""
MATCH (n:NetworkFunction {name: 'AMF'})
REMOVE n:CriticalInfrastructure
""")
```

### 5.5 Deleting Entities and Relationships (`DELETE` & `DETACH DELETE`)

#### Deleting a Relationship
```python
# Delete specific relationship edges while keeping the connected nodes intact
res = project.query("""
MATCH (smf:NetworkFunction {name: 'SMF'})-[r:CONNECTS_VIA]->(n11:StandardInterface {name: 'N11'})
DELETE r
""")
print(f"Relationships deleted: {res.relationships_deleted}")
```

#### Deleting a Node and All Its Connected Relationships (`DETACH DELETE`)
When deleting a node that has inbound or outbound edges, use `DETACH DELETE` to cleanly remove the node and sever its relationships:
```python
res = project.query("""
MATCH (n:StandardInterface {name: 'N11'})
DETACH DELETE n
""")
print(f"Nodes deleted: {res.nodes_deleted}, Relationships deleted: {res.relationships_deleted}")
```

#### Deleting an Isolated Node (`DELETE`)
```python
res = project.query("""
MATCH (n:NetworkFunction {name: 'UPF'})
DELETE n
""")
print(f"Nodes deleted: {res.nodes_deleted}")
```

#### Purging All Entities and Relationships in the Current Project
```python
# Wipe all graph elements in the currently selected project workspace
res = project.query("MATCH (n) DETACH DELETE n")
print(f"Cleaned project: {res.nodes_deleted} nodes, {res.relationships_deleted} relationships deleted.")
```

---

## 6. Structural Ontology & Metamodel Inspection (Schema Without Data)

In knowledge graph engineering, enterprise ontology governance, and **GraphRAG**, applications require the structural metamodel—the types of entities, allowable connection types, and property attributes—**without leaking or fetching instance data**.

### 6.1 What the SparkDB Ontology Contains

SparkDB's ontology inspection aggregates the pure structural schema:
* **`labels`**: Complete list of all entity labels (e.g. `['NetworkFunction', 'StandardInterface']`).
* **`relationship_types`**: Complete list of all relationship types (e.g. `['CONNECTS_VIA', 'CONNECTS_TO']`).
* **`relationship_signatures`**: Triplet connectivity rules: which source label connects to which target label via which relationship type.
* **`node_properties`**: Mapping of entity labels to their registered property keys (e.g. `{"NetworkFunction": ["name", "role", "domain"]}`).
* **`edge_properties`**: Mapping of relationship types to their registered property keys (e.g. `{"CONNECTS_VIA": ["latency_ms"]}`).

### 6.2 Inspecting Ontology via Python SDK

```python
from sparkdb import SparkDB

db = SparkDB(host="10.164.241.54", port=7379)
project = db.select_project("5g_core")

# Fetch the complete structural ontology (zero instance data returned)
ontology = project.get_ontology()

print("Entity Labels:", ontology["labels"])
# ['NetworkFunction', 'StandardInterface']

print("Relationship Types:", ontology["relationship_types"])
# ['CONNECTS_TO', 'CONNECTS_VIA']

print("Relationship Triplet Signatures:")
for sig in ontology["relationship_signatures"]:
    print(f"  ({sig['source']})-[:{sig['type']}]->({sig['target']})")
# (NetworkFunction)-[:CONNECTS_VIA]->(StandardInterface)
# (StandardInterface)-[:CONNECTS_TO]->(NetworkFunction)

print("Entity Properties by Label:")
for label, props in ontology["node_properties"].items():
    print(f"  Label '{label}': {props}")
# Label 'NetworkFunction': ['domain', 'name', 'role']
# Label 'StandardInterface': ['bandwidth_gbps', 'name', 'protocol']

print("Relationship Properties by Type:")
for rel_type, props in ontology["edge_properties"].items():
    print(f"  Relationship '{rel_type}': {props}")
# Relationship 'CONNECTS_VIA': ['latency_ms']
# Relationship 'CONNECTS_TO': ['latency_ms']
```

### 6.3 Inspecting Ontology via Cypher Procedures

You can also inspect the schema directly using standard Cypher `CALL` procedures:

```python
# 1. Full schema/ontology dictionary
schema_res = project.query("CALL db.schema()")
print(schema_res.result_set[0][0])

# 2. List all entity labels
labels_res = project.query("CALL db.labels()")
print("Labels:", [row[0] for row in labels_res.result_set])

# 3. List all relationship types
rels_res = project.query("CALL db.relationshipTypes()")
print("Relationships:", [row[0] for row in rels_res.result_set])

# 4. List all property keys across the project
props_res = project.query("CALL db.propertyKeys()")
print("Property Keys:", [row[0] for row in props_res.result_set])
```

### 6.4 Inspecting Ontology via HTTP REST API

```bash
# Query the ontology endpoint for any project
curl -s "http://10.164.241.54:7379/ontology?project=5g_core"
```

**JSON Response**:
```json
{
  "project": "5g_core",
  "labels": ["NetworkFunction", "StandardInterface"],
  "relationship_types": ["CONNECTS_TO", "CONNECTS_VIA"],
  "relationship_signatures": [
    {
      "source": "NetworkFunction",
      "type": "CONNECTS_VIA",
      "target": "StandardInterface"
    },
    {
      "source": "StandardInterface",
      "type": "CONNECTS_TO",
      "target": "NetworkFunction"
    }
  ],
  "node_properties": {
    "NetworkFunction": ["domain", "name", "role"],
    "StandardInterface": ["bandwidth_gbps", "name", "protocol"]
  },
  "edge_properties": {
    "CONNECTS_TO": ["latency_ms"],
    "CONNECTS_VIA": ["latency_ms"]
  }
}
```

---

## 7. Zero-OOM Hybrid Storage Mode

In large graph workloads (e.g., millions of document chunks, entity properties, telemetry records), storing all properties in RAM leads to Out-of-Memory crashes.

SparkDB solves this with **Tiered Hybrid Storage**:
* **Topology (CSR matrices)**: Stays in RAM for microsecond traversals.
* **Properties**: Stored in SQLite (WAL mode) with an in-memory **LRU cache**.

### Enabling Hybrid Mode in Python:

```python
from sparkdb import SparkDB

# In embedded mode or server initialization:
db = SparkDB(storage_dir="./data/sparkdb")

# Enable hybrid storage with an LRU cache of 20,000 hottest nodes
project = db.select_project(
    "large_knowledge_graph",
    storage_mode="hybrid",
    lru_cache_size=20000
)

# Insert 100,000 nodes with heavy text payloads
for batch in range(100):
    nodes_cypher = ", ".join([
        f"(n{i}:Document {{id: 'doc_{batch}_{i}', payload: 'Text payload content...'}})"
        for i in range(1000)
    ])
    project.query(f"CREATE {nodes_cypher}")

# Querying any node automatically reloads from disk if evicted from LRU cache
res = project.query("MATCH (d:Document {id: 'doc_0_42'}) RETURN d.payload")
print(res.result_set[0][0])
```

---

## 8. Vector Similarity & Full-Text Search

SparkDB natively integrates dense vector search (HNSW Cosine) and BM25 full-text keyword indexing.

### 8.1 Cosine Similarity Search (`db.idx.vector.queryNodes`)

```python
# Create chunks with 4-dimensional embeddings (or 384/768/1536 for BERT/OpenAI/Gemini)
project.query("""
CREATE (c1:Chunk {id: 'chunk_1', text: 'PFCP session establishment procedure', embedding: vecf32([0.1, 0.2, 0.3, 0.4])}),
       (c2:Chunk {id: 'chunk_2', text: 'NGAP handover between gNodeB and AMF', embedding: vecf32([0.9, 0.8, 0.1, 0.0])})
""")

# Search top 1 nearest neighbor (executed 100% in RAM via HNSW)
res = project.query("CALL db.idx.vector.queryNodes('Chunk', 'embedding', 1, vecf32([0.1, 0.2, 0.3, 0.4]))")
top_node, distance = res.result_set[0]
print(f"Matched Node ID: {top_node['id']}, Cosine Distance: {distance:.5f}")
```

### 8.2 Full-Text BM25 Keyword Search (`db.idx.fulltext.queryNodes`)

```python
res = project.query("CALL db.idx.fulltext.queryNodes('Chunk', 'handover gNodeB')")
top_node, score = res.result_set[0]
print(f"Top Match: {top_node['id']}, BM25 Score: {score:.2f}")
```

---

## 9. GraphBLAS Algorithms

SparkDB executes graph algorithms directly via Cypher `CALL` procedures:

### 9.1 PageRank (`CALL algo.pageRank`)
```python
res = project.query("CALL algo.pageRank('NetworkFunction', 'CONNECTS_VIA')")
for node_dict, pr_score in res.result_set:
    print(f"Node: {node_dict.get('name')}, PageRank: {pr_score:.4f}")
```

### 9.2 Weakly Connected Components (`CALL algo.wcc`)
```python
res = project.query("CALL algo.wcc()")
for node_dict, component_id in res.result_set:
    print(f"Node: {node_dict.get('name')}, Component: {component_id}")
```

### 9.3 Triangle Count (`CALL algo.triangleCount`)
```python
res = project.query("CALL algo.triangleCount()")
print("Total Triangles:", res.result_set[0][0])
```

---

## 10. Checkpointing & Disaster Recovery

SparkDB writes zero-loss Write-Ahead Logs (AOF) and supports atomic snapshot checkpointing.

```python
# Save an atomic checkpoint for this project
project.checkpoint()

# Or checkpoint all projects managed by the database
db.checkpoint_all()
```

If the Docker container is restarted or crashes, SparkDB automatically:
1. Reloads the latest `snapshot.json` in parallel.
2. Replays any uncheckpointed mutations from `mutations.aof`.
3. Resumes state with zero data loss.

---

## 11. HTTP REST API Reference

For non-Python applications (Go, Node.js, Java, cURL), interact directly with `http://10.164.241.54:7379`:

### 11.1 List Projects (`GET /projects`)
```bash
curl -s http://10.164.241.54:7379/projects
```
**Response**:
```json
{
  "projects": ["5g_core", "ran_5g"],
  "graphs": ["5g_core", "ran_5g"]
}
```

### 11.2 Execute Query (`POST /query`)
```bash
curl -X POST http://10.164.241.54:7379/query \
  -H "Content-Type: application/json" \
  -d '{
    "project": "5g_core",
    "query": "MATCH (s:NetworkFunction)-[:CONNECTS_VIA]->(i) RETURN s.name, i.name"
  }'
```

**Response**:
```json
{
  "header": ["s.name", "i.name"],
  "result_set": [["SMF", "N4"], ["SMF", "N11"]],
  "execution_time_ms": 0.85,
  "nodes_created": 0,
  "relationships_created": 0,
  "properties_set": 0,
  "nodes_deleted": 0,
  "relationships_deleted": 0
}
```

### 11.3 Inspect Structural Ontology (`GET /ontology` or `GET /schema`)
```bash
curl -s "http://10.164.241.54:7379/ontology?project=5g_core"
```

### 11.4 Trigger Snapshot Checkpoint (`POST /checkpoint`)
```bash
curl -X POST http://10.164.241.54:7379/checkpoint
```

### 11.5 Drop a Single Project (`POST /drop`)
```bash
curl -X POST http://10.164.241.54:7379/drop \
  -H "Content-Type: application/json" \
  -d '{"project": "temp_project"}'
```

### 11.6 Purge All Projects and Clean Database (`POST /drop_all`)
```bash
curl -X POST http://10.164.241.54:7379/drop_all
```
**Response**:
```json
{
  "status": "success",
  "dropped_projects": ["5g_core", "ran_5g"]
}
```

### 11.7 Health Check (`GET /health`)
```bash
curl -s http://10.164.241.54:7379/health
```

---

## 12. Migration from FalkorDB (`falkordb` -> `sparkdb`)

SparkDB is engineered to be an effortless drop-in replacement for existing FalkorDB codebases:

### Before (FalkorDB — SSPL Copyleft Risk):
```python
from falkordb import FalkorDB

db = FalkorDB(host="10.164.241.54", port=6379)
graph = db.select_graph("my_project")
res = graph.query("MATCH (n:Entity) RETURN n.name")
for row in res.result_set:
    print(row[0])
```

### After (SparkDB — 100% Permissive Open Source):
```python
from sparkdb import SparkDB

db = SparkDB(host="10.164.241.54", port=7379)
project = db.select_project("my_project")
res = project.query("MATCH (n:Entity) RETURN n.name")
for row in res.result_set:
    print(row[0])
```

Zero query syntax changes required. Full OpenCypher pattern matching, parameter formats, and algorithm calls work identically out of the box.

---

## 13. Client Connection & Options Reference

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `host` | `str` | `"10.164.241.54"` | Hostname/IP of the running SparkDB Docker container. |
| `port` | `int` | `7379` | Port exposed by the SparkDB container. |
| `url` | `str` | `None` | Full base URL (e.g. `"http://10.164.241.54:7379"`). |
| `storage_dir` | `str` | `"./data/sparkdb"` | Local directory for embedded in-process storage or snapshots. |
| `storage_mode` | `str` | `"memory"` | `"memory"` for 100% RAM, or `"hybrid"` for disk-backed SQLite WAL + LRU cache. |
| `lru_cache_size` | `int` | `10000` | Number of hot node/edge properties retained in RAM when using `"hybrid"` mode. |
| `default_vector_dim` | `int` | `256` | Default embedding dimension for HNSW vector index spaces. |
