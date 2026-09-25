# SparkDB: Developer & End-User Integration Guide

**Database Service**: SparkDB v1.1.0  
**License**: BSD 3-Clause / Apache 2.0 (Permissive Clean-Room — 100% Free of SSPL Copyleft Restrictions)  
**Host IP**: `10.164.241.54`  
**Port**: `7379`  
**Endpoint**: `http://10.164.241.54:7379`  
**Package Wheel**: `sparkdb-1.1.0-py3-none-any.whl` (39 KB, Zero External Dependencies)  

---

## 1. How to Get & Install the SparkDB Package

The SparkDB client SDK is packaged as a pure-Python, zero-dependency wheel. It requires **no C compilers, no numpy, and no heavy dependencies** on the client machine—it uses standard Python 3.8+ libraries (`urllib` and `json`).

### Method A: Direct Wheel Installation (Recommended for Teams)
Transfer the wheel file `sparkdb-1.1.0-py3-none-any.whl` (located in `dist/` on the server) to the developer's laptop or VM, then run:

```bash
pip install sparkdb-1.1.0-py3-none-any.whl
```

To upgrade in the future:
```bash
pip install --upgrade --force-reinstall sparkdb-1.1.0-py3-none-any.whl
```

### Method B: Install via Internal Artifact Repository (Artifactory / Nexus / Azure Artifacts)
If your organization hosts an internal PyPI mirror or Artifactory feed:
```bash
# Upload wheel to your company registry (one-time by admin):
twine upload --repository-url https://pkgs.dev.azure.com/YourOrg/_packaging/your_feed/pypi/upload/ dist/sparkdb-1.1.0-py3-none-any.whl

# Developers install directly:
pip install sparkdb --index-url https://pkgs.dev.azure.com/YourOrg/_packaging/your_feed/pypi/simple/
```

### Method C: Standalone Single-File Drop-In (Zero `pip` required)
If a developer cannot use `pip`, they can copy `sparkdb/client.py` directly into their project repository as `sparkdb_client.py` and import `SparkDB` directly:
```python
from sparkdb_client import SparkDBClient as SparkDB
```

---

## 2. Quickstart: Connecting to SparkDB

Connect to the centralized SparkDB instance running on `10.164.241.54:7379`:

```python
from sparkdb import SparkDB

# Connect to the remote SparkDB server
db = SparkDB(host="10.164.241.54", port=7379)

# Verify server connectivity
print("Connected to SparkDB:", db.base_url)
```

---

## 3. Project & Workspace Management (Multi-Tenancy)

SparkDB provides isolated multi-tenant project spaces. **You do not need to configure directories, paths, or filesystem locations.** Specifying a project name automatically provisions an isolated storage space and graph BLAS matrix store.

### 3.1 Selecting or Creating a Project Workspace
```python
# Selects an existing project or automatically provisions a new one
project = db.select_project("enterprise_network")
```

### 3.2 Listing All Active Projects
```python
projects = db.list_projects()
print("Active projects on server:", projects)
# Output: ['enterprise_network', 'infra_knowledge_graph', 'user_session_tracker']
```

### 3.3 Deleting a Project (Wiping All Data for a Workspace)
```python
# Drops the specific project and frees all associated RAM and disk storage
db.drop_project("enterprise_network")
```

### 3.4 Purging All Projects (Resetting the Database)
```python
# Drops all projects currently on the server
dropped_list = db.drop_all_projects()
print("Dropped projects:", dropped_list)
```

---

## 4. Graph Operations & Cypher Query Guide

The `project` object executes openCypher queries with full parameter substitution to prevent injection and maximize execution plan caching.

### 4.1 Creating Entities (Nodes)
Nodes can have labels and arbitrary key-value properties:

```python
# Create a single entity
result = project.query(
    """
    CREATE (u:User {
        id: $user_id,
        name: $name,
        role: $role,
        active: true
    })
    RETURN u.id, u.name
    """,
    params={"user_id": "usr_101", "name": "Alice Smith", "role": "Network Engineer"}
)
print("Entity created:", result.result_set)
# Output: [['usr_101', 'Alice Smith']]
```

### 4.2 Creating Relationships (Edges)
Connect entities using typed, directed relationships:

```python
# Create relationship between existing entities
project.query(
    """
    MATCH (u:User {id: $user_id}), (s:Server {id: $server_id})
    CREATE (u)-[r:MANAGES {since: 2024, permission: "admin"}]->(s)
    RETURN type(r), r.permission
    """,
    params={"user_id": "usr_101", "server_id": "srv_east_01"}
)
```

### 4.3 Querying Entities & Traversals
Retrieve data, filter properties, and traverse multi-hop graph patterns:

```python
# Multi-hop traversal: Find all servers managed by engineers in a team
query = """
MATCH (u:User)-[:MANAGES]->(s:Server)
WHERE u.role = $role
RETURN u.name AS engineer, s.hostname AS server, s.ip AS ip_address
ORDER BY u.name ASC
LIMIT 50
"""
result = project.query(query, params={"role": "Network Engineer"})

# Inspect results
print("Columns:", result.header)
for row in result.result_set:
    print(dict(zip(result.header, row)))
```

### 4.4 Updating Attributes (Properties)
Use `SET` to modify existing attributes or add new attributes to nodes or relationships:

```python
# Update attributes on a specific entity
project.query(
    """
    MATCH (s:Server {id: $server_id})
    SET s.status = $new_status, s.last_ping = $timestamp
    RETURN s.id, s.status, s.last_ping
    """,
    params={
        "server_id": "srv_east_01",
        "new_status": "MAINTENANCE",
        "timestamp": 1727200000
    }
)
```

### 4.5 Removing Specific Attributes
Use `REMOVE` to delete specific attributes from an entity without deleting the entity itself:

```python
# Remove the temporary attribute from the entity
project.query(
    """
    MATCH (s:Server {id: $server_id})
    REMOVE s.temp_cache_key
    RETURN s.id
    """,
    params={"server_id": "srv_east_01"}
)
```

### 4.6 Deleting Relationships & Entities
* **Delete an edge**:
```python
project.query(
    """
    MATCH (u:User {id: $user_id})-[r:MANAGES]->(s:Server {id: $server_id})
    DELETE r
    """,
    params={"user_id": "usr_101", "server_id": "srv_east_01"}
)
```

* **Detach delete a node** (removes node and all connecting relationships):
```python
project.query(
    """
    MATCH (s:Server {id: $server_id})
    DETACH DELETE s
    """,
    params={"server_id": "srv_decommissioned_99"}
)
```

---

## 5. Extracting Project Ontology & Schema (Data-Free Inspection)

When building LLM RAG pipelines, schema validators, or architecture explorers, you often need the **exact schema (ontology)** of the graph **without exposing sensitive customer or user row data**.

SparkDB provides a dedicated `get_ontology()` method:

```python
ontology = project.get_ontology()
# Or: ontology = project.get_schema()

print("Node labels present:", ontology["node_labels"])
print("Property keys per label:", ontology["property_keys_by_label"])
print("Relationship types:", ontology["relationship_types"])
print("Connections (src -[rel]-> dst):", ontology["relationship_schema"])
```

### Example Ontology Output:
```json
{
  "project": "enterprise_network",
  "node_labels": ["NetworkFunction", "StandardInterface"],
  "property_keys_by_label": {
    "NetworkFunction": ["id", "name", "role", "domain"],
    "StandardInterface": ["name", "protocol"]
  },
  "relationship_types": ["CONNECTS_TO", "CONNECTS_VIA"],
  "relationship_schema": [
    {
      "src_label": "NetworkFunction",
      "rel_type": "CONNECTS_VIA",
      "dst_label": "StandardInterface"
    },
    {
      "src_label": "StandardInterface",
      "rel_type": "CONNECTS_TO",
      "dst_label": "NetworkFunction"
    }
  ],
  "property_keys_by_relationship": {
    "CONNECTS_VIA": ["latency_ms"]
  }
}
```

You can also query the schema via Cypher procedures:
```python
# Procedural Cypher calls
labels = project.query("CALL db.labels()")
rel_types = project.query("CALL db.relationshipTypes()")
schema = project.query("CALL db.schema()")
> **Privacy Note**: Notice that zero user names, IPs, or record values are returned—only structural meta-types. This is safe to inject directly into LLM prompts for natural language Cypher generation.

---

## 6. Secondary Indexing & Query Profiling

### 6.1 Creating Property Indexes
Create range indexes on high-cardinality lookup keys (e.g. `id`, `email`, `serial_number`) for $O(1)$ node lookups:

```python
# Create index on User.id and Server.hostname
project.create_node_range_index("User", "id")
project.create_node_range_index("Server", "hostname")

# List active indexes
indexes = project.list_indexes()
print("Active indexes:", indexes)
```

### 6.2 Query Plan Analysis & Slow Logs
```python
# Inspect execution plan without executing:
plan = project.explain("MATCH (u:User {id: 'usr_101'}) RETURN u.name")
print(plan.result_set)

# Retrieve execution logs for slow queries:
logs = project.slowlog()
for entry in logs:
    print(f"[{entry['timestamp']}] {entry['query']} ({entry['duration_ms']} ms)")
```

---

## 7. Data Persistence & Checkpointing

SparkDB uses write-ahead logging (WAL) for durability. You can also explicitly trigger an atomic snapshot checkpoint:

```python
# Checkpoint current project to disk
checkpoint_info = project.checkpoint()
print("Checkpoint created:", checkpoint_info)

# Or checkpoint all projects across the database instance
all_snapshots = db.checkpoint_all()
print("All projects check-pointed:", all_snapshots)
```

---

## 8. REST API / cURL Quick Reference

For microservices written in Go, Java, C#, or Node.js, you can interact with SparkDB directly over HTTP REST without any SDK:

### 8.1 Health Check (Sub-millisecond)
```bash
curl http://10.164.241.54:7379/health
```
Response:
```json
{
  "status": "healthy",
  "engine": "SparkDB v1.1.0 (BSD 3-Clause)",
  "host_ip": "10.164.241.54",
  "port": 7379,
  "active_projects": ["enterprise_network"]
}
```

### 8.2 List Projects
```bash
curl http://10.164.241.54:7379/projects
```

### 8.3 Execute Cypher Query
```bash
curl -X POST http://10.164.241.54:7379/query \
  -H "Content-Type: application/json" \
  -d '{
    "project": "enterprise_network",
    "query": "MATCH (u:User) RETURN u.name, u.role LIMIT 10"
  }'
```

### 8.4 Retrieve Project Schema / Ontology
```bash
curl "http://10.164.241.54:7379/ontology?project=enterprise_network"
```

### 8.5 Trigger Snapshot Checkpoint
```bash
curl -X POST http://10.164.241.54:7379/checkpoint \
  -H "Content-Type: application/json" \
  -d '{"project": "enterprise_network"}'
```

### 8.6 Drop a Project
```bash
curl -X POST http://10.164.241.54:7379/drop \
  -H "Content-Type: application/json" \
  -d '{"project": "enterprise_network"}'
```

---

## 9. Complete End-to-End Python Example

Here is a complete, self-contained Python script demonstrating the entire workflow:

```python
#!/usr/bin/env python3
"""SparkDB Complete End-to-End Demo Script."""

from sparkdb import SparkDB

def run_demo():
    # 1. Connect
    db = SparkDB(host="10.164.241.54", port=7379)
    print("1. Connected to SparkDB server at:", db.base_url)

    # 2. Select Project Workspace
    project_name = "network_inventory_demo"
    project = db.select_project(project_name)
    print(f"2. Selected project workspace: '{project_name}'")

    # 3. Create Entities and Relationships
    print("3. Inserting graph topology...")
    project.query(
        """
        CREATE (dc:Datacenter {id: 'dc_kista_01', name: 'Stockholm Core DC', country: 'SE'}),
               (r1:Router {id: 'rtr_core_01', model: 'Cisco 8000', ip: '192.168.1.1'}),
               (r2:Router {id: 'rtr_edge_01', model: 'Juniper PTX', ip: '192.168.1.2'}),
               (dc)-[:HOUSES {rack: 'R-42'}]->(r1),
               (r1)-[:UPLINK {speed_gbps: 400}]->(r2)
        """
    )

    # 4. Query Topology
    print("4. Traversing graph...")
    res = project.query(
        """
        MATCH (dc:Datacenter)-[:HOUSES]->(r1:Router)-[link:UPLINK]->(r2:Router)
        RETURN dc.name AS datacenter, r1.id AS core_router, link.speed_gbps AS bandwidth, r2.id AS edge_router
        """
    )
    for row in res.result_set:
        print("   Found path:", dict(zip(res.header, row)))

    # 5. Update Attributes (SET)
    print("5. Updating attributes (SET)...")
    project.query(
        """
        MATCH (r:Router {id: 'rtr_edge_01'})
        SET r.status = 'UPGRADED', r.firmware = 'v14.2.1'
        """
    )

    # 6. Remove an Attribute (REMOVE)
    print("6. Removing attribute (REMOVE)...")
    project.query(
        """
        MATCH (r:Router {id: 'rtr_edge_01'})
        REMOVE r.status
        """
    )

    # 7. Extract Schema / Ontology (No sensitive row data)
    print("7. Inspecting ontology schema...")
    schema = project.get_ontology()
    print("   Node Labels:", schema["node_labels"])
    print("   Relationship Types:", schema["relationship_types"])
    for rel in schema["relationship_schema"]:
        print(f"   Connection: ({rel['src_label']})-[:{rel['rel_type']}]->({rel['dst_label']})")

    # 8. Checkpoint Data
    print("8. Creating atomic disk checkpoint...")
    cp = project.checkpoint()
    print("   Checkpoint status:", cp)

    print("\nDemo completed successfully!")

if __name__ == "__main__":
    run_demo()
```
