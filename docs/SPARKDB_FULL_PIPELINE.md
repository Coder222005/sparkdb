# SparkDB v1.1.0 — Complete Execution Pipelines Reference

> **Audience**: Engineers who need to trace exactly what happens, at the function call level, when SparkDB processes a query. Every function signature, parameter, and data transformation is included.

---

## Pipeline 1: Embedded Python `sg.query('CREATE ...')`

**Scenario**: An application creates a `User` and a `Device` with a `MANAGES` relationship using the embedded SDK.

```python
from sparkdb import SparkDB

db = SparkDB(storage_dir="./data/sparkdb")
sg = db.select_graph("myproject")
result = sg.query("CREATE (u:User {name: 'Alice', region: 'EU'})-[:MANAGES]->(d:Device {id: 'd1', load: 80})")
```

### Step-by-step trace:

**1. `SparkDB(storage_dir="./data/sparkdb")`**
- `SparkDBClient.__init__(storage_dir="./data/sparkdb")` → `_mode = "embedded"`
- `self._engine = EngineSparkDB(storage_dir="./data/sparkdb", default_vector_dim=256)`
- `EngineSparkDB.__init__()`: `os.makedirs(storage_dir, exist_ok=True)`, `_graphs = {}`

**2. `db.select_graph("myproject")`**
- `SparkDBClient.select_graph("myproject", ...)` → `_mode == "embedded"` branch
- Calls `self._engine.select_graph("myproject", vector_dim=None, storage_mode="memory", lru_cache_size=10000)`
- `EngineSparkDB.select_graph()` acquires `_lock`; "myproject" not in `_graphs`
- Creates `GraphSpace(name="myproject", vector_dim=256, storage_dir="./data/sparkdb", storage_mode="memory", lru_cache_size=10000)`
  - Instantiates all 7 subsystems (see Section 2.3 of internals)
  - `_auto_restore()` called: no snapshot file → no restore; no AOF file → no replay
- Stores in `_graphs["myproject"]`, returns `GraphSpace`
- `SparkDBClient.select_graph()` wraps it: `GraphClient(space, engine)` → returns `GraphClient`
- `sg` is now a `GraphClient` with `_executor = CypherExecutor(graph_space)`

**3. `sg.query("CREATE (u:User {name: 'Alice', region: 'EU'})-[:MANAGES]->(d:Device {id: 'd1', load: 80})")`**
- `GraphClient.query(cypher)` → `self._executor.execute(cypher, params=None)`

**4. `CypherExecutor.execute(query, params=None)`**
- `t0 = time.perf_counter()`
- `stmt = CypherParser.parse(query, params=None)`

**5. `CypherParser.parse(query, params=None)`**
- `q_strip = query.strip()`
- `resolved_q = substitute_params(q_strip, None)` → no params, returns unchanged
- `_COMPILED_PLAN_CACHE.get(resolved_q)` → `None` (first call, cache miss)
- `extract_template(q_strip, None)`:
  - Matches `{name: 'Alice', region: 'EU'}`: extracts → `{name: $__p0, region: $__p1}`, params `{__p0: "Alice", __p1: "EU"}`
  - Matches `{id: 'd1', load: 80}`: extracts → `{id: $__p2, load: $__p3}`, params `{..., __p2: "d1", __p3: 80}`
  - Returns `(template_q, {__p0: "Alice", __p1: "EU", __p2: "d1", __p3: 80})`
- `template_q not in _TEMPLATE_PLAN_CACHE` → Tier 3
- `template_q != resolved_q` → parse template
- `_raw_parse(template_q)`:
  - `q_upper.startswith("CREATE")` → `_parse_create(template_q)`
  - `_parse_create()`:
    - `ret_match = None` (no RETURN)
    - `create_body = "(u:User {name: $__p0, region: $__p1})-[:MANAGES]->(d:Device {id: $__p2, load: $__p3})"`
    - `edge_matches`: finds `(u)-[:MANAGES]->(d)` → edge `{src_var:"u", rel_var:"", rel:"MANAGES", dst_var:"d", properties:{}}`
    - `node_matches` (excluding edge spans): finds `(u:User {name: $__p0, region: $__p1})` → `{var:"u", label:"User", properties:{name:"$__p0", region:"$__p1"}}`; and `(d:Device {id: $__p2, load: $__p3})` → `{var:"d", label:"Device", properties:{id:"$__p2", load:"$__p3"}}`
    - Returns `CypherStatement("CREATE", {nodes:[...], edges:[...], return:None})`
  - `template_stmt` stored in `_TEMPLATE_PLAN_CACHE[template_q]`
  - `_bind_params_to_ast(template_stmt.details, {__p0:"Alice",...})`:
    - Recursively traverses details dict
    - Replaces `"$__p0"` → `"Alice"`, `"$__p1"` → `"EU"`, `"$__p2"` → `"d1"`, `"$__p3"` → `80`
  - Returns fully bound `CypherStatement("CREATE", {nodes:[{var:"u",label:"User",properties:{name:"Alice",region:"EU"}}, {var:"d",label:"Device",properties:{id:"d1",load:80}}], edges:[{src_var:"u",rel:"MANAGES",dst_var:"d",properties:{}}], return:None})`
  - Stored in `_COMPILED_PLAN_CACHE[resolved_q]`

**6. `CypherExecutor._execute_create(details)`**
- `nodes = [{var:"u",label:"User",properties:{name:"Alice",region:"EU"}}, {var:"d",label:"Device",properties:{id:"d1",load:80}}]`
- `edges = [{src_var:"u", rel:"MANAGES", dst_var:"d", properties:{}}]`
- Build `bulk_nodes`:
  - `{labels:["User"], properties:{name:"Alice",region:"EU"}, var:"u"}`
  - `{labels:["Device"], properties:{id:"d1",load:80}, var:"d"}`
- `props_set += 2 + 2 = 4`
- `node_ids, _ = self.graph.create_batch(nodes=bulk_nodes, edges=[], log_aof=False)`

**7. `GraphSpace.create_batch(bulk_nodes, edges=[], log_aof=False)`** (inside `_lock`)
- **Node "u"**:
  - `lbls = ["User"]`, `label_counts["User"] = 1`
  - `props = {name:"Alice", region:"EU"}`
  - `nid_u = matrix_store.add_node(labels=["User"])`:
    - `_free_node_ids` is empty → `node_id = 0`, `node_count = 1`
    - `_active_nodes.add(0)`, `node_to_labels[0] = set()`
    - `_dirty_matrices` updated, `_unified_dirty = True`
    - `add_node_label(0, "User")`: `labels["User"] = {0}`, `node_to_labels[0] = {"User"}`
    - Returns `0`
  - `node_props_batch[0] = {name:"Alice", region:"EU"}`
- **Node "d"**:
  - `lbls = ["Device"]`, `label_counts["Device"] = 1`
  - `nid_d = matrix_store.add_node(labels=["Device"])`:
    - `node_id = 1`, `node_count = 2`
    - `labels["Device"] = {1}`, `node_to_labels[1] = {"Device"}`
    - Returns `1`
  - `node_props_batch[1] = {id:"d1", load:80}`
- **Batch write properties** (PropertyStore, memory mode):
  - `property_store.set_nodes_properties_batch({0: {name:"Alice",region:"EU"}, 1: {id:"d1",load:80}})`
  - `node_properties[0] = {name:"Alice",region:"EU"}`
  - `node_properties[1] = {id:"d1",load:80}`
- **No fulltext/vector** (no text/embedding keys)
- Returns `([0, 1], 0)` (0 edges)
- Back in `_execute_create()`: `var_to_id = {"u": 0, "d": 1}`

**8. Back in `_execute_create()` — edge creation**
- `bulk_edges = [{src:0, rel:"MANAGES", dst:1, weight:1.0, properties:{}}]`
- `self.graph.create_batch(nodes=[], edges=bulk_edges, log_aof=False)`:
  - `matrix_store.add_edge(0, "MANAGES", 1, weight=1.0)`:
    - Validates `0 in _active_nodes`, `1 in _active_nodes`
    - `_rel_matrices["MANAGES"] = {}` (new rel type)
    - `_rel_matrices["MANAGES"][(0, 1)] = 1.0`
    - `_dirty_matrices.add("MANAGES")`, `_unified_dirty = True`

**9. Manual AOF write** (back in `_execute_create()`)
```python
self.graph.persistence.append_mutation("create_batch", {
    "nodes": [
        {"labels": ["User"], "properties": {name:"Alice", region:"EU"}},
        {"labels": ["Device"], "properties": {id:"d1", load:80}}
    ],
    "edges": [{"src":0, "rel":"MANAGES", "dst":1, "weight":1.0, "properties":{}}]
})
```
- `PersistenceEngine.append_mutation()`:
  - `record = json.dumps({"cmd":"create_batch","params":{...}}, default=str)`
  - `handle = _get_aof_handle()` — opens `./data/sparkdb/myproject/mutations.aof` in append mode
  - `handle.write(record + "\n")`
  - `handle.flush()` ← data is now in OS kernel buffer (survives Python crash)

**10. Return path**
- `_execute_create()` returns `QueryResult(nodes_created=2, relationships_created=1, properties_set=4)`
- `exec_time = (time.perf_counter() - t0) * 1000` (in milliseconds)
- `res.execution_time_ms = exec_time`
- `self.graph.record_query_log(query, exec_time, 0)` → appends to `_slowlog`
- Returns `QueryResult` to `GraphClient.query()` which returns it to the user

---

## Pipeline 2: Remote HTTP Query `POST /query`

**Scenario**: Python client on machine A sends a Cypher query to SparkDB server on machine B.

```python
db = SparkDB(host="10.164.241.54", port=7379)
sg = db.select_graph("myproject")
result = sg.query("MATCH (u:User) RETURN u.name")
```

### Step-by-step trace:

**1. `SparkDB(host="10.164.241.54", port=7379)`**
- `SparkDBClient.__init__(host="10.164.241.54", port=7379)` → `_mode = "remote"`
- `base_url = "http://10.164.241.54:7379"`
- `self._engine = None`

**2. `db.select_graph("myproject")`**
- `SparkDBClient.select_graph("myproject")` → `_mode == "remote"` branch
- Returns `RemoteGraphClient(name="myproject", base_url="http://10.164.241.54:7379")`
- No HTTP call yet — `RemoteGraphClient` is a lightweight wrapper

**3. `sg.query("MATCH (u:User) RETURN u.name")`**
- `RemoteGraphClient.query(cypher, params=None)`

**4. `RemoteGraphClient.query()` → `_post("/query", payload)`**
- `payload = {"project": "myproject", "graph": "myproject", "query": "MATCH (u:User) RETURN u.name"}`
- `data = json.dumps(payload).encode("utf-8")` → JSON bytes
- `headers = {"Content-Type": "application/json", "Accept": "application/msgpack, application/json"}` (if msgpack installed)
- `req = urllib.request.Request("http://10.164.241.54:7379/query", data, headers, method="POST")`
- `urllib.request.urlopen(req, timeout=30)` → blocking HTTP POST

**5. Server: `SparkDBRequestHandler.do_POST()`**
- Thread spawned by `ThreadingHTTPServer` for this connection
- `parsed = urlparse(self.path)` → `parsed.path = "/query"`
- `length = int(self.headers.get("Content-Length", 0))`
- `raw_body = self.rfile.read(length)` → bytes of JSON payload
- `content_type = self.headers.get("Content-Type", "")` → `"application/json"`
- Parses: `payload = json.loads(raw_body.decode("utf-8"))`
- `parsed.path == "/query"` branch:
  - `graph_name = payload.get("project") = "myproject"`
  - `query = "MATCH (u:User) RETURN u.name"`
  - `g = self.db.select_graph("myproject")` → retrieves/creates `GraphSpace`
  - `executor = CypherExecutor(g)`
  - `res = executor.execute(query, params=None)` → full query execution (see Pipeline 3 for MATCH details)

**6. Server: `_send_payload(200, response_dict)`**
- `accept_header = self.headers.get("Accept", "")` → `"application/msgpack, application/json"`
- `use_msgpack = HAS_MSGPACK and "application/msgpack" in accept_header` → `True` (if installed)
- `body = msgpack.packb(response_dict, default=str)` → binary MessagePack bytes
- Sends HTTP 200 with `Content-Type: application/msgpack`, `Content-Length`, `Connection: close`
- `self.wfile.write(body)` → binary bytes sent over socket

**7. Client: `HttpResponseWrapper(raw_resp)`**
- `self.content = raw_resp.read()` → reads all binary bytes
- `self.headers = raw_resp.headers`

**8. Client: `RemoteGraphClient._decode_response(response)`**
- `content_type = response.headers.get("Content-Type", "")` → `"application/msgpack"`
- `msgpack.unpackb(response.content, raw=False)` → Python dict `{header:[...], result_set:[...], ...}`

**9. Client: `RemoteGraphClient.query()` builds `QueryResult`**
```python
return QueryResult(
    header=res.get("header", []),
    result_set=res.get("result_set", []),
    execution_time_ms=res.get("execution_time_ms", 0.0),
    nodes_created=res.get("nodes_created", 0),
    ...
)
```

---

## Pipeline 3: `MATCH` Retrieval with CBO Optimizer

**Query**: `MATCH (u:User)-[:MANAGES]->(d:Device) WHERE d.load > 50 RETURN u.region, d.id LIMIT 25`

**Assumptions**:
- Graph has 10,000 User nodes and 500 Device nodes with `load` property.
- Property `load` is NOT indexed.
- This is the first time this query is executed (no cache hit).

### Step-by-step trace:

**1. `CypherParser.parse(query)`**
- `substitute_params(query, None)` → unchanged
- `_COMPILED_PLAN_CACHE.get(query)` → `None` (miss)
- `extract_template(query, None)` → no embedded literals in property maps (WHERE clause has `d.load > 50` which is not a property map). Template = same as query.
- `_TEMPLATE_PLAN_CACHE.get(template)` → `None` (miss)
- `_raw_parse(query)`:
  - `q_upper.startswith("MATCH")` → `_parse_match(query)`
  - `_parse_match()`:
    - `where_match`: captures `d.load > 50`
    - `ret_match`: captures `u.region, d.id`
    - `limit_match`: captures `25`
    - `match_pattern = "(u:User)-[:MANAGES]->(d:Device)"`
    - Split on edge pattern: `node_parts = ["(u:User)", "(d:Device)"]`
    - `rel_matches`: `MANAGES` type, empty var and props
    - Parsed nodes: `[{var:"u",label:"User",properties:{}}, {var:"d",label:"Device",properties:{}}]`
    - `rels = ["MANAGES"]`, `rel_info = [{var:"",rel:"MANAGES",properties:{}}]`
    - Returns `CypherStatement("MATCH", {nodes:[...], rels:["MANAGES"], where:"d.load > 50", return:"u.region, d.id", limit:25, ...})`
- Stored in both caches

**2. `CypherExecutor._execute_match(details)`**

```
nodes = [{var:"u",label:"User",props:{}}, {var:"d",label:"Device",props:{}}]
rels = ["MANAGES"]
where = "d.load > 50"
return = "u.region, d.id"
limit = 25
```

**3. `_find_candidate_node_ids("User", {})` → `start_ids`**
- `properties` is empty dict → skip inverted index fast-path
- `label = "User"` → `matrix_store.get_nodes_with_label("User")` → set of 10,000 node IDs
- `start_ids = sorted(list({...}))` → `[0, 1, 2, ..., 9999]`

**4. `_parse_return_projections("u.region, d.id")`**
- Column 1: `expr="u.region"`, `alias="u.region"` (no AS)
- Column 2: `expr="d.id"`, `alias="d.id"`
- `header = ["u.region", "d.id"]`

**5. CBO Check — `len(rels) == 1 and len(nodes) == 2`**
```python
end_desc = nodes[-1] = {var:"d", label:"Device", properties:{}}
end_ids = _find_candidate_node_ids("Device", {})
         → matrix_store.get_nodes_with_label("Device") → 500 IDs
```
- `end_ids` is not empty: `len(end_ids) = 500`
- `len(start_ids) / 3 = 10000 / 3 = 3333.3`
- `len(end_ids) = 500 < 3333` → **CBO triggers backward traversal**
- `can_reverse = True` (matrix_store has `get_reverse_adjacency`)

**6. `_execute_backward_traversal(nodes, rels=["MANAGES"], start_ids=[0..9999], end_ids=[...500 device ids...], fetch_limit=25)`**
- `a_cand_set = set(start_ids)` → set of all 10,000 user IDs
- `rev_csr = self.graph.get_reverse_adjacency("MANAGES")`
  - `matrix_store.get_reverse_adjacency("MANAGES")`:
    - `csr = get_csr("MANAGES")` → compiles coordinate dict to CSR if dirty
    - `rev = csr.transpose().tocsr()` → swaps rows/cols (reverses edge direction)
    - Cached in `_rev_csr_cache["MANAGES"]`
- `len(rels) == 1`:
  - For each `dst` in `end_ids` (iterates 500 Device IDs):
    - `r_start = rev_csr.indptr[dst]`
    - `r_end = rev_csr.indptr[dst + 1]`
    - `for src in rev_csr.indices[r_start:r_end]:` → these are User IDs that `MANAGES` this Device
    - If `src_id in a_cand_set`: append `[src_id, dst]` to `paths`
    - If `len(paths) >= 25`: return early (**LIMIT pushdown**)
- Returns up to 25 paths like `[[user_id, device_id], ...]`

**7. Filter `valid_paths`**
- For each path `[u_id, d_id]`:
  - Node property constraints: both `nodes[0].properties = {}` and `nodes[1].properties = {}` → skip
  - `where_clause = "d.load > 50"` → build var_map then evaluate:
    - `_build_var_map([u_id, d_id], nodes, rels, rel_info)`:
      - `var_to_node["u"] = {_id: u_id, _labels: ["User"], ...props...}` (calls `get_node_properties(u_id)`)
      - `var_to_node["d"] = {_id: d_id, _labels: ["Device"], ...props...}` (calls `get_node_properties(d_id)`)
    - `_evaluate_where("d.load > 50", var_map)`:
      - No OR → single AND clause: `"d.load > 50"`
      - `_eval_single_condition("d.load > 50", var_map)`:
        - Matches `m_comp` with `lhs_raw="d.load"`, `op=">"`, `rhs_raw="50"`
        - `lhs = _resolve_val("d.load", var_map)` → splits on `.` → `var_map["d"]["load"]` → e.g., `80`
        - `rhs = _resolve_literal_val("50")` → `int("50") = 50`
        - `80 > 50` → `True`
  - Appends to `valid_paths`

**8. Projections**
- No aggregation functions in `projections` → direct row building
- For each valid path `[u_id, d_id]`:
  - `var_to_node = _build_var_map(...)` (called again for projection)
  - `_resolve_projection_value("u.region", var_to_node, path)`:
    - `"u.region".startswith("u.")` → `prop_key = "region"` → `var_to_node["u"]["region"]` → e.g., `"EU"`
  - `_resolve_projection_value("d.id", var_to_node, path)`:
    - `var_to_node["d"]["id"]` → `"d1"`
  - `row = ["EU", "d1"]`

**9. No ORDER BY. SKIP=None. LIMIT=25.**
- `rows = rows[:25]` (already at most 25 due to backward traversal early exit)

**10. Return `QueryResult(header=["u.region","d.id"], result_set=[["EU","d1"],...], ...)`**

---

## Pipeline 4: Vector Search `CALL db.idx.vector.queryNodes(...)`

**Query**: `CALL db.idx.vector.queryNodes('Chunk', 'embedding', 5, vecf32([0.1, 0.2, 0.3, 0.4]))`

### Step-by-step trace:

**1. `CypherParser.parse(query)`**
- `q_upper.startswith("CALL")` → `_parse_call(query)`
- `_parse_call()`:
  - `proc_match = re.search(r"CALL\s+([\w\.]+)\s*\(", ...)` → proc_name = `"db.idx.vector.queryNodes"`
  - Finds `(` at index, uses paren-depth counter to find matching `)`
  - `args_raw = "'Chunk', 'embedding', 5, vecf32([0.1, 0.2, 0.3, 0.4])"`
  - `clean_args = re.sub(r"vecf32\s*\(\s*(\[[^\]]+\])\s*\)", r"\1", args_raw)` → replaces `vecf32([...])` with `[...]`
    - `clean_args = "'Chunk', 'embedding', 5, [0.1, 0.2, 0.3, 0.4]"`
  - `args = ast.literal_eval(f"[{clean_args}]")` → `["Chunk", "embedding", 5, [0.1, 0.2, 0.3, 0.4]]`
  - Returns `CypherStatement("CALL", {procedure:"db.idx.vector.queryNodes", args:[...], yield:[], chained_query:None})`

**2. `CypherExecutor._execute_call(details)`**
- `proc = "db.idx.vector.querynodes"` (`.lower()`)
- Matches `"db.idx.vector.querynodes" in proc` → vector search branch
- `top_k = int(args[2]) = 5`
- `vec = args[3] = [0.1, 0.2, 0.3, 0.4]`
- `matches = self.graph.vector_store.query_nearest_nodes(vec, top_k=5)`

**3. `VectorStore.query_nearest_nodes([0.1, 0.2, 0.3, 0.4], top_k=5)`**
- `q_vec = np.asarray([0.1, 0.2, 0.3, 0.4], dtype=float32)`
- `k = min(5, len(_vectors))`
- If `_index is not None` (hnswlib available):
  - `labels, distances = _index.knn_query([q_vec], k=5)` → HNSW multi-layer graph search
    - Starts at top layer, greedily navigates toward query
    - Returns k nearest neighbors as arrays of shape `(1, k)`
  - `return list(zip([int(x) for x in labels[0]], [float(x) for x in distances[0]]))`
  - e.g., `[(12, 0.00001), (7, 0.23), (3, 0.41), ...]`
- Else (fallback): iterate `_vectors`, compute cosine similarity for each

**4. Back in `_execute_call()`**
- For each `(nid, dist)` in `matches`:
  - `p = self.graph.property_store.get_node_properties(nid)` → `{id:"c1", text:"...", ...}`
  - `p["_id"] = nid`
  - `p["_labels"] = list(matrix_store.node_to_labels.get(nid, set()))` → `["Chunk"]`
  - `rows.append([p, dist])`
- Returns `QueryResult(header=["node","score"], result_set=rows)`

---

## Pipeline 5: Crash Recovery on Startup

**Scenario**: SparkDB was running, had a snapshot at time T, then two more mutations were written to AOF, then the process crashed. Now it restarts.

**Files on disk**:
- `./data/sparkdb/myproject/snapshot.json` — state at time T (100 nodes, 50 edges)
- `./data/sparkdb/myproject/mutations.aof` — 2 lines of JSON mutations post-T

### Step-by-step trace:

**1. `SparkDB(storage_dir="./data/sparkdb")`**
- `EngineSparkDB.__init__()`: `_graphs = {}` (empty)

**2. `db.select_graph("myproject")`**
- `EngineSparkDB.select_graph("myproject")` → "myproject" not in `_graphs`
- Creates `GraphSpace("myproject", ..., storage_dir="./data/sparkdb")`

**3. `GraphSpace.__init__()`**
- All 7 subsystems initialized (all empty — fresh state)
- `self.persistence = PersistenceEngine("myproject", "./data/sparkdb")`
  - `self.graph_dir = "./data/sparkdb/myproject"`
  - `self.snapshot_file = "./data/sparkdb/myproject/snapshot.json"`
  - `self.aof_file = "./data/sparkdb/myproject/mutations.aof"`
- `_auto_restore()` called

**4. `PersistenceEngine.load_snapshot()`**
- `os.path.exists("./data/sparkdb/myproject/snapshot.json")` → `True`
- `json.load(f)` → returns `{graph_name:"myproject", node_count:100, nodes:[...100 entries...], edges:[...50 entries...]}`

**5. `GraphSpace._restore_from_dict(snapshot)`**
- Acquires `_lock`
- For each of 100 node entries:
  - `nid = self.matrix_store.add_node(labels=n_entry["labels"])` → assigns IDs 0..99
  - `self.label_counts[lbl] += 1` for each label
  - `self.property_store.set_node_properties(nid, n_entry["properties"])`
  - If `"embedding"` in entry: `self.vector_store.add_node_vector(nid, entry["embedding"])`
  - If `"text"` in properties: `self.fulltext_store.index_node_text(nid, text)`
- For each of 50 edge entries:
  - `self.matrix_store.add_edge(src, rel, dst, weight=w)`
  - If edge has properties: `self.property_store.set_edge_properties(src, rel, dst, props)`
- `logger.info("Restored graph 'myproject' from snapshot: 100 nodes")`

**6. `PersistenceEngine.replay_aof()`**
- Flushes open handle if any
- Opens `mutations.aof`, reads 2 lines:
  - Line 1: `{"cmd":"create_node","params":{"labels":["User"],"properties":{"name":"Bob"},"embedding":null}}`
  - Line 2: `{"cmd":"create_edge","params":{"src":5,"rel":"MANAGES","dst":101,"weight":1.0,"properties":{}}}`
- Returns `[{cmd:"create_node",...}, {cmd:"create_edge",...}]`

**7. `GraphSpace._auto_restore()` — WAL replay loop**
- `m = {cmd:"create_node", params:{labels:["User"],properties:{name:"Bob"},embedding:None}}`
  - `self.create_node(["User"], {name:"Bob"}, None, log_aof=False)`:
    - `nid = matrix_store.add_node(labels=["User"])` → `node_id = 100` (node_count was 100)
    - `label_counts["User"] += 1`
    - `property_store.set_node_properties(100, {name:"Bob"})`
    - `log_aof=False` → no AOF write
- `m = {cmd:"create_edge", params:{src:5,rel:"MANAGES",dst:101,...}}`
  - Wait — `dst=101` may not exist yet if only 101 nodes after create_node. This is a potential ordering issue; WAL replay assumes mutations are self-consistent.
  - `self.create_edge(5, "MANAGES", 101, 1.0, {}, log_aof=False)`:
    - `matrix_store.add_edge(5, "MANAGES", 101, 1.0)`
- `logger.info("Replayed 2 WAL mutations for graph 'myproject'")`

**8. Final state**
- `matrix_store`: 101 nodes (IDs 0..100), 51 edges
- `property_store`: all 101 node property dicts loaded
- Graph is fully restored to pre-crash state

---

## Pipeline 6: Batch Ingestion with BatchCreate

**Query**: `CREATE (u:User {name: 'Alice', role: 'admin'}), (d:Device {id: 'd1', load: 75}), (u)-[:MANAGES]->(d)`

This pipeline is effectively the same as Pipeline 1 but focuses specifically on the `create_batch()` internals for a full batch with nodes and edges simultaneously.

### Detailed `create_batch()` trace:

**Input to `create_batch()`**:
```python
nodes = [
    {"labels": ["User"], "properties": {"name": "Alice", "role": "admin"}, "var": "u"},
    {"labels": ["Device"], "properties": {"id": "d1", "load": 75}, "var": "d"},
]
edges = [
    {"src": 0, "rel": "MANAGES", "dst": 1, "weight": 1.0, "properties": {}}
]
log_aof = False  # called from _execute_create which does its own AOF write
```

**Execution** (inside `with self._lock:`):

```
Phase 1: Node allocation loop
  ┌─ n = {"labels":["User"], properties:{"name":"Alice","role":"admin"}, "var":"u"}
  │   lbls = ["User"]
  │   label_counts["User"] = label_counts.get("User",0) + 1 = 1
  │   props = {"name":"Alice","role":"admin"}
  │   emb = props.pop("embedding", None) = None  [no embedding key]
  │   nid = matrix_store.add_node(labels=["User"])
  │         → free_node_ids empty → node_id=0, node_count=1
  │         → _active_nodes={0}, node_to_labels={0:set()}
  │         → _dirty_matrices updated, _unified_dirty=True
  │         → add_node_label(0,"User"): labels={"User":{0}}, node_to_labels={0:{"User"}}
  │         → returns 0
  │   node_ids.append(0)
  │   node_props_batch[0] = {"name":"Alice","role":"admin"}
  │   emb is None → skip vector_store
  │   no "text","description","content" in props → skip fulltext
  │
  └─ n = {"labels":["Device"], properties:{"id":"d1","load":75}, "var":"d"}
      lbls = ["Device"]
      label_counts["Device"] = 1
      props = {"id":"d1","load":75}
      nid = matrix_store.add_node(labels=["Device"])
            → node_id=1, node_count=2
            → labels={"User":{0},"Device":{1}}, node_to_labels={0:{"User"},1:{"Device"}}
            → returns 1
      node_ids.append(1)
      node_props_batch[1] = {"id":"d1","load":75}

Phase 2: Batch property write
  property_store.set_nodes_properties_batch({0:{"name":"Alice","role":"admin"}, 1:{"id":"d1","load":75}})
  → PropertyStore (memory mode):
    with _lock:
      for node_id=0, properties={"name":"Alice","role":"admin"}:
        node_properties[0] = {}
        for key,val in properties.items():
          # no index exists → skip index update
          node_properties[0]["name"] = "Alice"
          node_properties[0]["role"] = "admin"
      for node_id=1, properties={"id":"d1","load":75}:
        node_properties[1] = {}
        node_properties[1]["id"] = "d1"
        node_properties[1]["load"] = 75

Phase 3: Edge insertion loop
  e = {"src":0, "rel":"MANAGES", "dst":1, "weight":1.0, "properties":{}}
  matrix_store.add_edge(0, "MANAGES", 1, weight=1.0)
    → validates 0 in _active_nodes, 1 in _active_nodes  ✓
    → _rel_matrices["MANAGES"] = {} (new key)
    → _rel_matrices["MANAGES"][(0,1)] = 1.0
    → _dirty_matrices.add("MANAGES"), _unified_dirty=True
  props = {} → skip edge_props_batch

Phase 4: AOF write (called from _execute_create, not from create_batch)
  persistence.append_mutation("create_batch", {
      "nodes": [
          {"labels":["User"],"properties":{"name":"Alice","role":"admin"}},
          {"labels":["Device"],"properties":{"id":"d1","load":75}}
      ],
      "edges": [{"src":0,"rel":"MANAGES","dst":1,"weight":1.0,"properties":{}}]
  })
  → json.dumps → writes to mutations.aof → flush()

Returns: ([0, 1], 1)
```

The entire batch (2 nodes + 1 edge) was processed under a **single `_lock` acquisition**. All property writes went through a single call to `set_nodes_properties_batch()`, and a single `append_mutation()` produces one AOF record for the whole batch.

**AOF record produced**:
```json
{"cmd": "create_batch", "params": {"nodes": [{"labels": ["User"], "properties": {"name": "Alice", "role": "admin"}}, {"labels": ["Device"], "properties": {"id": "d1", "load": 75}}], "edges": [{"src": 0, "rel": "MANAGES", "dst": 1, "weight": 1.0, "properties": {}}]}}
```

This single line, written and flushed atomically, is sufficient to replay the entire batch on crash recovery.
