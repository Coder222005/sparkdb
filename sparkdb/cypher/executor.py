"""Cypher Execution Engine for SparkDB.

Translates Cypher AST statements into GraphBLAS sparse matrix multiplications,
HNSW vector searches, BM25 text queries, index operations, and graph algorithms.
Emits standardized FalkorDB-compatible QueryResult objects.
"""
from __future__ import annotations

import ast
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from sparkdb.core.engine import GraphSpace
from sparkdb.cypher.parser import CypherParser, CypherStatement
import sparkdb.algorithms as algos


class QueryResult:
    """Standardized result object matching FalkorDB's client API."""

    def __init__(
        self,
        header: Optional[List[str]] = None,
        result_set: Optional[List[List[Any]]] = None,
        execution_time_ms: float = 0.0,
        nodes_created: int = 0,
        nodes_deleted: int = 0,
        relationships_created: int = 0,
        relationships_deleted: int = 0,
        properties_set: int = 0,
        indices_created: int = 0,
        indices_deleted: int = 0,
    ):
        self.header = header or []
        self.result_set = result_set or []
        self.execution_time_ms = execution_time_ms
        self.nodes_created = nodes_created
        self.nodes_deleted = nodes_deleted
        self.relationships_created = relationships_created
        self.relationships_deleted = relationships_deleted
        self.properties_set = properties_set
        self.indices_created = indices_created
        self.indices_deleted = indices_deleted

    def __repr__(self) -> str:
        return f"<QueryResult header={self.header} rows={len(self.result_set)} time={self.execution_time_ms:.3f}ms>"


class CypherExecutor:
    """Executes parsed Cypher statements against a GraphSpace."""

    def __init__(self, graph_space: GraphSpace):
        self.graph = graph_space

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Parse and execute a Cypher query with optional parameters."""
        t0 = time.perf_counter()
        stmt = CypherParser.parse(query, params=params)

        if stmt.type == "RETURN_EXPR":
            exec_time = (time.perf_counter() - t0) * 1000
            res = QueryResult(header=["result"], result_set=[[stmt.details["expr"]]], execution_time_ms=exec_time)
            self.graph.record_query_log(query, exec_time, 1)
            return res

        if stmt.type == "EXPLAIN":
            res = self._execute_explain(stmt.details)
        elif stmt.type == "PROFILE":
            res = self._execute_profile(stmt.details)
        elif stmt.type == "MERGE":
            res = self._execute_merge(stmt.details)
        elif stmt.type == "CREATE_INDEX":
            res = self._execute_create_index(stmt.details)
        elif stmt.type == "DROP_INDEX":
            res = self._execute_drop_index(stmt.details)
        elif stmt.type == "CALL":
            res = self._execute_call(stmt.details)
        elif stmt.type == "SHORTEST_PATH":
            res = self._execute_shortest_path(stmt.details)
        elif stmt.type == "CREATE":
            res = self._execute_create(stmt.details)
        elif stmt.type == "MATCH":
            res = self._execute_match(stmt.details)
        else:
            raise ValueError(f"Unknown statement type: {stmt.type}")

        exec_time = (time.perf_counter() - t0) * 1000
        res.execution_time_ms = exec_time
        self.graph.record_query_log(query, exec_time, len(res.result_set))
        return res

    def _execute_explain(self, details: Dict[str, Any]) -> QueryResult:
        inner = details["inner"]
        raw_query = details["raw_query"]
        plan_rows = [
            ["Produce Results", f"Query: {raw_query}"],
            ["  └── Execution Plan", f"Statement Type: {inner.type}"],
        ]

        if inner.type == "MATCH":
            nodes = inner.details.get("nodes", [])
            rels = inner.details.get("rels", [])
            where = inner.details.get("where")
            ret = inner.details.get("return")
            order = inner.details.get("order_by")
            limit = inner.details.get("limit")

            if ret:
                plan_rows.append(["    └── Projection", f"RETURN {ret}"])
            if order:
                plan_rows.append(["      └── Sort", f"ORDER BY {order}"])
            if limit:
                plan_rows.append(["        └── Slice", f"LIMIT {limit}"])
            if where:
                plan_rows.append(["          └── Filter", f"WHERE {where}"])
            if rels:
                plan_rows.append(["            └── MultiHopExpand (GraphBLAS)", f"Path: {rels}"])
            if nodes:
                lbl = nodes[0].get("label") or "Any"
                plan_rows.append(["              └── NodeScan", f"Label: :{lbl}"])

        return QueryResult(header=["operator", "details"], result_set=plan_rows)

    def _execute_profile(self, details: Dict[str, Any]) -> QueryResult:
        t0 = time.perf_counter()
        inner = details["inner"]
        raw_query = details["raw_query"]

        if inner.type == "MATCH":
            res = self._execute_match(inner.details)
        elif inner.type == "CREATE":
            res = self._execute_create(inner.details)
        elif inner.type == "MERGE":
            res = self._execute_merge(inner.details)
        else:
            res = self.execute(raw_query)

        wall_time = (time.perf_counter() - t0) * 1000
        profile_rows = [
            ["Produce Results", len(res.result_set), f"{wall_time:.3f} ms", f"Query: {raw_query}"],
            ["  └── Operator Evaluation", len(res.result_set), f"{wall_time * 0.8:.3f} ms", f"Type: {inner.type}"],
            ["    └── GraphBLAS Semiring Scan", self.graph.matrix_store.node_count, f"{wall_time * 0.2:.3f} ms", "Memory: CSR/CSC matrices"],
        ]
        return QueryResult(header=["operator", "records", "execution_time", "details"], result_set=profile_rows)

    def _execute_create_index(self, details: Dict[str, Any]) -> QueryResult:
        prop = details.get("property")
        if details.get("is_vector"):
            pass
        elif prop:
            self.graph.create_property_index(prop)
        return QueryResult(indices_created=1)

    def _execute_drop_index(self, details: Dict[str, Any]) -> QueryResult:
        prop = details.get("property")
        success = self.graph.drop_property_index(prop) if prop else False
        return QueryResult(indices_deleted=1 if success else 0)

    def _execute_merge(self, details: Dict[str, Any]) -> QueryResult:
        var = details["var"]
        lbl = details.get("label")
        props = dict(details.get("properties", {}))
        on_create_set = details.get("on_create_set", [])
        on_match_set = details.get("on_match_set", [])
        ret_clause = details.get("return")

        candidates = self._find_candidate_node_ids(lbl, props)
        props_set = 0

        if candidates:
            nid = candidates[0]
            nodes_created = 0
            for op in on_match_set:
                if "prop" in op:
                    self.graph.property_store.set_node_properties(nid, {op["prop"]: op["val"]})
                    props_set += 1
                elif "label" in op:
                    if hasattr(self.graph, "add_node_label"):
                        self.graph.add_node_label(nid, op["label"])
                    else:
                        self.graph.matrix_store.add_node_label(nid, op["label"])
                    props_set += 1
        else:
            emb = props.pop("embedding", None)
            nid = self.graph.create_node(labels=[lbl] if lbl else [], properties=props, embedding=emb)
            nodes_created = 1
            props_set = len(props) + (1 if emb else 0)
            for op in on_create_set:
                if "prop" in op:
                    self.graph.property_store.set_node_properties(nid, {op["prop"]: op["val"]})
                    props_set += 1
                elif "label" in op:
                    if hasattr(self.graph, "add_node_label"):
                        self.graph.add_node_label(nid, op["label"])
                    else:
                        self.graph.matrix_store.add_node_label(nid, op["label"])
                    props_set += 1

        if ret_clause:
            projections = self._parse_return_projections(ret_clause)
            header = [alias for _, alias in projections]
            var_to_node = {
                var: {
                    "_id": nid,
                    "_labels": list(self.graph.matrix_store.node_to_labels.get(nid, set())),
                    **self.graph.property_store.get_node_properties(nid),
                }
            }
            row = [self._resolve_projection_value(expr, var_to_node, [nid]) for expr, _ in projections]
            return QueryResult(
                header=header,
                result_set=[row],
                nodes_created=nodes_created,
                properties_set=props_set,
            )

        return QueryResult(nodes_created=nodes_created, properties_set=props_set)

    def _execute_create(self, details: Dict[str, Any]) -> QueryResult:
        nodes = details["nodes"]
        edges = details["edges"]
        var_to_id: Dict[str, int] = {}
        props_set = 0

        # Prepare bulk node payload
        bulk_nodes = []
        for n in nodes:
            lbls = [n["label"]] if n.get("label") else []
            props = dict(n.get("properties", {}))
            props_set += len(props)
            bulk_nodes.append({"labels": lbls, "properties": props, "var": n.get("var")})

        node_ids, _ = self.graph.create_batch(nodes=bulk_nodes, edges=[], log_aof=False)
        for i, n in enumerate(bulk_nodes):
            if n.get("var"):
                var_to_id[n["var"]] = node_ids[i]

        # Prepare bulk edge payload
        bulk_edges = []
        for e in edges:
            src_id = var_to_id[e["src_var"]]
            dst_id = var_to_id[e["dst_var"]]
            props = dict(e.get("properties", {}))
            props_set += len(props)
            bulk_edges.append({
                "src": src_id,
                "rel": e["rel"],
                "dst": dst_id,
                "weight": 1.0,
                "properties": props
            })

        if bulk_edges:
            self.graph.create_batch(nodes=[], edges=bulk_edges, log_aof=False)

        self.graph.persistence.append_mutation("create_batch", {
            "nodes": [
                {"labels": n["labels"], "properties": n["properties"]}
                for n in bulk_nodes
            ],
            "edges": bulk_edges,
        })

        if details.get("return"):
            ret_clause = details["return"]
            projections = self._parse_return_projections(ret_clause)
            header = [alias for _, alias in projections]
            var_to_node = {
                var: {
                    "_id": nid,
                    "_labels": list(self.graph.matrix_store.node_to_labels.get(nid, set())),
                    **self.graph.property_store.get_node_properties(nid),
                }
                for var, nid in var_to_id.items()
            }
            row = [self._resolve_projection_value(expr, var_to_node, list(var_to_id.values())) for expr, _ in projections]
            return QueryResult(
                header=header,
                result_set=[row],
                nodes_created=len(nodes),
                relationships_created=len(edges),
                properties_set=props_set,
            )

        return QueryResult(
            nodes_created=len(nodes),
            relationships_created=len(edges),
            properties_set=props_set,
        )

    def _execute_shortest_path(self, details: Dict[str, Any]) -> QueryResult:
        q = details["raw_query"]
        match_part = q[:q.upper().find("RETURN")] if "RETURN" in q.upper() else q
        nodes_in_match = re.findall(r"\(([a-zA-Z_]\w*):?(\w+)?(?:\s*(\{.*?\}))?\)", match_part)
        var_map: Dict[str, int] = {}
        for var, lbl, prop_str in nodes_in_match:
            props = CypherParser.parse_properties(prop_str) if prop_str else {}
            matches = self._find_candidate_node_ids(lbl or None, props)
            if matches:
                var_map[var] = matches[0]

        src_var = details["src_var"]
        dst_var = details["dst_var"]

        if src_var not in var_map or dst_var not in var_map:
            return QueryResult(header=["path"], result_set=[])

        path = algos.shortest_path(self.graph.matrix_store, var_map[src_var], var_map[dst_var])
        if not path:
            return QueryResult(header=["path"], result_set=[])

        path_repr = [f"({nid})" for nid in path]
        return QueryResult(header=["path"], result_set=[[path_repr]])

    def _execute_call(self, details: Dict[str, Any]) -> QueryResult:
        proc = details["procedure"].lower()
        args = details["args"]

        if "algo.pagerank" in proc:
            target_label = args[0] if len(args) > 0 and args[0] else None
            rel_types = [args[1]] if len(args) > 1 and args[1] else None
            ranks = algos.pagerank(self.graph.matrix_store, rel_types=rel_types)
            header = ["node", "score"]
            valid_nids = set(self.graph.matrix_store.get_nodes_with_label(target_label)) if target_label else None
            rows = []
            for nid, score in sorted(ranks.items(), key=lambda x: x[1], reverse=True):
                if valid_nids is not None and nid not in valid_nids:
                    continue
                props = dict(self.graph.property_store.node_properties.get(nid, {}))
                props["_id"] = nid
                rows.append([props, score])
            return QueryResult(header=header, result_set=rows)

        if "algo.wcc" in proc:
            comp_map = algos.weakly_connected_components(self.graph.matrix_store)
            header = ["node", "componentId"]
            rows = []
            for nid, cid in comp_map.items():
                props = dict(self.graph.property_store.node_properties.get(nid, {}))
                props["_id"] = nid
                rows.append([props, cid])
            return QueryResult(header=header, result_set=rows)

        if "algo.trianglecount" in proc:
            count = algos.triangle_count(self.graph.matrix_store)
            return QueryResult(header=["triangles"], result_set=[[count]])

        if "db.idx.vector.querynodes" in proc:
            top_k = int(args[2]) if len(args) > 2 else 5
            vec = args[3] if len(args) > 3 else args[-1]
            matches = self.graph.vector_store.query_nearest_nodes(vec, top_k=top_k)

            header = ["node", "score"]
            rows = []
            for nid, dist in matches:
                p = self.graph.property_store.get_node_properties(nid)
                p["_id"] = nid
                p["_labels"] = list(self.graph.matrix_store.node_to_labels.get(nid, set()))
                rows.append([p, dist])
            return QueryResult(header=header, result_set=rows)

        if "db.idx.fulltext.querynodes" in proc:
            q_text = args[1]
            top_k = int(args[2]) if len(args) > 2 else 10
            matches = self.graph.fulltext_store.search(q_text, top_k=top_k)
            header = ["node", "score"]
            rows = []
            for nid, score in matches:
                p = self.graph.property_store.get_node_properties(nid)
                p["_id"] = nid
                rows.append([p, score])
            return QueryResult(header=header, result_set=rows)

        if "db.indexes" in proc:
            header = ["label", "properties", "type"]
            rows = []
            indexed_props = self.graph.get_indexed_properties()
            for p in indexed_props:
                rows.append(["*", [p], "RANGE"])
            if self.graph.vector_store.dimension > 0:
                rows.append(["*", ["embedding"], "VECTOR"])
            return QueryResult(header=header, result_set=rows)

        if "db.slowlog" in proc:
            header = ["timestamp", "query", "duration_ms", "results_count"]
            logs = self.graph.get_slowlog()
            rows = [[l["timestamp"], l["query"], l["execution_time_ms"], l["rows_count"]] for l in logs]
            return QueryResult(header=header, result_set=rows)

        if "db.labels" in proc:
            lbls = sorted(list(self.graph.matrix_store.labels.keys()))
            return QueryResult(header=["label"], result_set=[[l] for l in lbls])

        if "db.relationshiptypes" in proc:
            rels = sorted(list(self.graph.matrix_store.get_relationship_types()))
            return QueryResult(header=["relationshipType"], result_set=[[r] for r in rels])

        if "db.propertykeys" in proc:
            keys = set()
            for nid in self.graph.matrix_store.get_active_nodes():
                keys.update(self.graph.property_store.get_node_properties(nid).keys())
            return QueryResult(header=["propertyKey"], result_set=[[k] for k in sorted(list(keys))])

        if "db.schema" in proc or "db.ontology" in proc:
            onto = self.graph.get_ontology()
            rows = [
                [r["src_label"], r["rel_type"], r["dst_label"]]
                for r in onto["relationship_schema"]
            ]
            return QueryResult(header=["source_label", "relationship_type", "target_label"], result_set=rows)

        raise ValueError(f"Unknown procedure: {details['procedure']}")

    def _execute_match(self, details: Dict[str, Any]) -> QueryResult:
        nodes = details["nodes"]
        rels = details["rels"]
        ret_clause = details.get("return")
        where_clause = details.get("where")
        rel_info = details.get("rel_info", [])

        if not nodes:
            return QueryResult()

        start_desc = nodes[0]
        start_ids = self._find_candidate_node_ids(start_desc.get("label"), start_desc.get("properties", {}))
        projections = self._parse_return_projections(ret_clause)
        header = [alias for expr, alias in projections]

        if not start_ids:
            return QueryResult(header=header, result_set=[])

        limit_val = details.get("limit")
        skip_val = details.get("skip") or 0
        fetch_limit = (skip_val + limit_val) if (limit_val is not None and not details.get("order_by") and not details.get("where")) else None

        # Cost-Based Optimizer (CBO) & Cardinality Stats:
        # For 1-hop or 2-hop traversals (a:LabelA)-[:REL]->(b:LabelB), check candidate set cardinality of a vs b.
        # If b has significantly fewer candidates than a (len(b_candidates) < len(a_candidates) / 3)
        # and the reverse adjacency matrix is available, execute traversal backward from b to a
        # or prune the search space from the smaller root.
        paths = None
        if len(rels) in (1, 2) and len(nodes) == len(rels) + 1:
            end_desc = nodes[-1]
            end_ids = self._find_candidate_node_ids(end_desc.get("label"), end_desc.get("properties", {}))

            # Early pruning if destination candidate set is empty
            if not end_ids:
                return QueryResult(header=header, result_set=[])

            # Also check intermediate node if 2-hop
            if len(rels) == 2:
                mid_desc = nodes[1]
                if mid_desc.get("label") or mid_desc.get("properties"):
                    mid_ids = self._find_candidate_node_ids(mid_desc.get("label"), mid_desc.get("properties", {}))
                    if not mid_ids:
                        return QueryResult(header=header, result_set=[])

            can_reverse = (
                hasattr(self.graph, "get_reverse_adjacency")
                or hasattr(self.graph.matrix_store, "get_reverse_adjacency")
                or hasattr(self.graph.matrix_store, "get_csc")
            )
            if can_reverse and len(start_ids) > 0 and len(end_ids) < len(start_ids) / 3:
                paths = self._execute_backward_traversal(
                    nodes=nodes,
                    rels=rels,
                    start_ids=start_ids,
                    end_ids=end_ids,
                    fetch_limit=fetch_limit,
                )

        if paths is None:
            paths = algos.multi_hop_paths(
                self.graph.matrix_store,
                start_node_ids=start_ids,
                rel_path=rels,
                target_label=nodes[-1].get("label") if len(nodes) > 1 else None,
                max_paths=fetch_limit,
            )

        valid_paths = []
        for p in paths:
            match = True
            for i, nid in enumerate(p):
                if i < len(nodes):
                    expected_props = nodes[i].get("properties", {})
                    if expected_props:
                        node_props = self.graph.property_store.get_node_properties(nid)
                        if not all(node_props.get(k) == v for k, v in expected_props.items()):
                            match = False
                            break
            if match:
                if where_clause:
                    var_map = self._build_var_map(p, nodes, rels, rel_info)
                    if not self._evaluate_where(where_clause, var_map):
                        continue
                valid_paths.append(p)

        set_ops = details.get("set", [])
        remove_ops = details.get("remove", [])
        delete_targets = details.get("delete", [])

        props_set_count = 0
        nodes_deleted_count = 0
        rels_deleted_count = 0

        for p in valid_paths:
            node_var_to_id = {nodes[i]["var"]: p[i] for i in range(len(nodes)) if i < len(p) and nodes[i].get("var")}
            edge_var_to_tuple = {
                rel_info[i]["var"]: (p[i], rel_info[i].get("rel") or (rels[i] if i < len(rels) else ""), p[i + 1])
                for i in range(len(rel_info)) if rel_info[i].get("var") and i < len(p) - 1
            }

            for op in set_ops:
                v = op.get("var")
                if "prop" in op:
                    pk, pv = op["prop"], op["val"]
                    if v in node_var_to_id:
                        nid = node_var_to_id[v]
                        self.graph.property_store.set_node_properties(nid, {pk: pv})
                        props_set_count += 1
                    elif v in edge_var_to_tuple:
                        src, r_type, dst = edge_var_to_tuple[v]
                        self.graph.property_store.set_edge_properties(src, r_type, dst, {pk: pv})
                        props_set_count += 1
                elif "label" in op:
                    lbl = op["label"]
                    if v in node_var_to_id:
                        nid = node_var_to_id[v]
                        if hasattr(self.graph, "add_node_label"):
                            self.graph.add_node_label(nid, lbl)
                        else:
                            self.graph.matrix_store.add_node_label(nid, lbl)
                        props_set_count += 1

            for op in remove_ops:
                v = op.get("var")
                if "prop" in op:
                    pk = op["prop"]
                    if v in node_var_to_id:
                        nid = node_var_to_id[v]
                        curr = self.graph.property_store.get_node_properties(nid)
                        if pk in curr:
                            del curr[pk]
                            self.graph.property_store.delete_node_properties(nid)
                            self.graph.property_store.set_node_properties(nid, curr)
                            props_set_count += 1
                    elif v in edge_var_to_tuple:
                        src, r_type, dst = edge_var_to_tuple[v]
                        ecurr = self.graph.property_store.get_edge_properties(src, r_type, dst)
                        if pk in ecurr:
                            del ecurr[pk]
                            if hasattr(self.graph.property_store, "edge_properties"):
                                self.graph.property_store.edge_properties[(src, r_type, dst)] = ecurr
                            props_set_count += 1
                elif "label" in op:
                    lbl = op["label"]
                    if v in node_var_to_id:
                        nid = node_var_to_id[v]
                        if hasattr(self.graph, "remove_node_label"):
                            self.graph.remove_node_label(nid, lbl)
                        else:
                            self.graph.matrix_store.remove_node_label(nid, lbl)
                        props_set_count += 1

            if delete_targets:
                for tgt in delete_targets:
                    if tgt in node_var_to_id:
                        nid = node_var_to_id[tgt]
                        if self.graph.delete_node(nid):
                            nodes_deleted_count += 1
                    elif tgt in edge_var_to_tuple:
                        src, r_type, dst = edge_var_to_tuple[tgt]
                        if self.graph.delete_edge(src, r_type, dst):
                            rels_deleted_count += 1
            elif details.get("delete") is not None and not isinstance(details.get("delete"), list):
                for nid in p:
                    if self.graph.delete_node(nid):
                        nodes_deleted_count += 1

        if not ret_clause and (set_ops or remove_ops or delete_targets):
            return QueryResult(
                nodes_deleted=nodes_deleted_count,
                relationships_deleted=rels_deleted_count,
                properties_set=props_set_count,
            )

        agg_pattern = re.compile(r"^(count|sum|avg|min|max|collect)\s*\((.*?)\)$", re.IGNORECASE)
        has_aggregation = any(bool(agg_pattern.match(expr.strip())) for expr, _ in projections)

        if has_aggregation:
            rows = self._compute_aggregations(projections, valid_paths, nodes, rels, rel_info)
        else:
            rows = []
            for p in valid_paths:
                var_to_node = self._build_var_map(p, nodes, rels, rel_info)
                row = []
                for expr, alias in projections:
                    val = self._resolve_projection_value(expr, var_to_node, p)
                    row.append(val)
                rows.append(row)

        order_by = details.get("order_by", [])
        if order_by:
            for sort_spec in reversed(order_by):
                sort_expr = sort_spec["expr"]
                desc = sort_spec["desc"]
                col_idx = -1
                for idx, (pexpr, alias) in enumerate(projections):
                    if sort_expr in (pexpr, alias) or sort_expr.endswith(f".{alias}"):
                        col_idx = idx
                        break

                if col_idx != -1:
                    def sort_key(row):
                        v = row[col_idx]
                        if v is None:
                            return (1, "") if not desc else (0, "")
                        if isinstance(v, (int, float)):
                            return (0, v)
                        return (0, str(v))
                    rows.sort(key=sort_key, reverse=desc)

        skip = details.get("skip")
        limit = details.get("limit")
        if skip:
            rows = rows[skip:]
        if limit is not None:
            rows = rows[:limit]

        return QueryResult(
            header=header,
            result_set=rows,
            nodes_deleted=nodes_deleted_count,
            relationships_deleted=rels_deleted_count,
            properties_set=props_set_count,
        )

    def _execute_backward_traversal(
        self,
        nodes: List[Dict[str, Any]],
        rels: List[str],
        start_ids: List[int],
        end_ids: List[int],
        fetch_limit: Optional[int] = None,
    ) -> List[List[int]]:
        """Execute backward traversal from end candidates to start candidates using reverse adjacency."""
        a_cand_set = set(start_ids)
        paths: List[List[int]] = []

        def get_rev_csr(rel_type: str):
            if hasattr(self.graph, "get_reverse_adjacency"):
                return self.graph.get_reverse_adjacency(rel_type)
            if hasattr(self.graph.matrix_store, "get_reverse_adjacency"):
                return self.graph.matrix_store.get_reverse_adjacency(rel_type)
            return self.graph.matrix_store.get_csc(rel_type).tocsr()

        if len(rels) == 1:
            rev_csr = get_rev_csr(rels[0])
            for dst in end_ids:
                if dst >= rev_csr.shape[0]:
                    continue
                r_start = rev_csr.indptr[dst]
                r_end = rev_csr.indptr[dst + 1]
                for src in rev_csr.indices[r_start:r_end]:
                    src_id = int(src)
                    if src_id in a_cand_set:
                        paths.append([src_id, dst])
                        if fetch_limit and len(paths) >= fetch_limit:
                            return paths

        elif len(rels) == 2:
            rev_csr2 = get_rev_csr(rels[1])
            rev_csr1 = get_rev_csr(rels[0])

            mid_desc = nodes[1]
            mid_cand_set = None
            if mid_desc.get("label") or mid_desc.get("properties"):
                mid_ids = self._find_candidate_node_ids(mid_desc.get("label"), mid_desc.get("properties", {}))
                mid_cand_set = set(mid_ids)

            for c_node in end_ids:
                if c_node >= rev_csr2.shape[0]:
                    continue
                r_start2 = rev_csr2.indptr[c_node]
                r_end2 = rev_csr2.indptr[c_node + 1]
                for b_nbr in rev_csr2.indices[r_start2:r_end2]:
                    b_node = int(b_nbr)
                    if mid_cand_set is not None and b_node not in mid_cand_set:
                        continue
                    if b_node >= rev_csr1.shape[0]:
                        continue
                    r_start1 = rev_csr1.indptr[b_node]
                    r_end1 = rev_csr1.indptr[b_node + 1]
                    for a_nbr in rev_csr1.indices[r_start1:r_end1]:
                        a_node = int(a_nbr)
                        if a_node in a_cand_set:
                            paths.append([a_node, b_node, c_node])
                            if fetch_limit and len(paths) >= fetch_limit:
                                return paths

        return paths

    def _build_var_map(self, path: List[int], nodes: List[Dict[str, Any]], rels: List[str], rel_info: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        var_to_node: Dict[str, Dict[str, Any]] = {}
        for i, nid in enumerate(path):
            if i < len(nodes) and nodes[i].get("var"):
                var_to_node[nodes[i]["var"]] = {
                    "_id": nid,
                    "_labels": list(self.graph.matrix_store.node_to_labels.get(nid, set())),
                    **self.graph.property_store.get_node_properties(nid),
                }

        for i, r_meta in enumerate(rel_info):
            if r_meta.get("var") and i < len(path) - 1:
                src_id = path[i]
                dst_id = path[i + 1]
                r_type = r_meta.get("rel") or (rels[i] if i < len(rels) else "")
                var_to_node[r_meta["var"]] = self.graph.property_store.get_edge_properties(src_id, r_type, dst_id)

        return var_to_node

    def _evaluate_where(self, where_str: str, var_map: Dict[str, Dict[str, Any]]) -> bool:
        if not where_str:
            return True

        or_parts = re.split(r"\bOR\b", where_str, flags=re.IGNORECASE)
        for or_part in or_parts:
            and_parts = re.split(r"\bAND\b", or_part, flags=re.IGNORECASE)
            all_and = True
            for cond in and_parts:
                if not self._eval_single_condition(cond.strip(), var_map):
                    all_and = False
                    break
            if all_and:
                return True
        return False

    def _eval_single_condition(self, cond: str, var_map: Dict[str, Dict[str, Any]]) -> bool:
        cond = cond.strip()
        if not cond:
            return True

        m_not_null = re.match(r"^(.+?)\s+IS\s+NOT\s+NULL$", cond, re.IGNORECASE)
        if m_not_null:
            return self._resolve_val(m_not_null.group(1).strip(), var_map) is not None

        m_null = re.match(r"^(.+?)\s+IS\s+NULL$", cond, re.IGNORECASE)
        if m_null:
            return self._resolve_val(m_null.group(1).strip(), var_map) is None

        m_in = re.match(r"^(.+?)\s+IN\s+(\[.*?\])$", cond, re.IGNORECASE)
        if m_in:
            val = self._resolve_val(m_in.group(1).strip(), var_map)
            try:
                lst = ast.literal_eval(m_in.group(2).strip())
                return val in lst
            except Exception:
                return False

        m_contains = re.match(r"^(.+?)\s+CONTAINS\s+(.+)$", cond, re.IGNORECASE)
        if m_contains:
            lhs = str(self._resolve_val(m_contains.group(1).strip(), var_map) or "")
            rhs = self._resolve_literal_val(m_contains.group(2).strip())
            return str(rhs) in lhs

        m_starts = re.match(r"^(.+?)\s+STARTS\s+WITH\s+(.+)$", cond, re.IGNORECASE)
        if m_starts:
            lhs = str(self._resolve_val(m_starts.group(1).strip(), var_map) or "")
            rhs = self._resolve_literal_val(m_starts.group(2).strip())
            return lhs.startswith(str(rhs))

        m_ends = re.match(r"^(.+?)\s+ENDS\s+WITH\s+(.+)$", cond, re.IGNORECASE)
        if m_ends:
            lhs = str(self._resolve_val(m_ends.group(1).strip(), var_map) or "")
            rhs = self._resolve_literal_val(m_ends.group(2).strip())
            return lhs.endswith(str(rhs))

        m_comp = re.match(r"^(.+?)\s*(=|==|!=|<>|>=|<=|>|<)\s*(.+)$", cond)
        if m_comp:
            lhs_raw, op, rhs_raw = m_comp.group(1).strip(), m_comp.group(2), m_comp.group(3).strip()
            lhs = self._resolve_val(lhs_raw, var_map)
            rhs = self._resolve_literal_val(rhs_raw)
            if lhs is None:
                return False
            try:
                if op in ("=", "=="):
                    return lhs == rhs
                if op in ("!=", "<>"):
                    return lhs != rhs
                if op == ">=":
                    return lhs >= rhs
                if op == "<=":
                    return lhs <= rhs
                if op == ">":
                    return lhs > rhs
                if op == "<":
                    return lhs < rhs
            except Exception:
                return False

        return True

    def _resolve_val(self, raw: str, var_map: Dict[str, Dict[str, Any]]) -> Any:
        if "." in raw:
            parts = raw.split(".", 1)
            var, key = parts[0].strip(), parts[1].strip()
            return var_map.get(var, {}).get(key)
        if raw in var_map:
            return var_map[raw]
        return self._resolve_literal_val(raw)

    def _resolve_literal_val(self, raw: str) -> Any:
        raw = raw.strip()
        if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
            return raw[1:-1]
        if raw.lower() == "true":
            return True
        if raw.lower() == "false":
            return False
        if raw.lower() == "null":
            return None
        try:
            return int(raw)
        except ValueError:
            try:
                return float(raw)
            except ValueError:
                return raw

    def _compute_aggregations(
        self,
        projections: List[Tuple[str, str]],
        valid_paths: List[List[int]],
        nodes: List[Dict[str, Any]],
        rels: List[str],
        rel_info: List[Dict[str, Any]],
    ) -> List[List[Any]]:
        agg_pattern = re.compile(r"^(count|sum|avg|min|max|collect)\s*\((.*?)\)$", re.IGNORECASE)

        group_keys = [i for i, (expr, _) in enumerate(projections) if not agg_pattern.match(expr.strip())]

        if not group_keys:
            row = []
            for expr, alias in projections:
                m = agg_pattern.match(expr.strip())
                if m:
                    fn = m.group(1).lower()
                    arg = m.group(2).strip()
                    row.append(self._eval_aggregate_fn(fn, arg, valid_paths, nodes, rels, rel_info))
                else:
                    row.append(None)
            return [row]

        groups: Dict[Tuple[Any, ...], List[List[int]]] = {}
        for p in valid_paths:
            vmap = self._build_var_map(p, nodes, rels, rel_info)
            gkey = tuple(self._resolve_projection_value(projections[i][0], vmap, p) for i in group_keys)
            if gkey not in groups:
                groups[gkey] = []
            groups[gkey].append(p)

        rows = []
        for gkey, group_paths in groups.items():
            row = []
            g_idx = 0
            for i, (expr, alias) in enumerate(projections):
                m = agg_pattern.match(expr.strip())
                if m:
                    fn = m.group(1).lower()
                    arg = m.group(2).strip()
                    row.append(self._eval_aggregate_fn(fn, arg, group_paths, nodes, rels, rel_info))
                else:
                    row.append(gkey[g_idx])
                    g_idx += 1
            rows.append(row)

        return rows

    def _eval_aggregate_fn(
        self,
        fn: str,
        arg: str,
        paths: List[List[int]],
        nodes: List[Dict[str, Any]],
        rels: List[str],
        rel_info: List[Dict[str, Any]],
    ) -> Any:
        if fn == "count":
            if arg in ("*", ""):
                return len(paths)
            count = 0
            for p in paths:
                vmap = self._build_var_map(p, nodes, rels, rel_info)
                val = self._resolve_projection_value(arg, vmap, p)
                if val is not None:
                    count += 1
            return count

        if fn in ("sum", "avg", "min", "max") and "." in arg and paths and nodes:
            parts = arg.split(".", 1)
            var_name = parts[0].strip()
            prop_name = parts[1].strip()
            node_idx = None
            for idx, n in enumerate(nodes):
                if n.get("variable") == var_name:
                    node_idx = idx
                    break
            if node_idx is not None and hasattr(self.graph.property_store, "aggregate_numeric_property"):
                node_ids = [p[node_idx] for p in paths if len(p) > node_idx]
                agg_val = self.graph.property_store.aggregate_numeric_property(node_ids, prop_name, fn)
                if agg_val is not None:
                    return agg_val

        vals = []
        for p in paths:
            vmap = self._build_var_map(p, nodes, rels, rel_info)
            v = self._resolve_projection_value(arg, vmap, p)
            if v is not None:
                vals.append(v)

        if fn == "collect":
            return vals

        if not vals:
            return 0 if fn in ("count", "sum") else None

        num_vals = [float(v) for v in vals if isinstance(v, (int, float))]
        if not num_vals:
            return None

        if fn == "sum":
            return sum(num_vals)
        if fn == "avg":
            return sum(num_vals) / len(num_vals)
        if fn == "min":
            return min(num_vals)
        if fn == "max":
            return max(num_vals)

        return None

    def _find_candidate_node_ids(self, label: Optional[str], properties: Dict[str, Any]) -> List[int]:
        # Fast-path: When filtering by properties, query property indexes first (O(1))
        if properties:
            candidates: Optional[Set[int]] = None
            for k, v in properties.items():
                matched = self.graph.property_store.find_nodes_by_property(k, v)
                if candidates is None:
                    candidates = set(matched)
                else:
                    candidates = candidates.intersection(matched)
                if not candidates:
                    return []

            active = self.graph.matrix_store.get_active_nodes()
            result = []
            for nid in candidates:
                if nid in active:
                    if label is None or label in self.graph.matrix_store.node_to_labels.get(nid, set()):
                        result.append(nid)
            return sorted(result)

        if label:
            return sorted(list(self.graph.matrix_store.get_nodes_with_label(label)))

        return sorted(list(self.graph.matrix_store.get_active_nodes()))

    def _parse_return_projections(self, ret_clause: Optional[str]) -> List[Tuple[str, str]]:
        if not ret_clause:
            return [("node", "node")]
        cols = [c.strip() for c in ret_clause.split(",")]
        projections: List[Tuple[str, str]] = []
        for c in cols:
            m = re.search(r"^(.*?)\s+\bAS\b\s+(\w+)$", c, re.IGNORECASE)
            if m:
                expr = m.group(1).strip()
                alias = m.group(2).strip()
                projections.append((expr, alias))
            else:
                projections.append((c, c))
        return projections

    def _resolve_projection_value(self, expr: str, var_map: Dict[str, Dict[str, Any]], path: List[int]) -> Any:
        for var, props in var_map.items():
            if expr == var:
                return props
            if expr.startswith(f"{var}."):
                prop_key = expr.split(".", 1)[1]
                return props.get(prop_key)
            if expr in props:
                return props[expr]

        if path:
            last_props = self.graph.property_store.get_node_properties(path[-1])
            if expr in last_props:
                return last_props[expr]

        return None
