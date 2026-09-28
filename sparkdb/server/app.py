"""SparkDB High-Performance Multi-Threaded HTTP / REST Server Application.

Provides a fast, zero-dependency multi-threaded HTTP server (via standard library `http.server.ThreadingHTTPServer`)
to execute Cypher queries, inspect graph spaces, inspect project ontology / schema, trigger snapshots,
and monitor database health.
Supports compact binary MessagePack protocol with transparent JSON fallback.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import logging
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse, parse_qs

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

from sparkdb.core.engine import SparkDB

logger = logging.getLogger(__name__)


class SparkDBRequestHandler(BaseHTTPRequestHandler):
    db: SparkDB = None  # Injected by server

    # PERFORMANCE CRITICAL: Disable DNS reverse lookup which blocks for 10-30s on VPN / LAN IPs!
    def address_string(self) -> str:
        return self.client_address[0]

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write(f"[{self.log_date_time_string()}] {self.client_address[0]} {format % args}\n")

    def _send_payload(self, status: int, payload: Dict[str, Any]) -> None:
        """Send response formatted with MessagePack or JSON depending on Accept header and format query param."""
        accept_header = self.headers.get("Accept", "")
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        format_param = qs.get("format", [""])[0].lower()

        use_msgpack = HAS_MSGPACK and ("application/msgpack" in accept_header or format_param == "msgpack")
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

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        """Backwards-compatible alias for sending response with format negotiation."""
        self._send_payload(status, payload)

    def _authorize(self, admin: bool = False) -> bool:
        """Authorize requests using configured admin and optional read-only bearer tokens."""
        admin_token = os.getenv("SPARKDB_AUTH_TOKEN")
        readonly_token = os.getenv("SPARKDB_READONLY_TOKEN")
        if not admin_token and not readonly_token:
            self._send_payload(503, {"error": "Server authentication is not configured"})
            return False

        header = self.headers.get("Authorization", "")
        supplied = header[7:].strip() if header.lower().startswith("bearer ") else ""
        if admin_token and supplied == admin_token:
            return True
        if not admin and readonly_token and supplied == readonly_token:
            return True
        self._send_payload(403 if supplied else 401, {"error": "Invalid or missing bearer token"})
        return False

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            projects = self.db.list_graphs()
            host_ip = os.getenv("SPARKDB_HOST", str(self.server.server_address[0]))
            if host_ip in ("0.0.0.0", "::"):
                host_ip = "127.0.0.1"
            self._send_payload(200, {
                "status": "healthy",
                "engine": "SparkDB v1.1.1 (BSD 3-Clause)",
                "host_ip": host_ip,
                "port": getattr(self.server, "server_port", 7379),
                "active_projects": projects,
                "active_graphs": projects,
            })
            return

        if not self._authorize(admin=False):
            return

        if parsed.path in ("/graphs", "/projects"):
            projects = self.db.list_graphs()
            self._send_payload(200, {"projects": projects, "graphs": projects})
            return

        if parsed.path in ("/ontology", "/schema"):
            qs = parse_qs(parsed.query)
            project_name = qs.get("project", qs.get("graph", ["default"]))[0]
            space = self.db.select_graph(project_name)
            self._send_payload(200, space.get_ontology())
            return

        self._send_payload(404, {"error": "Endpoint not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in ("/query", "/query_batch", "/checkpoint", "/drop", "/drop_all"):
            self._send_payload(404, {"error": "Endpoint not found"})
            return

        raw_auth = self.headers.get("Authorization", "")
        supplied = raw_auth[7:].strip() if raw_auth.lower().startswith("bearer ") else ""
        query_is_readonly = parsed.path in ("/query", "/query_batch")
        if query_is_readonly:
            # Read-only tokens may execute only non-mutating queries.
            body_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(body_length)
            content_type = self.headers.get("Content-Type", "")
            try:
                if "application/msgpack" in content_type and HAS_MSGPACK:
                    auth_payload = msgpack.unpackb(raw_body, raw=False) if raw_body else {}
                else:
                    auth_payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
            except Exception as e:
                self._send_payload(400, {"error": f"Invalid payload: {e}"})
                return
            query_text = auth_payload.get("query", "") if parsed.path == "/query" else json.dumps(auth_payload.get("queries", []))
            mutation = re.search(r"\b(CREATE|DELETE|SET|REMOVE|MERGE|DROP)\b", query_text, re.IGNORECASE)
            if mutation and os.getenv("SPARKDB_AUTH_TOKEN") != supplied:
                self._send_payload(403, {"error": "An admin bearer token is required for mutations"})
                return
            # Reuse the decoded request below without reading the body twice.
            payload = auth_payload
        else:
            if not self._authorize(admin=True):
                return
            payload = None

        if query_is_readonly:
            if not self._authorize(admin=False):
                return
        # For admin requests, authorization was checked above. Read the body now.
        length = int(self.headers.get("Content-Length", 0))
        raw_body = b"" if query_is_readonly else self.rfile.read(length)
        content_type = self.headers.get("Content-Type", "")
        if not query_is_readonly:
            try:
                if "application/msgpack" in content_type and HAS_MSGPACK:
                    payload = msgpack.unpackb(raw_body, raw=False) if raw_body else {}
                else:
                    raw_text = raw_body.decode("utf-8") if isinstance(raw_body, bytes) else raw_body
                    payload = json.loads(raw_text) if raw_text else {}
            except Exception as e:
                self._send_payload(400, {"error": f"Invalid payload: {e}"})
                return

        if parsed.path == "/query":
            graph_name = payload.get("project") or payload.get("graph", "default")
            query = payload.get("query")
            if not query:
                self._send_payload(400, {"error": "Missing 'query' field"})
                return

            try:
                g = self.db.select_graph(graph_name)
                params = payload.get("params")
                from sparkdb.cypher.executor import CypherExecutor
                executor = CypherExecutor(g)
                res = executor.execute(query, params=params)
                self._send_payload(200, {
                    "header": res.header,
                    "result_set": res.result_set,
                    "execution_time_ms": res.execution_time_ms,
                    "nodes_created": res.nodes_created,
                    "relationships_created": res.relationships_created,
                    "nodes_deleted": res.nodes_deleted,
                    "relationships_deleted": res.relationships_deleted,
                    "properties_set": res.properties_set,
                    "indices_created": res.indices_created,
                    "indices_deleted": res.indices_deleted,
                })
            except Exception as e:
                self._send_payload(500, {"error": str(e)})
            return

        if parsed.path == "/query_batch":
            default_graph = payload.get("project") or payload.get("graph", "default")
            queries = payload.get("queries", [])
            if not isinstance(queries, list) or not queries:
                self._send_payload(400, {"error": "Missing or empty 'queries' list in payload"})
                return

            parallel_requested = payload.get("parallel", True)
            try:
                max_workers = max(1, min(int(payload.get("max_workers", 8)), 32))
            except (ValueError, TypeError):
                max_workers = 8

            t_batch_start = time.perf_counter()
            try:
                # CRITICAL EDGE CASE: Warm up / load all referenced project(s) ONCE on the parent thread.
                # If a project is cold on disk, this pre-loads snapshot.json, replays AOF, and opens SQLite
                # before pool workers launch. Worker threads then read the warm in-memory GraphSpace directly
                # with zero cold-start delay or concurrent initialization race conditions.
                referenced_projects: Set[str] = set()
                for q in queries:
                    if isinstance(q, dict):
                        p_name = q.get("project") or q.get("graph") or default_graph
                        referenced_projects.add(str(p_name))
                    else:
                        referenced_projects.add(default_graph)

                graph_map: Dict[str, Any] = {
                    p_name: self.db.select_graph(p_name)
                    for p_name in referenced_projects
                }

                from sparkdb.cypher.executor import CypherExecutor

                def _run_single_query(q_item: Any) -> Dict[str, Any]:
                    if isinstance(q_item, str):
                        q_str = q_item
                        q_params = None
                        target_g = graph_map[default_graph]
                    elif isinstance(q_item, dict):
                        q_str = q_item.get("query", "")
                        q_params = q_item.get("params")
                        p_name = q_item.get("project") or q_item.get("graph") or default_graph
                        target_g = graph_map.get(str(p_name), graph_map[default_graph])
                    else:
                        return {"success": False, "error": "Query item must be a string or object with 'query'"}

                    if not q_str:
                        return {"success": False, "error": "Empty query string"}

                    try:
                        executor = CypherExecutor(target_g)
                        res = executor.execute(q_str, params=q_params)
                        return {
                            "success": True,
                            "header": res.header,
                            "result_set": res.result_set,
                            "execution_time_ms": res.execution_time_ms,
                            "nodes_created": res.nodes_created,
                            "relationships_created": res.relationships_created,
                            "nodes_deleted": res.nodes_deleted,
                            "relationships_deleted": res.relationships_deleted,
                            "properties_set": res.properties_set,
                            "indices_created": res.indices_created,
                            "indices_deleted": res.indices_deleted,
                        }
                    except Exception as ex:
                        return {"success": False, "error": str(ex)}

                # Mutation detection:
                # If any query contains mutations (CREATE, SET, DELETE, MERGE, DROP),
                # running in parallel could cause race conditions on topology matrices.
                # In that case, we safely execute them sequentially to preserve consistency.
                mutation_keywords = re.compile(r"\b(CREATE|DELETE|SET|REMOVE|MERGE|DROP)\b", re.IGNORECASE)
                has_mutations = any(
                    mutation_keywords.search(q if isinstance(q, str) else q.get("query", ""))
                    for q in queries
                )

                should_parallel = parallel_requested and (not has_mutations) and (len(queries) > 1)

                if should_parallel:
                    workers = min(max_workers, len(queries))
                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        results = list(pool.map(_run_single_query, queries))
                else:
                    results = [_run_single_query(q) for q in queries]

                total_time_ms = (time.perf_counter() - t_batch_start) * 1000.0
                self._send_payload(200, {
                    "total_queries": len(queries),
                    "parallel": should_parallel,
                    "max_workers": max_workers if should_parallel else 1,
                    "total_execution_time_ms": round(total_time_ms, 3),
                    "results": results,
                })
            except Exception as e:
                self._send_payload(500, {"error": str(e)})
            return

        if parsed.path == "/checkpoint":
            results = self.db.checkpoint_all()
            self._send_payload(200, {"status": "ok", "snapshots": results})
            return

        if parsed.path == "/drop":
            graph_name = payload.get("project") or payload.get("graph")
            if not graph_name:
                self._send_payload(400, {"error": "Missing 'project' or 'graph' field"})
                return
            success = self.db.drop_graph(graph_name)
            self._send_payload(200, {"status": "ok" if success else "not_found", "project": graph_name})
            return

        if parsed.path == "/drop_all":
            dropped = self.db.drop_all_projects()
            self._send_payload(200, {"status": "ok", "dropped": dropped})
            return

        self._send_payload(404, {"error": "Endpoint not found"})


def run_server(host: str = "0.0.0.0", port: int = 7379, storage_dir: str = "./data/sparkdb") -> None:
    db = SparkDB(storage_dir=storage_dir)
    SparkDBRequestHandler.db = db

    server = ThreadingHTTPServer((host, port), SparkDBRequestHandler)
    print(f"SparkDB multi-threaded server running on http://{host}:{port} (Storage: {storage_dir}, MessagePack: {HAS_MSGPACK})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down SparkDB server...")
        server.server_close()


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 7379
    storage_dir = sys.argv[3] if len(sys.argv) > 3 else os.getenv("SPARKDB_STORAGE", "/data/sparkdb")
    run_server(host, port, storage_dir=storage_dir)
