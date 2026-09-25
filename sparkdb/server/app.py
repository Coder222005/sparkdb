"""SparkDB High-Performance Multi-Threaded HTTP / REST Server Application.

Provides a fast, zero-dependency multi-threaded HTTP server (via standard library `http.server.ThreadingHTTPServer`)
to execute Cypher queries, inspect graph spaces, inspect project ontology / schema, trigger snapshots,
and monitor database health.
Supports compact binary MessagePack protocol with transparent JSON fallback.
"""
from __future__ import annotations

import json
import logging
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import os
import sys
import time
from typing import Any, Dict
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

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            projects = self.db.list_graphs()
            host_ip = os.getenv("SPARKDB_HOST", str(self.server.server_address[0]))
            if host_ip in ("0.0.0.0", "::"):
                host_ip = "127.0.0.1"
            self._send_payload(200, {
                "status": "healthy",
                "engine": "SparkDB v1.1.0 (BSD 3-Clause)",
                "host_ip": host_ip,
                "port": getattr(self.server, "server_port", 7379),
                "active_projects": projects,
                "active_graphs": projects,
            })
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
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length)
        content_type = self.headers.get("Content-Type", "")
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
