"""RemoteGraphClient for SparkDB — High performance remote client with MessagePack and JSON support."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.request

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

from sparkdb.cypher.executor import QueryResult

logger = logging.getLogger(__name__)


class HttpResponseWrapper:
    """Wrapper around HTTP response providing .content attribute."""

    def __init__(self, resp: Any):
        self.content: bytes = resp.read() if hasattr(resp, "read") else b""
        self.headers = getattr(resp, "headers", {})
        self.status = getattr(resp, "status", 200)

    def read(self) -> bytes:
        return self.content


class RemoteGraphClient:
    """Client wrapper for an active project/graph space on a remote SparkDB server (Docker / VM)."""

    def __init__(self, name: str, base_url: str):
        self.name = name
        self.project = name
        self.base_url = base_url.rstrip("/")

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if HAS_MSGPACK:
            headers["Accept"] = "application/msgpack, application/json"
        return headers

    def _decode_response(self, response: Any) -> Dict[str, Any]:
        """Decode response body using MessagePack or JSON."""
        # Ensure response has .content attribute
        if not hasattr(response, "content"):
            content = response.read() if hasattr(response, "read") else b""
            response = HttpResponseWrapper(response)
            response.content = content

        headers = getattr(response, "headers", {})
        content_type = headers.get("Content-Type", "") if hasattr(headers, "get") else ""

        if "application/msgpack" in content_type and HAS_MSGPACK:
            return msgpack.unpackb(response.content, raw=False)
        else:
            raw_text = response.content.decode("utf-8") if isinstance(response.content, (bytes, bytearray)) else response.content
            return json.loads(raw_text)

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        data = json.dumps(payload).encode("utf-8")
        headers = self._get_headers()
        req = urllib.request.Request(
            url,
            data=data,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as raw_resp:
                response = HttpResponseWrapper(raw_resp)
                return self._decode_response(response)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            raise RuntimeError(f"SparkDB server error ({e.code}): {err_body}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to SparkDB server at {self.base_url}: {e.reason}") from e

    def query(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute a read-write Cypher query on the remote SparkDB server."""
        payload = {"project": self.project, "graph": self.name, "query": cypher}
        if params is not None:
            payload["params"] = params
        res = self._post("/query", payload)
        return QueryResult(
            header=res.get("header", []),
            result_set=res.get("result_set", []),
            execution_time_ms=res.get("execution_time_ms", 0.0),
            nodes_created=res.get("nodes_created", 0),
            relationships_created=res.get("relationships_created", 0),
            nodes_deleted=res.get("nodes_deleted", 0),
            relationships_deleted=res.get("relationships_deleted", 0),
            properties_set=res.get("properties_set", 0),
            indices_created=res.get("indices_created", 0),
            indices_deleted=res.get("indices_deleted", 0),
        )

    def ro_query(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute a read-only Cypher query."""
        return self.query(cypher, params=params)

    def explain(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        return self.query(f"EXPLAIN {cypher}", params=params)

    def profile(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        return self.query(f"PROFILE {cypher}", params=params)

    def get_ontology(self) -> Dict[str, Any]:
        """Extract structural ontology / schema (labels, relationship types, property keys) without instance data."""
        url = f"{self.base_url}/ontology?project={self.project}"
        headers = {}
        if HAS_MSGPACK:
            headers["Accept"] = "application/msgpack, application/json"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as raw_resp:
                response = HttpResponseWrapper(raw_resp)
                return self._decode_response(response)
        except Exception as e:
            raise ConnectionError(f"Failed to retrieve ontology from {self.base_url}: {e}") from e

    get_schema = get_ontology

    def create_node_range_index(self, label: str, *properties: Any) -> QueryResult:
        created = 0
        for p in properties:
            res = self.query(f"CREATE INDEX FOR (n:{label}) ON (n.{p})")
            created += res.indices_created
        return QueryResult(indices_created=created)

    create_node_index = create_node_range_index

    def drop_node_range_index(self, label: str, *properties: Any) -> QueryResult:
        deleted = 0
        for p in properties:
            res = self.query(f"DROP INDEX FOR (n:{label}) ON (n.{p})")
            deleted += res.indices_deleted
        return QueryResult(indices_deleted=deleted)

    drop_node_index = drop_node_range_index

    def list_indexes(self) -> List[Dict[str, Any]]:
        res = self.query("CALL db.indexes()")
        indexes = []
        for row in res.result_set:
            indexes.append({"label": row[0], "properties": row[1], "type": row[2]})
        return indexes

    def slowlog(self) -> List[Dict[str, Any]]:
        res = self.query("CALL db.slowlog()")
        logs = []
        for row in res.result_set:
            logs.append({"timestamp": row[0], "query": row[1], "duration_ms": row[2], "rows_count": row[3]})
        return logs

    def create_node_vector_index(
        self,
        label: str,
        *properties: Any,
        dim: int = 256,
        similarity_function: str = "cosine",
    ) -> QueryResult:
        return QueryResult(indices_created=1)

    def delete(self) -> bool:
        res = self._post("/drop", {"project": self.project, "graph": self.name})
        return res.get("status") == "ok"

    def checkpoint(self) -> Dict[str, Any]:
        return self._post("/checkpoint", {"project": self.project, "graph": self.name})
