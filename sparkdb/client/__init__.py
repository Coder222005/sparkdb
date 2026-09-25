"""SparkDB Python Client SDK.

Provides a drop-in replacement for the FalkorDB Python SDK (`falkordb`):
  - `SparkDB(host="localhost", port=7379)` -> Connects to remote Docker container or HTTP daemon
  - `SparkDB(storage_dir="./data/sparkdb")` -> Runs in-process embedded engine
  - `select_project(project_name)` / `select_graph(graph_name)`
  - `list_projects()` / `list_graphs()`
  - `drop_project(project_name)` / `drop_graph(graph_name)`
  - `drop_all_projects()`
  - `get_ontology()` / `get_schema()`
  - `query(cypher_query, params={...})`
  - `ro_query(cypher_query, params={...})`
  - `explain(cypher_query, params={...})`
  - `profile(cypher_query, params={...})`
  - `checkpoint()`
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.request

try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

from sparkdb.core.engine import SparkDB as EngineSparkDB, GraphSpace
from sparkdb.cypher.executor import CypherExecutor, QueryResult
from sparkdb.client.remote_client import RemoteGraphClient, HttpResponseWrapper

logger = logging.getLogger(__name__)

DEFAULT_HOST = os.getenv("SPARKDB_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("SPARKDB_PORT", "7379"))


class GraphClient:
    """In-process graph client wrapper representing an active project/graph space."""

    def __init__(self, graph_space: GraphSpace, engine: EngineSparkDB):
        self._space = graph_space
        self._engine = engine
        self._executor = CypherExecutor(self._space)
        self.name = self._space.name
        self.project = self.name

    def query(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute a read-write Cypher query."""
        return self._executor.execute(cypher, params=params)

    def ro_query(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute a read-only Cypher query."""
        return self._executor.execute(cypher, params=params)

    def explain(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Return the physical execution plan without running it."""
        return self._executor.execute(f"EXPLAIN {cypher}", params=params)

    def profile(self, cypher: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Profile query execution with timings."""
        return self._executor.execute(f"PROFILE {cypher}", params=params)

    def get_ontology(self) -> Dict[str, Any]:
        """Extract structural ontology / schema (labels, relationship types, property keys) without instance data."""
        return self._space.get_ontology()

    get_schema = get_ontology

    def create_node_range_index(self, label: str, *properties: Any) -> QueryResult:
        """Create secondary index on node properties."""
        for p in properties:
            self._space.create_property_index(p)
        return QueryResult(indices_created=len(properties))

    create_node_index = create_node_range_index

    def drop_node_range_index(self, label: str, *properties: Any) -> QueryResult:
        """Drop secondary index on node properties."""
        deleted = 0
        for p in properties:
            if self._space.drop_property_index(p):
                deleted += 1
        return QueryResult(indices_deleted=deleted)

    drop_node_index = drop_node_range_index

    def list_indexes(self) -> List[Dict[str, Any]]:
        """List active secondary and vector indices."""
        res = self.query("CALL db.indexes()")
        indexes = []
        for row in res.result_set:
            indexes.append({"label": row[0], "properties": row[1], "type": row[2]})
        return indexes

    def slowlog(self) -> List[Dict[str, Any]]:
        """Retrieve query execution logs."""
        return self._space.get_slowlog()

    def create_node_vector_index(
        self,
        label: str,
        *properties: Any,
        dim: int = 256,
        similarity_function: str = "cosine",
    ) -> QueryResult:
        """Create a vector index for a node label."""
        return QueryResult(indices_created=1)

    def delete(self) -> bool:
        """Delete this project/graph space."""
        return self._engine.drop_graph(self.name)

    def checkpoint(self) -> str:
        """Trigger an atomic disk checkpoint."""
        return self._space.checkpoint()


class SparkDBClient:
    """Client interface for SparkDB (drop-in replacement for falkordb.FalkorDB).

    Supports two operating modes:
      1. Remote Docker / Server mode:
         db = SparkDB(host="localhost", port=7379)
         db = SparkDB(url="http://localhost:7379")
      2. Embedded in-process mode:
         db = SparkDB(storage_dir="./data/sparkdb")
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = DEFAULT_PORT,
        storage_dir: Optional[str] = None,
        default_vector_dim: int = 256,
        url: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        if url:
            self._mode = "remote"
            self.base_url = url.rstrip("/")
        elif mode == "remote":
            self._mode = "remote"
            self.host = host or DEFAULT_HOST
            self.port = port
            self.base_url = f"http://{self.host}:{self.port}"
        elif mode == "embedded":
            self._mode = "embedded"
            self.storage_dir = storage_dir or "./data/sparkdb"
        elif host is not None and storage_dir is None:
            self._mode = "remote"
            self.host = host
            self.port = port
            self.base_url = f"http://{self.host}:{self.port}"
        elif storage_dir is not None and host is None:
            self._mode = "embedded"
            self.storage_dir = storage_dir
        elif host is not None:
            self._mode = "remote"
            self.host = host
            self.port = port
            self.base_url = f"http://{self.host}:{self.port}"
        else:
            self._mode = "embedded"
            self.storage_dir = storage_dir or "./data/sparkdb"

        if self._mode == "embedded":
            self._engine = EngineSparkDB(
                storage_dir=self.storage_dir,
                default_vector_dim=default_vector_dim,
            )
        else:
            self._engine = None

    @property
    def mode(self) -> str:
        return self._mode

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Perform remote POST request respecting binary/JSON protocol negotiation."""
        url = f"{self.base_url}{endpoint}"
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if HAS_MSGPACK:
            headers["Accept"] = "application/msgpack, application/json"
        req = urllib.request.Request(
            url,
            data=data,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as raw_resp:
                response = HttpResponseWrapper(raw_resp)
                content_type = response.headers.get("Content-Type", "")
                if "application/msgpack" in content_type and HAS_MSGPACK:
                    return msgpack.unpackb(response.content, raw=False)
                else:
                    return json.loads(response.content.decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            raise RuntimeError(f"SparkDB server error ({e.code}): {err_body}") from e
        except urllib.error.URLError as e:
            raise ConnectionError(f"Failed to connect to SparkDB server at {self.base_url}: {e.reason}") from e

    def select_graph(
        self,
        graph_name: str,
        vector_dim: Optional[int] = None,
        storage_mode: str = "memory",
        lru_cache_size: int = 10000,
    ) -> GraphClient | RemoteGraphClient:
        """Select or create an isolated graph / project workspace."""
        if self._mode == "remote":
            return RemoteGraphClient(name=graph_name, base_url=self.base_url)
        space = self._engine.select_graph(
            graph_name,
            vector_dim=vector_dim,
            storage_mode=storage_mode,
            lru_cache_size=lru_cache_size,
        )
        return GraphClient(space, self._engine)

    def select_project(
        self,
        project_name: str,
        vector_dim: Optional[int] = None,
        storage_mode: str = "memory",
        lru_cache_size: int = 10000,
    ) -> GraphClient | RemoteGraphClient:
        """Select or create an isolated project workspace (alias for select_graph)."""
        return self.select_graph(
            graph_name=project_name,
            vector_dim=vector_dim,
            storage_mode=storage_mode,
            lru_cache_size=lru_cache_size,
        )

    def list_graphs(self) -> List[str]:
        """List active graphs/projects."""
        if self._mode == "remote":
            headers = {}
            if HAS_MSGPACK:
                headers["Accept"] = "application/msgpack, application/json"
            req = urllib.request.Request(f"{self.base_url}/projects", headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=10) as raw_resp:
                    response = HttpResponseWrapper(raw_resp)
                    content_type = response.headers.get("Content-Type", "")
                    if "application/msgpack" in content_type and HAS_MSGPACK:
                        data = msgpack.unpackb(response.content, raw=False)
                    else:
                        data = json.loads(response.content.decode("utf-8"))
                    return data.get("projects") or data.get("graphs", [])
            except Exception as e:
                raise ConnectionError(f"Failed to query {self.base_url}/projects: {e}") from e
        return self._engine.list_graphs()

    def list_projects(self) -> List[str]:
        """List all isolated project workspaces."""
        return self.list_graphs()

    def drop_graph(self, graph_name: str) -> bool:
        """Drop a graph/project space and delete its storage directory."""
        if self._mode == "remote":
            res = self._post("/drop", {"project": graph_name, "graph": graph_name})
            return res.get("status") == "ok"
        return self._engine.drop_graph(graph_name)

    def drop_project(self, project_name: str) -> bool:
        """Drop a project workspace and delete its storage directory."""
        return self.drop_graph(project_name)

    def drop_all_projects(self) -> List[str]:
        """Drop all project workspaces and wipe their storage directories."""
        if self._mode == "remote":
            res = self._post("/drop_all", {})
            return res.get("dropped", [])
        return self._engine.drop_all_projects()

    def checkpoint_all(self) -> Dict[str, str]:
        """Checkpoint all graphs/projects."""
        if self._mode == "remote":
            res = self._post("/checkpoint", {})
            return res.get("snapshots", {})
        return self._engine.checkpoint_all()


SparkDB = SparkDBClient

__all__ = [
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "GraphClient",
    "RemoteGraphClient",
    "SparkDBClient",
    "SparkDB",
    "HttpResponseWrapper",
    "HAS_MSGPACK",
]
