"""SparkDB — High-Performance, Clean-Room Graph Database Engine.

Permissively licensed (BSD 3-Clause / Apache 2.0).
Zero SSPL copyleft risk.
"""
from sparkdb.client import SparkDBClient as SparkDB, GraphClient
from sparkdb.core.engine import GraphSpace
from sparkdb.cypher.executor import QueryResult

__version__ = "1.1.0"

__all__ = [
    "SparkDB",
    "GraphClient",
    "GraphSpace",
    "QueryResult",
    "__version__",
]
