"""SparkDB High-Performance Multi-Threaded HTTP / REST Server.

Provides a fast, zero-dependency multi-threaded HTTP server.
Re-exports SparkDBRequestHandler and run_server from sparkdb.server.app.
"""
from __future__ import annotations

import sys
import os

from sparkdb.server.app import SparkDBRequestHandler, run_server, HAS_MSGPACK

__all__ = ["SparkDBRequestHandler", "run_server", "HAS_MSGPACK"]

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 7379
    storage_dir = sys.argv[3] if len(sys.argv) > 3 else os.getenv("SPARKDB_STORAGE", "/data/sparkdb")
    run_server(host, port, storage_dir=storage_dir)
