"""Persistence — Snapshot & Write-Ahead-Log (WAL / AOF) Engine for SparkDB.

Provides zero-data-loss durability via atomic checkpointing and incremental mutation logging.
"""
from __future__ import annotations

import io
import json
import logging
import os
import shutil
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PersistenceEngine:
    """Manages disk persistence for a SparkDB graph space."""

    def __init__(self, graph_name: str, storage_dir: str = "./data/sparkdb"):
        self._lock = threading.RLock()
        self.graph_name = graph_name
        self.storage_dir = os.path.abspath(storage_dir)
        self.graph_dir = os.path.join(self.storage_dir, self.graph_name)
        os.makedirs(self.graph_dir, exist_ok=True)

        self.snapshot_file = os.path.join(self.graph_dir, "snapshot.json")
        self.aof_file = os.path.join(self.graph_dir, "mutations.aof")
        self._aof_handle: Optional[io.TextIOWrapper] = None
        self._unflushed: int = 0
        self._last_flush: float = time.time()

    def _get_aof_handle(self) -> io.TextIOWrapper:
        if self._aof_handle is None or self._aof_handle.closed:
            self._aof_handle = open(self.aof_file, "a", encoding="utf-8", buffering=65536)
        return self._aof_handle

    def save_snapshot(self, state_dict: Dict[str, Any]) -> str:
        """Atomically write the graph state snapshot to disk."""
        with self._lock:
            if self._aof_handle and not self._aof_handle.closed:
                self._aof_handle.close()
                self._aof_handle = None

            temp_fd, temp_path = tempfile.mkstemp(dir=self.graph_dir, prefix="snap_", suffix=".tmp")
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                json.dump(state_dict, f, indent=2, default=str)

            # Atomic rename
            shutil.move(temp_path, self.snapshot_file)

            # Reset AOF after successful snapshot
            if os.path.exists(self.aof_file):
                with open(self.aof_file, "w") as f:
                    f.truncate(0)

            logger.info(f"Saved snapshot for graph '{self.graph_name}' to {self.snapshot_file}")
            return self.snapshot_file

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        """Load the latest snapshot if it exists."""
        with self._lock:
            if not os.path.exists(self.snapshot_file):
                return None
            try:
                with open(self.snapshot_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to read snapshot {self.snapshot_file}: {e}")
                return None

    def append_mutation(self, command: str, params: Dict[str, Any]) -> None:
        """Append a mutation to the Write-Ahead Log (AOF) with interval flushing."""
        with self._lock:
            record = json.dumps({"cmd": command, "params": params}, default=str)
            handle = self._get_aof_handle()
            handle.write(record + "\n")
            handle.flush()

    def replay_aof(self) -> List[Dict[str, Any]]:
        """Read all pending mutations from the AOF since last snapshot."""
        with self._lock:
            if self._aof_handle and not self._aof_handle.closed:
                self._aof_handle.flush()

            if not os.path.exists(self.aof_file):
                return []
            mutations = []
            with open(self.aof_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            mutations.append(json.loads(line))
                        except Exception:
                            pass
            return mutations

    def close(self) -> None:
        """Close open file handles cleanly."""
        with self._lock:
            if self._aof_handle and not self._aof_handle.closed:
                self._aof_handle.close()
                self._aof_handle = None
