"""Interactive CLI REPL for SparkDB.

Allows querying any graph space directly from the terminal with formatted tables.
"""
from __future__ import annotations

import argparse
import cmd
import sys
import time

from sparkdb.client import SparkDBClient


class SparkDBCLI(cmd.Cmd):
    intro = "Welcome to the SparkDB Interactive Shell. Type :help or ? to list commands. Type 'exit' to quit.\n"
    prompt = "sparkdb> "

    def __init__(self, graph_name: str = "default", storage_dir: str = "./data/sparkdb"):
        super().__init__()
        self.db = SparkDBClient(storage_dir=storage_dir)
        self.graph_name = graph_name
        self.graph = self.db.select_graph(self.graph_name)
        self.prompt = f"sparkdb[{self.graph_name}]> "

    def do_use(self, arg: str):
        """Switch active graph space: USE <graph_name>"""
        gname = arg.strip()
        if gname:
            self.graph_name = gname
            self.graph = self.db.select_graph(self.graph_name)
            self.prompt = f"sparkdb[{self.graph_name}]> "
            print(f"Switched to graph '{self.graph_name}'")

    def do_checkpoint(self, arg: str):
        """Save an atomic snapshot of current graph."""
        path = self.graph.checkpoint()
        print(f"Checkpoint saved to {path}")

    def default(self, line: str):
        """Execute arbitrary Cypher query."""
        q = line.strip()
        if not q:
            return
        if q.lower() in ["exit", "quit", ":q"]:
            return True

        try:
            res = self.graph.query(q)
            if res.header and res.result_set:
                # Print header
                print("\n" + " | ".join(res.header))
                print("-" * (len(" | ".join(res.header)) + 4))
                for row in res.result_set:
                    print(" | ".join(str(item) for item in row))
            elif res.nodes_created or res.relationships_created:
                print(f"\nNodes created: {res.nodes_created}, Relationships created: {res.relationships_created}, Properties set: {res.properties_set}")
            elif res.nodes_deleted:
                print(f"\nNodes deleted: {res.nodes_deleted}")
            else:
                print("\nEmpty result set.")

            print(f"Execution time: {res.execution_time_ms:.3f} ms\n")
        except Exception as e:
            print(f"Error: {e}\n")

    def do_exit(self, arg: str):
        """Exit CLI."""
        return True


def main():
    parser = argparse.ArgumentParser(description="SparkDB Interactive Shell")
    parser.add_argument("--graph", default="default", help="Graph space to select")
    parser.add_argument("--storage", default="./data/sparkdb", help="Storage directory")
    args = parser.parse_args()

    cli = SparkDBCLI(graph_name=args.graph, storage_dir=args.storage)
    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\nExiting SparkDB.")


if __name__ == "__main__":
    main()
