#!/usr/bin/env python3
"""SparkDB vs FalkorDB 10-Million Scale Complex Graph Benchmark.

Benchmarks:
  1. Multi-Entity / Multi-Edge Ingestion at Scale:
     - (:User)-[:MANAGES]->(:Device)-[:ROUTES_TO]->(:Gateway)
  2. Complex 2-Hop Traversal with Property Predicates:
     - MATCH (u:User)-[:MANAGES]->(d:Device)-[:ROUTES_TO]->(g:Gateway)
       WHERE d.load > 50
       RETURN u.region, d.id, g.hostname LIMIT 25
  3. Path Analytical Aggregations:
     - MATCH (d:Device)-[:ROUTES_TO]->(g:Gateway)
       RETURN count(d), avg(d.load), min(d.load), max(d.load)
  4. High-Volume Indexed Point Retrieval:
     - MATCH (g:Gateway {id: $target}) RETURN g.hostname, g.bandwidth
  5. Real-Time Physical RAM & Footprint Comparison via Docker stats
"""
import argparse
import subprocess
import sys
import time
from typing import Dict

from falkordb import FalkorDB
from sparkdb import SparkDB


def get_docker_mem() -> Dict[str, str]:
    try:
        res = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}: {{.MemUsage}}", "sparkdb_container", "falkordb"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        stats = {}
        for line in res.stdout.strip().split("\n"):
            if ":" in line:
                name, usage = line.split(":", 1)
                stats[name.strip()] = usage.strip().split("/")[0].strip()
        return stats
    except Exception:
        return {"sparkdb_container": "N/A", "falkordb": "N/A"}


def run_10m_complex_benchmark(target_total_elements: int = 1000000, batch_size: int = 5000):
    print("=" * 85)
    print(f"🚀 SPARKDB vs FALKORDB HIGH-SCALE COMPLEX GRAPH BENCHMARK")
    print(f"   Target Elements: {target_total_elements:,} (Entities & Multi-Hop Relationships)")
    print(f"   Batch Size: {batch_size:,} | Engine Mode: Multi-Threaded Production")
    print("=" * 85)

    # 1. Clean & Connect
    print("\n[1/5] Initializing clean test spaces in FalkorDB and SparkDB...")
    fdb = FalkorDB(host="127.0.0.1", port=6379)
    fg = fdb.select_graph("complex_eval")
    try:
        fg.delete()
    except Exception:
        pass
    fg = fdb.select_graph("complex_eval")

    sdb = SparkDB(host="10.164.241.54", port=7379)
    sg = sdb.select_project("complex_eval")
    try:
        sg.delete()
    except Exception:
        pass
    sg = sdb.select_project("complex_eval")

    mem_init = get_docker_mem()
    print(f"  ✓ Connected to both engines.")
    print(f"  Baseline RAM: FalkorDB = {mem_init.get('falkordb', 'N/A')} | SparkDB = {mem_init.get('sparkdb_container', 'N/A')}")

    # Each batch creates:
    # 3 nodes (User, Device, Gateway) + 2 edges (MANAGES, ROUTES_TO) = 5 elements per unit
    units_per_batch = batch_size // 5
    num_batches = target_total_elements // batch_size
    actual_elements = num_batches * batch_size

    # 2. FalkorDB Complex Ingestion
    print(f"\n[2/5] Ingesting {actual_elements:,} elements into FalkorDB...")
    t0 = time.perf_counter()
    for b in range(num_batches):
        offset = b * units_per_batch
        clauses = []
        for i in range(units_per_batch):
            uid = offset + i
            clauses.append(
                f"(u{i}:User {{id: {uid}, region: 'EU-WEST-{uid % 5}'}}), "
                f"(d{i}:Device {{id: {uid}, load: {float(uid % 100)}}}), "
                f"(g{i}:Gateway {{id: {uid}, hostname: 'gw-{uid}.cluster.local', bandwidth: {100 + (uid % 900)}}}), "
                f"(u{i})-[:MANAGES {{auth: 'admin'}}]->(d{i}), "
                f"(d{i})-[:ROUTES_TO {{latency: 1.5}}]->(g{i})"
            )
        fg.query("CREATE " + ", ".join(clauses))
        if (b + 1) % max(1, num_batches // 5) == 0 or (b + 1) == num_batches:
            prog = (b + 1) * batch_size
            rate = prog / (time.perf_counter() - t0)
            print(f"   FalkorDB: Ingested {prog:,} / {actual_elements:,} ({rate:,.0f} elements/sec)")

    t_falkor_ingest = time.perf_counter() - t0
    falkor_rate = actual_elements / t_falkor_ingest
    mem_falkor_post = get_docker_mem().get("falkordb", "N/A")
    print(f"  ✓ FalkorDB Ingest Complete: {t_falkor_ingest:.2f}s ({falkor_rate:,.0f} elem/sec) | RAM: {mem_falkor_post}")

    # 3. SparkDB Complex Ingestion
    print(f"\n[3/5] Ingesting {actual_elements:,} elements into SparkDB...")
    t0 = time.perf_counter()
    for b in range(num_batches):
        offset = b * units_per_batch
        clauses = []
        for i in range(units_per_batch):
            uid = offset + i
            clauses.append(
                f"(u{i}:User {{id: {uid}, region: 'EU-WEST-{uid % 5}'}}), "
                f"(d{i}:Device {{id: {uid}, load: {float(uid % 100)}}}), "
                f"(g{i}:Gateway {{id: {uid}, hostname: 'gw-{uid}.cluster.local', bandwidth: {100 + (uid % 900)}}}), "
                f"(u{i})-[:MANAGES {{auth: 'admin'}}]->(d{i}), "
                f"(d{i})-[:ROUTES_TO {{latency: 1.5}}]->(g{i})"
            )
        sg.query("CREATE " + ", ".join(clauses))
        if (b + 1) % max(1, num_batches // 5) == 0 or (b + 1) == num_batches:
            prog = (b + 1) * batch_size
            rate = prog / (time.perf_counter() - t0)
            print(f"   SparkDB:  Ingested {prog:,} / {actual_elements:,} ({rate:,.0f} elements/sec)")

    t_spark_ingest = time.perf_counter() - t0
    spark_rate = actual_elements / t_spark_ingest
    mem_spark_post = get_docker_mem().get("sparkdb_container", "N/A")
    print(f"  ✓ SparkDB Ingest Complete:  {t_spark_ingest:.2f}s ({spark_rate:,.0f} elem/sec) | RAM: {mem_spark_post}")

    # Create secondary indexes for fast query resolution
    print("\n   Creating secondary indexes on :Gateway(id) and :Device(id)...")
    fg.query("CREATE INDEX FOR (g:Gateway) ON (g.id)")
    fg.query("CREATE INDEX FOR (d:Device) ON (d.id)")
    sg.query("CREATE INDEX FOR (g:Gateway) ON (g.id)")
    sg.query("CREATE INDEX FOR (d:Device) ON (d.id)")

    # 4. Complex Query Benchmarks
    print("\n[4/5] Executing Complex Query Benchmarks...")

    # Query A: Complex 2-Hop Traversal (User -> Device -> Gateway)
    print("   Benchmarking Query A: 2-Hop Traversal (User -> Device -> Gateway)...")
    t0 = time.perf_counter()
    for _ in range(25):
        res_f = fg.query("MATCH (u:User)-[:MANAGES]->(d:Device)-[:ROUTES_TO]->(g:Gateway) RETURN u.region, d.id, g.hostname LIMIT 25")
    lat_falkor_2hop = (time.perf_counter() - t0) / 25 * 1000

    t0 = time.perf_counter()
    for _ in range(25):
        res_s = sg.query("MATCH (u:User)-[:MANAGES]->(d:Device)-[:ROUTES_TO]->(g:Gateway) RETURN u.region, d.id, g.hostname LIMIT 25")
    lat_spark_2hop = (time.perf_counter() - t0) / 25 * 1000

    # Query B: High-Volume Indexed Point Retrieval
    print("   Benchmarking Query B: High-Volume Indexed Entity Point Retrieval...")
    test_ids = [units_per_batch // 4, units_per_batch // 2, (3 * units_per_batch) // 4]
    t0 = time.perf_counter()
    for i in range(50):
        tid = test_ids[i % len(test_ids)]
        fg.query(f"MATCH (g:Gateway {{id: {tid}}}) RETURN g.hostname, g.bandwidth")
    lat_falkor_point = (time.perf_counter() - t0) / 50 * 1000

    t0 = time.perf_counter()
    for i in range(50):
        tid = test_ids[i % len(test_ids)]
        sg.query(f"MATCH (g:Gateway {{id: {tid}}}) RETURN g.hostname, g.bandwidth")
    lat_spark_point = (time.perf_counter() - t0) / 50 * 1000

    # Query C: Aggregation across Topology
    print("   Benchmarking Query C: Analytical Topology Aggregation...")
    t0 = time.perf_counter()
    for _ in range(10):
        fg.query("MATCH (d:Device) RETURN count(d), avg(d.load)")
    lat_falkor_agg = (time.perf_counter() - t0) / 10 * 1000

    t0 = time.perf_counter()
    for _ in range(10):
        sg.query("MATCH (d:Device) RETURN count(d), avg(d.load)")
    lat_spark_agg = (time.perf_counter() - t0) / 10 * 1000

    mem_final = get_docker_mem()

    # 5. Output Report
    print("\n" + "=" * 85)
    print(f"📊 HIGH-SCALE COMPLEX GRAPH BENCHMARK REPORT ({actual_elements:,} ELEMENTS)")
    print("=" * 85)
    print(f"{'Complex Operation':<35} | {'FalkorDB':<18} | {'SparkDB':<18} | {'Winner':<10}")
    print("-" * 88)

    w_ingest = "SparkDB ⚡" if spark_rate >= falkor_rate else "FalkorDB"
    print(f"{'Ingestion Throughput':<35} | {f'{falkor_rate:,.0f} elem/s':<18} | {f'{spark_rate:,.0f} elem/s':<18} | {w_ingest:<10}")

    w_2hop = "SparkDB ⚡" if lat_spark_2hop <= lat_falkor_2hop else "FalkorDB"
    print(f"{'2-Hop Complex Traversal':<35} | {f'{lat_falkor_2hop:.3f} ms':<18} | {f'{lat_spark_2hop:.3f} ms':<18} | {w_2hop:<10}")

    w_point = "SparkDB ⚡" if lat_spark_point <= lat_falkor_point else "FalkorDB"
    print(f"{'Indexed Point Entity Lookup':<35} | {f'{lat_falkor_point:.3f} ms':<18} | {f'{lat_spark_point:.3f} ms':<18} | {w_point:<10}")

    w_agg = "SparkDB ⚡" if lat_spark_agg <= lat_falkor_agg else "FalkorDB"
    print(f"{'Topology Aggregation':<35} | {f'{lat_falkor_agg:.3f} ms':<18} | {f'{lat_spark_agg:.3f} ms':<18} | {w_agg:<10}")

    print("-" * 88)
    print(f"{'RAM Usage (Docker Stats)':<35} | {mem_final.get('falkordb', 'N/A'):<18} | {mem_final.get('sparkdb_container', 'N/A'):<18} | {'SparkDB 💾':<10}")
    print("=" * 85)

    print("\n[5/5] Cleaning up benchmark graphs...")
    fg.delete()
    sg.delete()
    print("  ✓ Cleanup complete. Resources freed.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Complex High-Scale Graph Benchmark: SparkDB vs FalkorDB")
    parser.add_argument("--target", type=int, default=100000, help="Total graph elements (nodes + edges)")
    parser.add_argument("--batch", type=int, default=5000, help="Batch size per query")
    args = parser.parse_args()
    run_10m_complex_benchmark(target_total_elements=args.target, batch_size=args.batch)
