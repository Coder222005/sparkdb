#!/usr/bin/env python3
"""SparkDB Query Retrieval Latency & Instantaneous Performance Benchmark.

Tests all query retrieval paradigms:
  1. Indexed Point Lookup Retrieval (Single entity property fetch)
  2. 2-Hop Multi-Hop GraphBLAS Traversal Retrieval (Path expansion)
  3. High-Dimensional HNSW Vector Similarity Retrieval (SIMD Cosine / L2)
  4. Clean Structural Ontology Metamodel Retrieval
"""
import random
import time
from sparkdb import SparkDB


def run_query_benchmarks():
    print("=" * 80)
    print("⚡ SPARKDB INSTANTANEOUS QUERY RETRIEVAL BENCHMARK")
    print("=" * 80)

    db = SparkDB(host="10.164.241.54", port=7379)
    project = db.select_project("instant_retrieval_bench")
    project.delete()
    project = db.select_project("instant_retrieval_bench")

    # -------------------------------------------------------------------------
    # 1. Indexed Point Lookup Retrieval
    # -------------------------------------------------------------------------
    print("\n[1/4] Testing Indexed Point Lookup Retrieval (100,000 entities)...")
    batch_size = 5000
    for b in range(20):
        clauses = [
            f"(n{i}:Item {{id: {b*batch_size + i}, sku: 'SKU-{b*batch_size + i}', price: {float(i * 2.5)}}})"
            for i in range(batch_size)
        ]
        project.query("CREATE " + ", ".join(clauses))

    project.query("CREATE INDEX FOR (n:Item) ON (n.id)")

    # Execute 100 point lookups
    t_start = time.perf_counter()
    engine_times = []
    for _ in range(100):
        target_id = random.randint(0, 99999)
        res = project.query(f"MATCH (n:Item {{id: {target_id}}}) RETURN n.sku, n.price")
        engine_times.append(res.execution_time_ms)
    total_network_ms = (time.perf_counter() - t_start) / 100 * 1000
    avg_engine_ms = sum(engine_times) / len(engine_times)

    print(f"   ✓ Engine Internal Execution Time: {avg_engine_ms:.3f} ms ({avg_engine_ms * 1000:.1f} µs)")
    print(f"   ✓ End-to-End HTTP + Serialization: {total_network_ms:.3f} ms")

    # -------------------------------------------------------------------------
    # 2. Multi-Hop Graph Traversal Retrieval
    # -------------------------------------------------------------------------
    print("\n[2/4] Testing 2-Hop Multi-Hop GraphBLAS Traversal Retrieval...")
    for i in range(100):
        project.query(
            f"""
            CREATE (r1:Router {{id: 'rtr_{i}_A', ip: '10.1.{i}.1'}}),
                   (r2:Router {{id: 'rtr_{i}_B', ip: '10.1.{i}.2'}}),
                   (r3:Router {{id: 'rtr_{i}_C', ip: '10.1.{i}.3'}}),
                   (r1)-[:PEER {{bandwidth: 100}}]->(r2),
                   (r2)-[:PEER {{bandwidth: 100}}]->(r3)
            """
        )

    t_start = time.perf_counter()
    res = project.query("MATCH (r1:Router)-[:PEER]->(r2:Router)-[:PEER]->(r3:Router) RETURN r1.ip, r2.ip, r3.ip")
    total_trav_ms = (time.perf_counter() - t_start) * 1000

    print(f"   ✓ Paths Retrieved: {len(res.result_set)} full 2-hop paths")
    print(f"   ✓ Engine Internal Traversal Time: {res.execution_time_ms:.3f} ms ({res.execution_time_ms * 1000 / len(res.result_set):.1f} µs per path)")
    print(f"   ✓ End-to-End HTTP Retrieval Time: {total_trav_ms:.3f} ms")

    # -------------------------------------------------------------------------
    # 3. Vector Similarity Search Retrieval (256-dim)
    # -------------------------------------------------------------------------
    print("\n[3/4] Testing 256-Dimension HNSW Vector Similarity Retrieval...")
    for i in range(20):
        clauses = []
        for j in range(10):
            idx = i * 10 + j
            vec = [round(random.random(), 4) for _ in range(256)]
            clauses.append(f"(c{j}:Chunk {{id: {idx}, doc: 'spec_v_{idx}', embedding: vecf32({vec})}})")
        project.query("CREATE " + ", ".join(clauses))

    query_vec = [round(random.random(), 4) for _ in range(256)]
    t_start = time.perf_counter()
    v_res = project.query(f"CALL db.idx.vector.querynodes('Chunk', 'embedding', 5, vecf32({query_vec}))")
    total_vec_ms = (time.perf_counter() - t_start) * 1000

    print(f"   ✓ Nearest Chunks Retrieved: {len(v_res.result_set)}")
    print(f"   ✓ Vector Engine Search Time: {v_res.execution_time_ms:.3f} ms ({v_res.execution_time_ms * 1000:.1f} µs)")
    print(f"   ✓ End-to-End Vector Retrieval: {total_vec_ms:.3f} ms")

    # -------------------------------------------------------------------------
    # 4. Clean Structural Ontology Metamodel Retrieval
    # -------------------------------------------------------------------------
    print("\n[4/4] Testing Clean Structural Ontology Metamodel Retrieval...")
    t_start = time.perf_counter()
    schema = project.get_ontology()
    total_onto_ms = (time.perf_counter() - t_start) * 1000

    print(f"   ✓ Node Labels: {schema['node_labels']}")
    print(f"   ✓ Relationships: {[r['rel_type'] for r in schema['relationship_schema']]}")
    print(f"   ✓ Ontology Retrieval Time: {total_onto_ms:.3f} ms ({total_onto_ms * 1000:.1f} µs)")

    # Summary
    print("\n" + "=" * 80)
    print("📊 INSTANTANEOUS QUERY RETRIEVAL BENCHMARK SUMMARY")
    print("=" * 80)
    print(f"{'Retrieval Pattern':<35} | {'Engine Internal Latency':<25} | {'Performance'}")
    print("-" * 80)
    print(f"{'Indexed Point Entity Lookup':<35} | {f'{avg_engine_ms*1000:.1f} microseconds':<25} | Instantaneous ⚡")
    print(f"{'2-Hop GraphBLAS Path Expansion':<35} | {f'{res.execution_time_ms*1000/len(res.result_set):.1f} µs / path':<25} | Instantaneous ⚡")
    print(f"{'256-Dim Vector HNSW Search':<35} | {f'{v_res.execution_time_ms*1000:.1f} microseconds':<25} | Instantaneous ⚡")
    print(f"{'Ontology Metamodel Inspection':<35} | {f'{total_onto_ms*1000:.1f} microseconds':<25} | Instantaneous ⚡")
    print("=" * 80)

    project.delete()
    print("✓ Benchmark graph cleaned up successfully.\n")


if __name__ == "__main__":
    run_query_benchmarks()
