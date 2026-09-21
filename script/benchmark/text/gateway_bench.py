#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Benchmark the ES-compatible gateway HTTP path (T7).

Writes a corpus through `_bulk`, runs keyword `_search` queries and measures
throughput/latency/quality, then repeats after append and rewrite rounds
(delete-by-query + bulk, the re-parse pattern).

Only the standard library is used, so it runs with the repository's Python
environment directly:

    python script/benchmark/text/gateway_bench.py \
        --gateway-url http://127.0.0.1:19200 --index my_index \
        --corpus corpus.jsonl --queries queries.jsonl --qrels qrels.tsv \
        --limit 20000 --out t7.json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def http_json(method: str, url: str, body: dict | None = None) -> dict:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        payload = error.read()
        raise RuntimeError(
            f"{method} {url} -> {error.code}: {payload[:300]!r}"
        ) from error
    return json.loads(payload) if payload else {}


def http_ndjson(url: str, payload: str) -> dict:
    request = urllib.request.Request(
        url,
        data=payload.encode(),
        headers={"content-type": "application/x-ndjson"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        payload = error.read()
        raise RuntimeError(f"POST {url} -> {error.code}: {payload[:300]!r}") from error
    return json.loads(payload) if payload else {}


def load_jsonl(path: Path, limit: int | None = None) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows.append(json.loads(line))
            if limit is not None and len(rows) >= limit:
                break
    return rows


def load_qrels(path: Path | None) -> dict[str, dict[str, int]]:
    qrels: dict[str, dict[str, int]] = {}
    if path is None:
        return qrels
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 4:
                continue
            qid, docid, grade = fields[0], fields[2], int(float(fields[3]))
            qrels.setdefault(qid, {})[docid] = grade
    return qrels


def bulk(index: str, docs: list[dict], batch_size: int) -> tuple[float, int]:
    """Write documents through `_bulk`; returns (seconds, written)."""
    url = f"{index}/_bulk"
    written = 0
    started = time.perf_counter()
    for start in range(0, len(docs), batch_size):
        batch = docs[start : start + batch_size]
        lines = []
        for doc in batch:
            lines.append(json.dumps({"create": {}}))
            lines.append(json.dumps(doc))
        response = http_ndjson(url, "\n".join(lines) + "\n")
        if response.get("errors"):
            raise RuntimeError(f"bulk reported errors: {response['items'][:2]}")
        written += len(batch)
    return time.perf_counter() - started, written


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    index = round((len(values) - 1) * fraction)
    return values[index]


def dcg(grades: list[int]) -> float:
    return sum(
        (2**grade - 1) / math.log2(position + 2)
        for position, grade in enumerate(grades)
    )


def search_once(index: str, query: str, size: int) -> tuple[list[str], float]:
    body = {
        "query": {
            "bool": {
                "must": [{"match": {"content": query}}],
                "must_not": [{"term": {"is_enabled": False}}],
            }
        },
        "size": size,
        "_source": {"excludes": ["embedding"]},
    }
    started = time.perf_counter()
    response = http_json("POST", f"{index}/_search", body)
    elapsed = time.perf_counter() - started
    hits = response.get("hits", {}).get("hits", [])
    return [hit["_source"].get("source_id", "") for hit in hits], elapsed


def evaluate(
    index: str,
    queries: list[dict],
    qrels: dict[str, dict[str, int]],
    top_k: int,
    concurrency: int,
) -> dict:
    # Latency pass.
    latencies = []

    def one(query: dict) -> list[str]:
        ids, elapsed = search_once(index, query["text"], top_k)
        latencies.append(elapsed)
        return ids

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(one, queries))

    # Quality pass (top-100 for Recall@100).
    quality_k = max(top_k, 100)
    ndcg = recall100 = mrr = 0.0
    counted = 0
    for query in queries:
        judged = qrels.get(query["id"])
        if not judged:
            continue
        ids, _ = search_once(index, query["text"], quality_k)
        grades = [judged.get(doc, 0) for doc in ids[:10]]
        ideal = sorted(judged.values(), reverse=True)
        ideal_dcg = dcg(ideal)
        ndcg += dcg(grades) / ideal_dcg if ideal_dcg else 0.0
        relevant = [doc for doc, grade in judged.items() if grade > 0]
        if relevant:
            recall100 += len(set(ids[:100]) & set(relevant)) / len(relevant)
        rank = next(
            (
                position
                for position, doc in enumerate(ids[:10])
                if judged.get(doc, 0) > 0
            ),
            None,
        )
        mrr += 1.0 / (rank + 1) if rank is not None else 0.0
        counted += 1
    counted = max(counted, 1)
    total = sum(latencies)
    return {
        "n_queries": len(queries),
        "qps": len(queries) / total if total else 0.0,
        "mean_ms": statistics.mean(latencies) * 1000 if latencies else 0.0,
        "p50_ms": percentile(latencies, 0.50) * 1000,
        "p95_ms": percentile(latencies, 0.95) * 1000,
        "p99_ms": percentile(latencies, 0.99) * 1000,
        "ndcg_at_10": ndcg / counted,
        "recall_at_100": recall100 / counted,
        "mrr_at_10": mrr / counted,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-url", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--corpus", required=True, type=Path)
    parser.add_argument("--queries", required=True, type=Path)
    parser.add_argument("--qrels", type=Path)
    parser.add_argument("--limit", type=int, default=20000)
    parser.add_argument("--query-limit", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=0)
    parser.add_argument("--per-round", type=int, default=2000)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    base = args.gateway_url.rstrip("/")
    index_url = f"{base}/{args.index}"

    info = http_json("GET", f"{base}/")
    version = info.get("version", {}).get("number", "?")
    print(f"gateway version: {version}")

    corpus = load_jsonl(args.corpus, args.limit)
    pool = load_jsonl(args.corpus)[
        args.limit : args.limit + args.rounds * args.per_round + 1024
    ]
    queries = load_jsonl(args.queries)
    qrels = load_qrels(args.qrels)
    if qrels:
        # Mirror the Rust harness: judged documents outside the loaded corpus
        # cannot be retrieved, so evaluate the queries that have at least one
        # judged document inside it.
        corpus_ids = {row["id"] for row in corpus}
        qrels = {
            qid: {doc: grade for doc, grade in judged.items() if doc in corpus_ids}
            for qid, judged in qrels.items()
        }
        qrels = {qid: judged for qid, judged in qrels.items() if judged}
        queries = [query for query in queries if query["id"] in qrels]
    queries = queries[: args.query_limit]
    judged = {doc for per_query in qrels.values() for doc in per_query}

    docs = [
        {"content": row["text"], "source_id": row["id"], "is_enabled": True}
        for row in corpus
    ]
    seconds, written = bulk(index_url, docs, args.batch_size)
    base_write = {
        "documents": written,
        "seconds": seconds,
        "docs_per_sec": written / seconds if seconds else 0.0,
    }
    print(
        f"base bulk: {written} docs in {seconds:.2f}s ({base_write['docs_per_sec']:.0f}/s)"
    )

    rounds = []
    next_pool = list(pool)
    for round_index in range(1, args.rounds + 1):
        updates = next_pool[: args.per_round]
        next_pool = next_pool[args.per_round :]
        round_docs = [
            {"content": row["text"], "source_id": row["id"], "is_enabled": True}
            for row in updates
        ]
        seconds, written = bulk(index_url, round_docs, args.batch_size)
        quality = evaluate(index_url, queries, qrels, args.top_k, args.concurrency)
        rounds.append(
            {
                "round": round_index,
                "documents": written,
                "write_docs_per_sec": written / seconds if seconds else 0.0,
                "search": quality,
            }
        )
        print(
            f"round {round_index}: {written} docs, {quality['qps']:.1f} qps, ndcg={quality['ndcg_at_10']:.4f}"
        )

    # Rewrite round: delete judged-free documents and re-insert other texts
    # (the re-parse pattern) to exercise tombstones + delta index updates.
    rewrite = None
    if args.rounds > 0:
        rewrite_targets = [row for row in corpus if row["id"] not in judged][
            : args.per_round
        ]
        if rewrite_targets:
            source_ids = [row["id"] for row in rewrite_targets]
            started = time.perf_counter()
            http_json(
                "POST",
                f"{index_url}/_delete_by_query",
                {"query": {"terms": {"source_id": source_ids}}},
            )
            delete_seconds = time.perf_counter() - started
            replacements = []
            for offset, row in enumerate(rewrite_targets):
                replacement = (
                    next_pool[offset % max(len(next_pool), 1)] if next_pool else row
                )
                replacements.append(
                    {
                        "content": replacement["text"],
                        "source_id": row["id"],
                        "is_enabled": True,
                    }
                )
            seconds, written = bulk(index_url, replacements, args.batch_size)
            quality = evaluate(index_url, queries, qrels, args.top_k, args.concurrency)
            rewrite = {
                "documents": written,
                "delete_seconds": delete_seconds,
                "write_docs_per_sec": written / seconds if seconds else 0.0,
                "search": quality,
            }
            print(
                f"rewrite: {written} docs re-inserted, ndcg={quality['ndcg_at_10']:.4f}"
            )

    summary = {
        "scenario": "gateway",
        "gateway_version": version,
        "index": args.index,
        "n_corpus": len(corpus),
        "n_queries": len(queries),
        "base_write": base_write,
        "base_search": evaluate(
            index_url, queries, qrels, args.top_k, args.concurrency
        ),
        "rounds": rounds,
        "rewrite": rewrite,
    }
    if args.out:
        args.out.write_text(json.dumps(summary, indent=2))
    else:
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
