#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Download and normalize real IR datasets for the text index benchmark.

The harness itself never touches the network: this script streams a dataset
from the Hugging Face Hub (the MTEB mirrors) and writes the normalized files
the Rust benchmark reads:

    corpus.jsonl   {"id": ..., "text": ...}   one document per line
    queries.jsonl  {"id": ..., "text": ...}   one query per line
    qrels.tsv      qid<TAB>0<TAB>docid<TAB>grade

Usage:
    uv run --with datasets python script/benchmark/text/download.py scifact \
        --out /data/text-bench/scifact
    uv run --with datasets python script/benchmark/text/download.py msmarco \
        --out /data/text-bench/msmarco-1m --limit 1000000

Datasets (HF repositories, parquet mirrors):
    scifact      mteb/scifact      5,183 docs / 300 test queries (smoke)
    msmarco      mteb/msmarco      8.8M docs / 7,437 dev queries (main)
    t2retrieval  mteb/T2Retrieval  118K docs / 22.8K queries (Chinese)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# repo -> qrels split (each config has a single split; the qrels split is
# picked explicitly so the standard test set is used per dataset).
DATASETS = {
    "scifact": ("mteb/scifact", "test"),
    "msmarco": ("mteb/msmarco", "dev"),
    "t2retrieval": ("mteb/T2Retrieval", "dev"),
}

# qrels are write-once per document, so a generous buffer keeps the JSONL
# writer cheap for the multi-million document corpus.
WRITE_BUFFER = 8192


def stream(repo: str, config: str, split: str | None = None):
    from datasets import get_dataset_split_names, load_dataset

    splits = get_dataset_split_names(repo, config)
    if not splits:
        raise SystemExit(f"dataset {repo!r} config {config!r} has no splits")
    if split is None:
        split = splits[0]
    elif split not in splits:
        raise SystemExit(
            f"dataset {repo!r} config {config!r} has no split {split!r} "
            f"(available: {splits})"
        )
    return load_dataset(repo, config, split=split, streaming=True)


def normalize_text(row: dict) -> str:
    title = (row.get("title") or "").strip()
    text = (row.get("text") or "").strip()
    if title:
        return f"{title}\n{text}"
    return text


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=sorted(DATASETS))
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Keep only the first N corpus documents (qrels are filtered accordingly).",
    )
    args = parser.parse_args()

    repo, qrels_split = DATASETS[args.dataset]
    args.out.mkdir(parents=True, exist_ok=True)

    kept_ids: set[str] | None = set() if args.limit else None
    corpus_count = 0
    corpus_path = args.out / "corpus.jsonl"
    with corpus_path.open("w", encoding="utf-8", buffering=WRITE_BUFFER) as corpus:
        for row in stream(repo, "corpus"):
            if args.limit is not None and corpus_count >= args.limit:
                break
            doc_id = str(row["_id"])
            if kept_ids is not None:
                kept_ids.add(doc_id)
            corpus.write(json.dumps({"id": doc_id, "text": normalize_text(row)}) + "\n")
            corpus_count += 1
    if corpus_count == 0:
        raise SystemExit("no corpus documents downloaded")
    print(f"corpus: {corpus_count} documents -> {corpus_path}", file=sys.stderr)

    query_count = 0
    queries_path = args.out / "queries.jsonl"
    with queries_path.open("w", encoding="utf-8", buffering=WRITE_BUFFER) as queries:
        for row in stream(repo, "queries"):
            queries.write(
                json.dumps({"id": str(row["_id"]), "text": row["text"]}) + "\n"
            )
            query_count += 1
    print(f"queries: {query_count} -> {queries_path}", file=sys.stderr)

    qrels_count = 0
    qrels_path = args.out / "qrels.tsv"
    with qrels_path.open("w", encoding="utf-8", buffering=WRITE_BUFFER) as qrels:
        for row in stream(repo, "default", qrels_split):
            qid = str(row["query-id"])
            doc_id = str(row["corpus-id"])
            if kept_ids is not None and doc_id not in kept_ids:
                continue
            grade = int(float(row["score"]))
            qrels.write(f"{qid}\t0\t{doc_id}\t{grade}\n")
            qrels_count += 1
    print(f"qrels: {qrels_count} -> {qrels_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
