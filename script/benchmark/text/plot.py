#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Render the text index benchmark plots.

Usage:
    uv run --with matplotlib python script/benchmark/text/plot.py \
        --results benchmark-results/text-20260101-000000
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

POLICY_COLORS = {
    "none": "#888888",
    "auto": "#d62728",
    "periodic": "#1f77b4",
    "always": "#2ca02c",
}
STATE_COLORS = {"fresh": "#1f77b4", "delta": "#d62728", "rebuilt": "#2ca02c"}


def load_results(results_dir: Path) -> list[dict]:
    results = []
    for path in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        data["_file"] = path.stem
        results.append(data)
    return results


def save(fig, outdir: Path, name: str) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(outdir / f"{name}.png", dpi=140)
    plt.close(fig)
    print(f"  {outdir / f'{name}.png'}")


def dataset_of(data: dict) -> str:
    for name in ("scifact", "msmarco", "t2retrieval"):
        if name in data.get("_file", ""):
            return name
    return "dataset"


def plot_t1(results: list[dict], outdir: Path) -> None:
    """Recall and index update cost per round under the rebuild policies."""
    for dataset in sorted({dataset_of(d) for d in results}):
        policies: dict[str, dict] = {}
        for data in results:
            name = data.get("_file", "")
            if not name.startswith("t1_") or dataset not in name:
                continue
            if data.get("scenario") != "stream":
                continue
            if not name.endswith("_append"):
                continue
            label = name.split(f"{dataset}_", 1)[1]
            label = label[: -len("_append")]
            policies[label] = data
        if not policies:
            continue
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.2))
        for label, data in sorted(policies.items()):
            rounds = [row["round"] for row in data["rounds_data"]]
            recalls = [
                row["search"].get("recall_at_k_vs_exact_bm25")
                if isinstance(row.get("search"), dict)
                else None
                for row in data["rounds_data"]
            ]
            index_ms = [row["index_update_ms"] for row in data["rounds_data"]]
            color = POLICY_COLORS.get(
                label.split("@")[0].rstrip("0123456789."), "#9467bd"
            )
            if label.startswith("auto") and label != "auto":
                color = POLICY_COLORS["auto"]
                label = f"auto@{label[len('auto') :]}"
            axes[0].plot(rounds, recalls, marker="o", label=label, color=color)
            axes[1].plot(rounds, index_ms, marker="o", label=label, color=color)
        axes[0].set(xlabel="round", ylabel="recall@10 vs exact BM25", ylim=(0, 1.02))
        axes[1].set(xlabel="round", ylabel="index update (ms)", yscale="log")
        axes[0].set_title(f"T1 {dataset}: recall under incremental updates")
        axes[1].set_title(f"T1 {dataset}: index update cost")
        axes[0].legend(fontsize=8)
        axes[1].legend(fontsize=8)
        save(fig, outdir, f"t1_{dataset}_policy_recall_cost")


def plot_t2(results: list[dict], outdir: Path) -> None:
    """Build scaling and the tokenizer/parameter matrix."""
    scaling = []
    matrix = []
    for data in results:
        name = data.get("_file", "")
        if data.get("scenario") != "build":
            continue
        if "scale" in name:
            scaling.append(data)
        elif name.startswith("t2_"):
            matrix.append(data)
    if scaling:
        scaling.sort(key=lambda d: d["n_docs"])
        docs = [d["n_docs"] for d in scaling]
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
        axes[0].plot(docs, [d["build_ms"] for d in scaling], marker="o")
        axes[0].set(xlabel="documents", ylabel="build (ms)", xscale="log", yscale="log")
        axes[1].plot(docs, [d["docs_per_sec"] for d in scaling], marker="o")
        axes[1].set(xlabel="documents", ylabel="docs/s", xscale="log")
        axes[2].plot(
            docs,
            [d["index_size_bytes"] / max(d["n_docs"], 1) for d in scaling],
            marker="o",
        )
        axes[2].set(xlabel="documents", ylabel="index bytes/doc", xscale="log")
        dataset = dataset_of(scaling[0])
        fig.suptitle(f"T2 {dataset}: fresh build scaling")
        save(fig, outdir, f"t2_{dataset}_scaling")
    if matrix:
        labels = [d["_file"].replace("t2_", "").replace("_", " ") for d in matrix]
        fig, axes = plt.subplots(1, 2, figsize=(max(10, len(matrix) * 1.1), 4.6))
        axes[0].bar(labels, [d["build_ms"] for d in matrix])
        axes[0].set(ylabel="build (ms)")
        axes[1].bar(labels, [d["quality"]["ndcg_at_10"] for d in matrix])
        axes[1].set(ylabel="nDCG@10 vs qrels", ylim=(0, 1))
        fig.suptitle("T2: tokenizer / positions / stored matrix")
        for axis in axes:
            axis.tick_params(axis="x", rotation=45, labelsize=8)
        save(fig, outdir, "t2_params_matrix")


def plot_t3(results: list[dict], outdir: Path) -> None:
    """Drift signals vs recall, with the auto-trigger firing points."""
    for dataset in sorted({dataset_of(d) for d in results}):
        triggers = [
            d
            for d in results
            if d.get("scenario") == "trigger" and dataset in d.get("_file", "")
        ]
        if not triggers:
            continue
        fig, axes = plt.subplots(
            1, len(triggers), figsize=(5 * len(triggers), 4.2), squeeze=False
        )
        for axis, data in zip(axes[0], sorted(triggers, key=lambda d: d["_file"])):
            rounds = [row["round"] for row in data["rounds_data"]]
            ratios = [row["max_delta_ratio"] for row in data["rounds_data"]]
            recalls = [
                row["search"].get("recall_at_k_vs_exact_bm25")
                if isinstance(row.get("search"), dict)
                else None
                for row in data["rounds_data"]
            ]
            axis.plot(rounds, ratios, marker="o", color="#d62728", label="delta/base")
            axis.set(xlabel="round", ylabel="delta/base ratio", yscale="log")
            twin = axis.twinx()
            twin.plot(rounds, recalls, marker="s", color="#2ca02c", label="recall@10")
            twin.set(ylabel="recall@10", ylim=(0, 1.02))
            drift = data["_file"].split("_")[-1]
            axis.set_title(f"T3 {dataset}: {drift}")
        save(fig, outdir, f"t3_{dataset}_trigger")


def plot_t4(results: list[dict], outdir: Path) -> None:
    """Candidate sweep per index state plus the verification cost."""
    for dataset in sorted({dataset_of(d) for d in results}):
        states: dict[str, dict] = {}
        for data in results:
            name = data.get("_file", "")
            if data.get("scenario") != "search" or dataset not in name:
                continue
            if name.endswith("_noverify"):
                continue
            state = name.split(f"t4_{dataset}_", 1)[-1].rsplit("_", 1)[0]
            states[state] = data
        if not states:
            continue
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
        for state, data in sorted(states.items()):
            sweep = data.get("sweep", [])
            color = STATE_COLORS.get(state, "#9467bd")
            axes[0].plot(
                [row["candidates_per_shard"] for row in sweep],
                [row["recall_at_k_vs_exact_bm25"] for row in sweep],
                marker="o",
                label=state,
                color=color,
            )
            axes[1].plot(
                [row["candidates_per_shard"] for row in sweep],
                [row["p99_ms"] for row in sweep],
                marker="o",
                label=state,
                color=color,
            )
            axes[2].plot(
                [row["candidates_per_shard"] for row in sweep],
                [row["qps"] for row in sweep],
                marker="o",
                label=state,
                color=color,
            )
        axes[0].set(
            xlabel="candidates/shard", ylabel="recall@10 vs exact BM25", ylim=(0, 1.02)
        )
        axes[1].set(xlabel="candidates/shard", ylabel="p99 (ms)", xscale="log")
        axes[2].set(xlabel="candidates/shard", ylabel="QPS", xscale="log")
        for axis in axes:
            axis.legend(fontsize=8)
        fig.suptitle(f"T4 {dataset}: search across index states")
        save(fig, outdir, f"t4_{dataset}_search_states")

        noverify = [
            d
            for d in results
            if d.get("_file") == f"t4_{dataset}_fresh_search_noverify"
        ]
        fresh = states.get("fresh")
        if noverify and fresh:
            verify_row = fresh["sweep"][-1]
            plain_row = noverify[0]["sweep"][-1]
            labels = ["verify on", "verify off"]
            fig, axes = plt.subplots(1, 2, figsize=(8, 4))
            axes[0].bar(labels, [verify_row["p50_ms"], plain_row["p50_ms"]])
            axes[0].set(ylabel="p50 (ms)")
            axes[1].bar(labels, [verify_row["qps"], plain_row["qps"]])
            axes[1].set(ylabel="QPS")
            fig.suptitle(f"T4 {dataset}: verification cost")
            save(fig, outdir, f"t4_{dataset}_verify_cost")


def plot_t5(results: list[dict], outdir: Path) -> None:
    """Shard count vs build cost, latency and recall."""
    for dataset in sorted({dataset_of(d) for d in results}):
        builds, searches = [], []
        for data in results:
            name = data.get("_file", "")
            if dataset not in name or "shards" not in name:
                continue
            shards = int(name.split("shards", 1)[1].split("_", 1)[0])
            if data.get("scenario") == "build":
                builds.append((shards, data))
            elif data.get("scenario") == "search":
                searches.append((shards, data))
        if not builds and not searches:
            continue
        builds.sort()
        searches.sort()
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
        if builds:
            axes[0].plot(
                [s for s, _ in builds],
                [d["build_ms"] for _, d in builds],
                marker="o",
            )
            axes[0].set(xlabel="shards", ylabel="build (ms)", xscale="log")
        for shards, data in searches:
            sweep = data.get("sweep", [])
            axes[1].plot(
                [row["candidates_per_shard"] for row in sweep],
                [row["recall_at_k_vs_exact_bm25"] for row in sweep],
                marker="o",
                label=f"{shards} shards",
            )
            axes[2].plot(
                [row["candidates_per_shard"] for row in sweep],
                [row["p99_ms"] for row in sweep],
                marker="o",
                label=f"{shards} shards",
            )
        axes[1].set(xlabel="candidates/shard", ylabel="recall@10", ylim=(0, 1.02))
        axes[2].set(xlabel="candidates/shard", ylabel="p99 (ms)", xscale="log")
        axes[1].legend(fontsize=8)
        axes[2].legend(fontsize=8)
        fig.suptitle(f"T5 {dataset}: hash bucket (shard) count")
        save(fig, outdir, f"t5_{dataset}_shards")


def plot_t6(results: list[dict], outdir: Path) -> None:
    rows = [d for d in results if d.get("scenario") == "sql"]
    if not rows:
        return
    labels = [d["_file"] for d in rows]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.2))
    axes[0].bar(labels, [d["search"]["qps"] for d in rows])
    axes[0].set(ylabel="QPS")
    axes[1].bar(labels, [d["search"]["p99_ms"] for d in rows])
    axes[1].set(ylabel="p99 (ms)")
    axes[2].bar(labels, [d["quality"]["ndcg_at_10"] for d in rows])
    axes[2].set(ylabel="nDCG@10", ylim=(0, 1))
    fig.suptitle("T6: end-to-end SQL search")
    save(fig, outdir, "t6_sql")


def plot_t7(results: list[dict], outdir: Path) -> None:
    rows = [d for d in results if d.get("scenario") == "gateway"]
    if not rows:
        return
    data = rows[0]
    fig, axes = plt.subplots(1, 3, figsize=(12, 4.2))
    stages = [("base", data["base_search"])]
    stages += [(f"round {r['round']}", r["search"]) for r in data.get("rounds", [])]
    if data.get("rewrite"):
        stages.append(("rewrite", data["rewrite"]["search"]))
    labels = [label for label, _ in stages]
    axes[0].bar(labels, [stage["qps"] for _, stage in stages])
    axes[0].set(ylabel="QPS")
    axes[1].bar(labels, [stage["p99_ms"] for _, stage in stages])
    axes[1].set(ylabel="p99 (ms)")
    axes[2].bar(labels, [stage["ndcg_at_10"] for _, stage in stages])
    axes[2].set(ylabel="nDCG@10", ylim=(0, 1))
    fig.suptitle("T7: gateway HTTP search (bulk write + incremental)")
    save(fig, outdir, "t7_gateway")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    args = parser.parse_args()
    results = load_results(args.results)
    if not results:
        raise SystemExit(f"no JSON results under {args.results}")
    outdir = args.results / "plots"
    print(f"plotting {len(results)} results -> {outdir}")
    plot_t1(results, outdir)
    plot_t2(results, outdir)
    plot_t3(results, outdir)
    plot_t4(results, outdir)
    plot_t5(results, outdir)
    plot_t6(results, outdir)
    plot_t7(results, outdir)


if __name__ == "__main__":
    main()
