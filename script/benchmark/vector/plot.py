#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
"""Plot vector index benchmark results produced by run.sh (E1-E4).

Usage:
    uv run --with matplotlib python script/benchmark/vector/plot.py \
        --results benchmark-results/20260101-120000

Plots are written to <results>/plots/.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

DRIFTS = ["uniform", "skew", "shift"]

POLICY_ORDER = {"none": 0, "periodic": 1, "always": 2, "auto": 3}
POLICY_STYLE = {
    "none": {"color": "#d62728", "linestyle": "--", "marker": "o"},
    "periodic": {"color": "#2ca02c", "linestyle": "-.", "marker": "s"},
    "always": {"color": "#1f77b4", "linestyle": ":", "marker": "^"},
}
AUTO_COLORS = ["#ff7f0e", "#9467bd", "#8c564b", "#e377c2", "#17becf"]


def load_results(results_dir: Path) -> list[dict]:
    out = []
    for path in sorted(results_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        data["_file"] = path.name
        out.append(data)
    return out


def policy_label(data: dict) -> str:
    policy = data.get("policy", "?")
    if policy == "auto":
        return f"auto@{data.get('max_delta_ratio')}"
    return policy


def policy_sort_key(data: dict) -> tuple:
    policy = data.get("policy", "?")
    ratio = data.get("max_delta_ratio", 0.0)
    return (POLICY_ORDER.get(policy, 9), ratio)


def policy_style(data: dict, auto_index: int) -> dict:
    policy = data.get("policy")
    if policy in POLICY_STYLE:
        return dict(POLICY_STYLE[policy])
    return {
        "color": AUTO_COLORS[auto_index % len(AUTO_COLORS)],
        "linestyle": "-",
        "marker": "D",
    }


def save(fig, outdir: Path, name: str) -> None:
    path = outdir / f"{name}.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  {path}")


def dataset_of(data: dict) -> str:
    return data.get("_file", "unknown").split("_")[1]


def plot_e1(data: list[dict], outdir: Path) -> None:
    runs = [d for d in data if d.get("scenario") == "stream"]
    if not runs:
        return
    print("E1:")
    datasets = sorted({dataset_of(d) for d in runs})
    for ds in datasets:
        ds_runs = [d for d in runs if dataset_of(d) == ds]
        for drift in DRIFTS:
            group = sorted(
                (d for d in ds_runs if d.get("drift") == drift),
                key=policy_sort_key,
            )
            if not group:
                continue
            fig, ax = plt.subplots(figsize=(9, 5.5))
            auto_index = 0
            for d in group:
                rounds = [r["round"] for r in d["rounds_data"]]
                recalls = [
                    (r.get("search") or {}).get("recall_at_k")
                    for r in d["rounds_data"]
                ]
                xs = [x for x, y in zip(rounds, recalls) if y is not None]
                ys = [y for y in recalls if y is not None]
                style = policy_style(d, auto_index)
                if d.get("policy") == "auto":
                    auto_index += 1
                ax.plot(xs, ys, label=policy_label(d), linewidth=1.8, **style)
                rebuilt = [
                    (r["round"], (r.get("search") or {}).get("recall_at_k"))
                    for r in d["rounds_data"]
                    if r.get("rebuilt")
                ]
                rebuilt = [(x, y) for x, y in rebuilt if y is not None]
                if rebuilt:
                    ax.scatter(
                        [x for x, _ in rebuilt],
                        [y for _, y in rebuilt],
                        marker="x",
                        s=60,
                        color=style["color"],
                        zorder=5,
                    )
            ax.set_xlabel("update round")
            ax.set_ylabel("recall@10")
            ax.set_ylim(0.0, 1.02)
            ax.set_title(f"E1 recall vs updates — {ds}, drift={drift} (x = rebuild)")
            ax.grid(alpha=0.3, linestyle="--")
            ax.legend(fontsize=8, ncol=2)
            save(fig, outdir, f"e1_recall_{ds}_{drift}")

    # quality vs cost scatter (one figure per dataset)
    markers = {"uniform": "o", "skew": "s", "shift": "^"}
    for ds in datasets:
        ds_runs = [d for d in runs if dataset_of(d) == ds]
        fig, ax = plt.subplots(figsize=(8.5, 6))
        for d in ds_runs:
            summary = d.get("summary", {})
            cost = summary.get("total_index_update_ms")
            recall = summary.get("min_recall")
            if cost is None or recall is None:
                continue
            drift = d.get("drift", "?")
            policy = d.get("policy")
            style = POLICY_STYLE.get(policy, {"color": "#ff7f0e"})
            ax.scatter(
                cost,
                recall,
                marker=markers.get(drift, "o"),
                color=style.get("color", "#ff7f0e"),
                s=45,
                alpha=0.85,
            )
            ax.annotate(
                f"{policy_label(d)}/{drift}",
                (cost, recall),
                fontsize=6,
                xytext=(3, 3),
                textcoords="offset points",
            )
        ax.set_xlabel("total index update time (ms)")
        ax.set_ylabel("min recall@10 over rounds")
        ax.set_title(f"E1 quality vs rebuild cost — {ds}")
        ax.grid(alpha=0.3, linestyle="--")
        save(fig, outdir, f"e1_quality_cost_{ds}")


def plot_e2(data: list[dict], outdir: Path) -> None:
    runs = [d for d in data if d.get("scenario") == "build"]
    if not runs:
        return
    print("E2:")
    datasets = sorted({d["_file"].split("_")[1] for d in runs})
    for ds in datasets:
        group = [d for d in runs if d["_file"].startswith(f"e2_{ds}_")]
        if not group:
            continue
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
        for d in sorted(group, key=lambda x: (x["config"]["nlist"], x["n_base"])):
            nlist = d["config"]["nlist"]
            color = AUTO_COLORS[list({g["config"]["nlist"] for g in group}).index(nlist) % len(AUTO_COLORS)]
            ax1.plot(
                d["n_base"],
                d["build_ms"],
                marker="o",
                color=color,
                label=f"nlist={nlist}",
            )
            ax2.plot(
                d["n_base"],
                d["index_size_bytes"] / (1024 * 1024),
                marker="o",
                color=color,
                label=f"nlist={nlist}",
            )
        for ax, ylabel in ((ax1, "build time (ms)"), (ax2, "index size (MiB)")):
            ax.set_xlabel("base vectors")
            ax.set_ylabel(ylabel)
            ax.grid(alpha=0.3, linestyle="--")
            ax.legend(fontsize=8)
        ax1.set_title(f"E2 fresh build time — {ds}")
        ax2.set_title(f"E2 index size — {ds}")
        save(fig, outdir, f"e2_build_{ds}")


def plot_e3(data: list[dict], outdir: Path) -> None:
    runs = [d for d in data if d.get("scenario") == "trigger"]
    if not runs:
        return
    print("E3:")
    datasets = sorted({d["_file"].split("_")[1] for d in runs})
    for ds in datasets:
        fig, axes = plt.subplots(1, len(DRIFTS), figsize=(5 * len(DRIFTS), 4.4))
        if len(DRIFTS) == 1:
            axes = [axes]
        for ax, drift in zip(axes, DRIFTS):
            d = next(
                (x for x in runs if x.get("drift") == drift and x["_file"].startswith(f"e3_{ds}_")),
                None,
            )
            if d is None:
                ax.set_visible(False)
                continue
            rounds = [r["round"] for r in d["rounds_data"]]
            cluster = [r["stats"]["max_cluster_delta_ratio"] for r in d["rounds_data"]]
            shard = [r["stats"]["shard_delta_ratio"] for r in d["rounds_data"]]
            recalls = [
                (r.get("search") or {}).get("recall_at_k")
                for r in d["rounds_data"]
            ]
            ax.plot(rounds, cluster, marker="o", color="#d62728", label="max cluster delta/base")
            ax.plot(rounds, shard, marker="s", color="#1f77b4", label="shard delta/base")
            ax.set_xlabel("update round")
            ax.set_ylabel("delta / base ratio")
            ax.grid(alpha=0.3, linestyle="--")
            ax2 = ax.twinx()
            xs = [x for x, y in zip(rounds, recalls) if y is not None]
            ys = [y for y in recalls if y is not None]
            ax2.plot(xs, ys, color="#2ca02c", linestyle=":", marker="x", label="recall@10")
            ax2.set_ylabel("recall@10")
            ax2.set_ylim(0.0, 1.02)
            ax.set_title(f"E3 trigger — {ds}/{drift}")
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2, fontsize=7, loc="center right")
        save(fig, outdir, f"e3_trigger_{ds}")


def plot_e4(data: list[dict], outdir: Path) -> None:
    runs = [d for d in data if d.get("scenario") == "search"]
    if not runs:
        return
    print("E4:")
    datasets = sorted({d["_file"].split("_")[1] for d in runs})
    states = ["fresh", "delta", "rebuilt"]
    state_style = {
        "fresh": {"color": "#1f77b4", "marker": "o"},
        "delta": {"color": "#d62728", "marker": "s"},
        "rebuilt": {"color": "#2ca02c", "marker": "^"},
    }
    for ds in datasets:
        fig, ax = plt.subplots(figsize=(8.5, 5.5))
        plotted = False
        for state in states:
            d = next(
                (
                    x
                    for x in runs
                    if x["_file"] == f"e4_{ds}_{state}.json"
                    or x["_file"].startswith(f"e4_{ds}_{state}_")
                ),
                None,
            )
            if d is None:
                continue
            sweep = sorted(d["sweep"], key=lambda s: s["recall_at_k"])
            ax.plot(
                [s["recall_at_k"] for s in sweep],
                [s["qps"] for s in sweep],
                label=state,
                linewidth=1.8,
                **state_style[state],
            )
            plotted = True
        if not plotted:
            plt.close(fig)
            continue
        ax.set_xlabel("recall@10")
        ax.set_ylabel("QPS")
        ax.set_yscale("log")
        ax.set_title(f"E4 recall vs QPS — {ds}")
        ax.grid(alpha=0.3, linestyle="--")
        ax.legend(fontsize=9)
        save(fig, outdir, f"e4_recall_qps_{ds}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, type=Path)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    outdir = args.out or (args.results / "plots")
    outdir.mkdir(parents=True, exist_ok=True)

    data = load_results(args.results)
    if not data:
        raise SystemExit(f"no JSON results under {args.results}")
    print(f"loaded {len(data)} results from {args.results}")

    plot_e1(data, outdir)
    plot_e2(data, outdir)
    plot_e3(data, outdir)
    plot_e4(data, outdir)


if __name__ == "__main__":
    main()
