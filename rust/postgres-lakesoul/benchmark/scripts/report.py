#!/usr/bin/env python3
# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Summaries over raw benchmark results. Standard library only.

  report.py summary RUN_DIR [--name NAME]   # per-query latency + resources
  report.py compare BASE_DIR CAND_DIR --name NAME
                                             # per-query median ratio between runs
  report.py memory RUN_DIR... [--names serial,burst]
                                             # memory-matrix table (scripts/memory-matrix.sh)

Inputs are the files `scripts/bench.py drive` writes: NAME.jsonl (driver
samples), NAME.sampler.<role>.<pod>.jsonl, NAME.pods.json.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def quantile(xs: list[float], q: float) -> float:
    if not xs:
        return math.nan
    s = sorted(xs)
    k = (len(s) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def samples(path: Path) -> list[dict]:
    out = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if "latency_s" in d:
            out.append(d)
    return out


def phase(r: dict) -> str:
    """TPC-H iteration phase: warmup (<0), cold (0, first measured pass after a
    fresh deploy with --warmup 0), warm (>0). Other scenarios have none."""
    if r.get("scenario") != "tpch":
        return ""
    i = r.get("iteration") or 0
    return "warmup" if i < 0 else "cold" if i == 0 else "warm"


def latency_table(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(r.get("scenario"), phase(r), r.get("query"))].append(r)
    table = []
    for (scen, ph, q), rs in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        ok = [r["latency_s"] for r in rs if r.get("status") == "ok"]
        errs: dict[str, int] = defaultdict(int)
        for r in rs:
            if r.get("status") != "ok":
                errs[f"{r.get('status')}:{r.get('sqlstate') or ''}"] += 1
        mean = statistics.fmean(ok) if ok else math.nan
        sd = statistics.stdev(ok) if len(ok) > 1 else math.nan
        table.append(
            {
                "scenario": scen,
                "phase": ph,
                "query": q,
                "n": len(rs),
                "ok": len(ok),
                "errors": dict(errs),
                "min": min(ok) if ok else math.nan,
                "p50": quantile(ok, 0.5),
                "p90": quantile(ok, 0.9),
                "p95": quantile(ok, 0.95),
                "p99": quantile(ok, 0.99),
                "max": max(ok) if ok else math.nan,
                "mean": mean,
                "cv": sd / mean if ok and mean else math.nan,
                "first_row_p50": quantile([r["first_row_s"] for r in rs if r.get("status") == "ok" and r.get("first_row_s") is not None], 0.5),
                "rows": statistics.median([r.get("rows") or 0 for r in rs]) if rs else 0,
                "bytes": statistics.median([r.get("bytes") or 0 for r in rs]) if rs else 0,
            }
        )
    return table


def throughput(rows: list[dict]) -> dict:
    ok = [r for r in rows if r.get("status") == "ok"]
    if not rows:
        return {}
    t0 = min(r["start_unix"] for r in rows)
    t1 = max(r["end_unix"] for r in rows)
    span = max(t1 - t0, 1e-9)
    return {
        "span_s": span,
        "queries": len(rows),
        "ok": len(ok),
        "ok_qps": len(ok) / span,
        "error_rate": 1 - len(ok) / len(rows),
        "result_bytes_per_s": sum(r.get("bytes") or 0 for r in ok) / span,
    }


def sampler_summary(path: Path) -> dict:
    rows = []
    for line in path.read_text().splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if d.get("pid"):
            rows.append(d)
    if not rows:
        return {"samples": 0}

    def peak(k):
        v = [r[k] for r in rows if isinstance(r.get(k), (int, float))]
        return max(v) if v else None

    def delta(k):
        v = [r[k] for r in rows if isinstance(r.get(k), (int, float))]
        return (v[-1] - v[0]) if len(v) > 1 else None

    pids = sorted({r["pid"] for r in rows})
    cpu_ns = delta("cpuacct_usage_ns")
    span = rows[-1]["ts"] - rows[0]["ts"]
    return {
        "samples": len(rows),
        "pids": pids,
        "restarted": len(pids) > 1,
        "mem_max": rows[-1].get("mem_max"),
        "peak_mem_current": peak("mem_current"),
        "peak_cgroup_rss": peak("memstat_total_rss") or peak("memstat_anon"),
        "peak_vmrss_kb": peak("vmrss_kb"),
        "vmhwm_kb_last": rows[-1].get("vmhwm_kb"),
        "peak_threads": peak("threads"),
        "peak_fds": peak("fds"),
        "failcnt_delta": delta("mem_failcnt"),
        "oom_kill_delta": delta("oom_oom_kill"),
        "throttled_periods_delta": delta("cpustat_nr_throttled"),
        "periods_delta": delta("cpustat_nr_periods"),
        "throttled_s_delta": (delta("cpustat_throttled_time") or 0) / 1e9,
        "avg_cores": (cpu_ns / 1e9 / span) if cpu_ns is not None and span > 0 else None,
        "io_read_bytes_delta": delta("io_read_bytes"),
        "io_rchar_delta": delta("io_rchar"),
    }


def restarts(pods_path: Path) -> list[dict]:
    if not pods_path.exists():
        return []
    out = []
    for p in json.loads(pods_path.read_text()):
        for c in p["containers"]:
            if c.get("restarts") or c.get("last_terminated"):
                out.append({"pod": p["name"], "container": c["name"], "restarts": c.get("restarts"), "last": c.get("last_terminated")})
    return out


def fmt(x, nd=3):
    if isinstance(x, float):
        return "nan" if math.isnan(x) else f"{x:.{nd}f}"
    return str(x)


def summarize(run: Path, name: str) -> dict:
    rows = samples(run / f"{name}.jsonl")
    res = {
        "name": name,
        "latency": latency_table(rows),
        "throughput": throughput(rows),
        "resources": {
            p.name[len(name) + len(".sampler."):-len(".jsonl")]: sampler_summary(p)
            for p in sorted(run.glob(f"{name}.sampler.*.jsonl"))
        },
        "restarts": restarts(run / f"{name}.pods.json"),
    }
    return res


def print_summary(s: dict) -> None:
    print(f"## {s['name']}")
    print("| scenario | phase | query | n | ok | errors | p50 s | p95 s | p99 s | max s | CV | rows |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in s["latency"]:
        print(
            f"| {r['scenario']} | {r['phase']} | {r['query']} | {r['n']} | {r['ok']} | {r['errors'] or ''} | {fmt(r['p50'])} | "
            f"{fmt(r['p95'])} | {fmt(r['p99'])} | {fmt(r['max'])} | {fmt(r['cv'], 2)} | {fmt(r['rows'], 0)} |"
        )
    print(f"\nthroughput: {json.dumps({k: round(v, 4) if isinstance(v, float) else v for k, v in s['throughput'].items()})}")
    for k, v in s["resources"].items():
        print(f"resources {k}: {json.dumps(v)}")
    if s["restarts"]:
        print(f"restarts: {json.dumps(s['restarts'])}")
    print()


def cmd_summary(a) -> None:
    run = Path(a.run_dir)
    names = [a.name] if a.name else sorted(
        p.name[: -len(".jsonl")] for p in run.glob("*.jsonl") if ".sampler." not in p.name
    )
    out = []
    for n in names:
        s = summarize(run, n)
        out.append(s)
        print_summary(s)
    (run / "summary.json").write_text(json.dumps(out, indent=1, default=str) + "\n")


def cmd_compare(a) -> None:
    def key(r):
        return (r["scenario"], r["phase"], r["query"])

    base = {key(r): r for r in latency_table(samples(Path(a.base) / f"{a.name}.jsonl"))}
    cand = {key(r): r for r in latency_table(samples(Path(a.cand) / f"{a.name}.jsonl"))}
    print("| phase | query | base p50 | cand p50 | ratio | base (p90-min)/p50 | verdict |")
    print("|---|---|---|---|---|---|---|")
    for k in sorted(set(base) & set(cand), key=str):
        b, c = base[k], cand[k]
        ratio = c["p50"] / b["p50"] if b["p50"] else math.nan
        noise = (b["p90"] - b["min"]) / b["p50"] if b["p50"] else math.nan
        thresh = max(0.10, noise)
        if min(b["ok"], c["ok"]) < 3:
            verdict = "n<3, no verdict"
        else:
            verdict = "regression" if ratio > 1 + thresh else "improvement" if ratio < 1 - thresh else "within noise"
        print(f"| {k[1]} | {k[2]} | {fmt(b['p50'])} | {fmt(c['p50'])} | {fmt(ratio, 2)} | {fmt(noise, 2)} | {verdict} |")


def prom_max(run: Path, name: str, query: str, pods: set[str]) -> float | None:
    """Max of a `sum by (pod)` range series over this run's pods only (the
    query window can include the previous deployment's pods)."""
    path = run / f"{name}.prom.json"
    if not path.exists():
        return None
    series = json.loads(path.read_text())["series"].get(query) or {}
    vals = [float(v[1]) for r in series.get("result", []) if r.get("metric", {}).get("pod") in pods
            for v in r.get("values", []) if v[1] not in ("NaN", "+Inf", "-Inf")]
    return max(vals) if vals else None


def coordinator_restarts(pods_path: Path) -> tuple[int, str | None]:
    if not pods_path.exists():
        return 0, None
    for p in json.loads(pods_path.read_text()):
        if p["labels"].get("app.kubernetes.io/component") == "coordinator":
            for c in p["containers"]:
                if c["name"] == "postgres-lakesoul":
                    last = c.get("last_terminated") or {}
                    return c.get("restarts") or 0, last.get("reason")
    return 0, None


def cmd_memory(a) -> None:
    mib = 1 << 20
    print("| run | phase | ok/n | errors | coord restarts (reason) | peak cgroup MiB | limit MiB | "
          "pool limit MiB | pool reserved max MiB | reservation failures |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for run_dir in a.run_dirs:
        run = Path(run_dir)
        prev_restarts = 0
        for name in a.names.split(","):
            if not (run / f"{name}.jsonl").exists():
                print(f"| {run.name} | {name} | missing | | | | | | | |")
                continue
            rows = samples(run / f"{name}.jsonl")
            ok = sum(r.get("status") == "ok" for r in rows)
            errs: dict[str, int] = defaultdict(int)
            for r in rows:
                if r.get("status") != "ok":
                    msg = (r.get("error") or "")
                    kind = "ResourcesExhausted" if "Resources exhausted" in msg or "ResourcesExhausted" in msg else (
                        r.get("sqlstate") or r.get("status"))
                    errs[kind] += 1
            restarts_now, reason = coordinator_restarts(run / f"{name}.pods.json")
            paths = list(run.glob(f"{name}.sampler.coordinator.*.jsonl"))
            pods = {p.name[len(f"{name}.sampler.coordinator."):-len(".jsonl")] for p in paths}
            coord = [sampler_summary(p) for p in paths]
            peak = max((c.get("peak_mem_current") or 0 for c in coord), default=0)
            limit = max((c.get("mem_max") or 0 for c in coord), default=0)
            pool_limit = prom_max(run, name, "sum by (pod) (lakesoul_memory_pool_limit_bytes)", pods)
            reserved = prom_max(run, name, "sum by (pod) (lakesoul_memory_pool_reserved_bytes)", pods)
            failures = prom_max(run, name, "sum by (pod) (increase(lakesoul_memory_reservation_failures_total[15s]))", pods)
            delta = restarts_now - prev_restarts
            prev_restarts = restarts_now
            print(f"| {run.name} | {name} | {ok}/{len(rows)} | {dict(errs) or ''} | "
                  f"{delta}{f' ({reason})' if delta and reason else ''} | {peak / mib:.0f} | {limit / mib:.0f} | "
                  f"{'' if pool_limit is None else f'{pool_limit / mib:.0f}'} | "
                  f"{'' if reserved is None else f'{reserved / mib:.0f}'} | "
                  f"{'' if failures is None else f'{failures:.0f}'} |")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("summary")
    s.add_argument("run_dir")
    s.add_argument("--name")
    s.set_defaults(fn=cmd_summary)
    c = sub.add_parser("compare")
    c.add_argument("base")
    c.add_argument("cand")
    c.add_argument("--name", required=True)
    c.set_defaults(fn=cmd_compare)
    m = sub.add_parser("memory")
    m.add_argument("run_dirs", nargs="+")
    m.add_argument("--names", default="serial,burst")
    m.set_defaults(fn=cmd_memory)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
