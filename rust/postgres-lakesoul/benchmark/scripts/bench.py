#!/usr/bin/env python3
# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Benchmark orchestrator: deploys a profile, runs driver/datagen Jobs and
collects raw evidence (driver samples, sampler sidecar logs, Prometheus
series, pod status) into a run directory. Talks to the cluster only through
`kubectl` with the current context. Standard library only.

  bench.py meta    --out results/<run>/env.json
  bench.py deploy  --run-dir D --profile small --service-tag T --bench-tag B \
                   [--env all:K=V] [--env coordinator:K=V] [--env worker:K=V] [--workers N]
  bench.py datagen --run-dir D --bench-tag B --sf 1 --location s3://bucket/bench
  bench.py drive   --run-dir D --bench-tag B --name NAME -- <lsbench args>
  bench.py collect --run-dir D --name NAME --since UNIX --until UNIX
  bench.py exec-cancel ... (see --help)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import string
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

NS = "lakesoul"
REGISTRY = "swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo"
HERE = Path(__file__).resolve().parent
BENCH = HERE.parent
K8S = BENCH / "k8s"
PROM = f"/api/v1/namespaces/{NS}/services/lakesoul-prometheus:9090/proxy"
CONTAINERS = {"coordinator": "postgres-lakesoul", "worker": "lakesoul-worker"}
DEPLOYMENTS = {"coordinator": "lakesoul-coordinator", "worker": "lakesoul-worker"}


def log(msg: str) -> None:
    print(f"[bench {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


TRANSIENT = ("TLS handshake timeout", "connection refused", "i/o timeout", "EOF", "context deadline exceeded")


def kubectl(*args: str, input: str | None = None, check: bool = True, timeout: float = 120) -> str:
    """Runs kubectl; retries API-server transport failures (the shared cluster's
    API endpoint drops TLS handshakes under load) up to three times."""
    cmd = ["kubectl", "--request-timeout=60s", *args]
    for attempt in range(3):
        r = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
        if r.returncode == 0 or not any(t in r.stderr for t in TRANSIENT):
            break
        log(f"kubectl transient failure (attempt {attempt + 1}): {r.stderr.strip()[:200]}")
        time.sleep(5 * (attempt + 1))
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {r.stderr.strip()}")
    return r.stdout


def kjson(*args: str) -> dict:
    return json.loads(kubectl(*args, "-o", "json"))


def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, sort_keys=True) + "\n")


# ---------------------------------------------------------------- metadata


def pods_snapshot() -> list[dict]:
    pods = kjson("-n", NS, "get", "pods")["items"]
    out = []
    for p in pods:
        st = p.get("status", {})
        cs = []
        for c in st.get("containerStatuses", []) or []:
            last = (c.get("lastState") or {}).get("terminated")
            cur = c.get("state") or {}
            cs.append(
                {
                    "name": c["name"],
                    "image": c.get("image"),
                    "image_id": c.get("imageID"),
                    "ready": c.get("ready"),
                    "restarts": c.get("restartCount"),
                    "state": next(iter(cur), None),
                    "last_terminated": last
                    and {k: last.get(k) for k in ("reason", "exitCode", "startedAt", "finishedAt")},
                }
            )
        res = {
            c["name"]: c.get("resources", {}) for c in p["spec"].get("containers", [])
        }
        out.append(
            {
                "name": p["metadata"]["name"],
                "labels": p["metadata"].get("labels", {}),
                "node": p["spec"].get("nodeName"),
                "phase": st.get("phase"),
                "qos": st.get("qosClass"),
                "pod_ip": st.get("podIP"),
                "start": st.get("startTime"),
                "resources": res,
                "containers": cs,
            }
        )
    return out


def node_snapshot() -> list[dict]:
    nodes = kjson("get", "nodes")["items"]
    desc = kubectl("describe", "nodes")
    alloc = {}
    for block in re.split(r"^Name:", desc, flags=re.M)[1:]:
        m = re.search(r"^\s*(\S+)", block)
        if not m:
            continue
        name = m.group(1)
        cpu = re.search(r"^\s+cpu\s+(\S+) \((\d+)%\)\s+(\S+) \((\d+)%\)", block, re.M)
        mem = re.search(r"^\s+memory\s+(\S+) \((\d+)%\)\s+(\S+) \((\d+)%\)", block, re.M)
        if cpu and mem:
            alloc[name] = {
                "cpu_requests": cpu.group(1),
                "cpu_limits": cpu.group(3),
                "memory_requests": mem.group(1),
                "memory_limits": mem.group(3),
            }
    try:
        top = kubectl("top", "nodes", "--no-headers", check=False)
    except subprocess.TimeoutExpired:
        top = ""
    usage = {}
    for line in top.splitlines():
        f = line.split()
        if len(f) >= 5:
            usage[f[0]] = {"cpu": f[1], "cpu_pct": f[2], "memory": f[3], "memory_pct": f[4]}
    out = []
    for n in nodes:
        name = n["metadata"]["name"]
        info = n["status"]["nodeInfo"]
        lab = n["metadata"].get("labels", {})
        out.append(
            {
                "name": name,
                "instance_type": lab.get("node.kubernetes.io/instance-type"),
                "zone": lab.get("topology.kubernetes.io/zone"),
                "arch": info.get("architecture"),
                "kernel": info.get("kernelVersion"),
                "os_image": info.get("osImage"),
                "container_runtime": info.get("containerRuntimeVersion"),
                "kubelet": info.get("kubeletVersion"),
                "capacity": n["status"].get("capacity"),
                "allocatable": n["status"].get("allocatable"),
                "allocated": alloc.get(name),
                "usage": usage.get(name),
                "conditions": {
                    c["type"]: c["status"] for c in n["status"].get("conditions", [])
                },
            }
        )
    return out


def cmd_meta(a) -> None:
    ver = json.loads(kubectl("version", "-o", "json"))
    sc = kjson("get", "storageclass")["items"]
    ds = kjson("-n", "kube-system", "get", "daemonsets")["items"]
    git = subprocess.run(
        ["jj", "log", "-r", "@|@-", "--no-graph", "-T",
         'change_id.short() ++ " " ++ commit_id.short() ++ " " ++ description.first_line() ++ "\\n"'],
        capture_output=True, text=True,
    ).stdout
    meta = {
        "captured_unix": time.time(),
        "context": kubectl("config", "current-context").strip(),
        "kubernetes": ver.get("serverVersion"),
        "kubectl": ver.get("clientVersion", {}).get("gitVersion"),
        "nodes": node_snapshot(),
        "storage_classes": [
            {"name": s["metadata"]["name"], "provisioner": s.get("provisioner"),
             "default": s["metadata"].get("annotations", {}).get("storageclass.kubernetes.io/is-default-class")}
            for s in sc
        ],
        "kube_system_daemonsets": [d["metadata"]["name"] for d in ds],
        "pods": pods_snapshot(),
        "jj": git.strip().splitlines(),
    }
    write_json(Path(a.out), meta)
    log(f"metadata -> {a.out}")


# ---------------------------------------------------------------- deploy


def env_patch(dep: str, container: str, env: dict[str, str]) -> str:
    items = "".join(
        f"            - name: {k}\n              value: {json.dumps(v)}\n" for k, v in env.items()
    )
    return (
        f"apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {dep}\n  namespace: {NS}\n"
        f"spec:\n  template:\n    spec:\n      containers:\n        - name: {container}\n"
        f"          env:\n{items}"
    )


def cmd_deploy(a) -> None:
    run = Path(a.run_dir)
    kdir = run / "k8s"
    kdir.mkdir(parents=True, exist_ok=True)
    envs: dict[str, dict[str, str]] = {"coordinator": {}, "worker": {}}
    for spec in a.env or []:
        role, kv = spec.split(":", 1)
        k, v = kv.split("=", 1)
        for r in envs if role == "all" else [role]:
            envs[r][k] = v
    # Every deploy gets a nonce so pods restart (fresh process, cold caches).
    for r in envs:
        envs[r]["LSBENCH_DEPLOY"] = run.name + "-" + str(int(time.time()))
    patches = []
    for role, env in envs.items():
        f = kdir / f"env-{role}.yaml"
        f.write_text(env_patch(DEPLOYMENTS[role], CONTAINERS[role], env))
        patches.append(f"  - path: {f.name}\n")
    replicas = ""
    if a.workers is not None:
        replicas = f"replicas:\n  - name: lakesoul-worker\n    count: {a.workers}\n"
    profile = (K8S / "profiles" / a.profile).resolve()
    (kdir / "kustomization.yaml").write_text(
        "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n"
        f"resources:\n  - {os.path.relpath(profile, kdir.resolve())}\n"
        "images:\n"
        f"  - name: {REGISTRY}/lakesoul-k8s\n    newTag: {a.service_tag}\n"
        f"  - name: {REGISTRY}/lakesoul-bench\n    newTag: {a.bench_tag}\n"
        f"patches:\n{''.join(patches)}{replicas}"
    )
    rendered = kubectl("kustomize", str(kdir))
    unpinned = sorted(set(re.findall(r"image: (\S+:dev)\b", rendered)))
    if unpinned:
        raise SystemExit(f"rendered manifests still reference unpinned images: {unpinned}")
    (kdir / "rendered.yaml").write_text(rendered)
    log(f"applying profile={a.profile} service={a.service_tag} bench={a.bench_tag}")
    kubectl("apply", "-f", "-", input=rendered, timeout=180)
    for d in ("lakesoul-coordinator", "lakesoul-worker", "lakesoul-prometheus", "lakesoul-tempo"):
        kubectl("-n", NS, "rollout", "status", f"deploy/{d}", "--timeout=600s", timeout=660)
    wait_terminated_pods()
    wait_worker_discovery(a.workers)
    snap = pods_snapshot()
    limits = {}
    for p in snap:
        comp = p["labels"].get("app.kubernetes.io/component")
        if comp in CONTAINERS:
            limits[p["name"]] = first_sampler_line(p["name"])
    write_json(
        run / "deploy.json",
        {
            "profile": a.profile,
            "service_tag": a.service_tag,
            "bench_tag": a.bench_tag,
            "env": envs,
            "workers": a.workers,
            "deployed_unix": time.time(),
            "pods": snap,
            "effective_cgroup": limits,
        },
    )
    log(f"deployed; cgroup: { {k: (v or {}).get('mem_max') for k, v in limits.items()} }")


def wait_worker_discovery(workers: int | None, timeout: float = 180) -> None:
    """The coordinator can start before its workers are Ready; until its next
    EndpointSlice refresh it rejects distributed plans ("found no eligible
    worker") instead of planning locally. Wait for a refresh that sees every
    worker so measurements never include that startup window."""
    want = workers if workers is not None else int(
        kubectl("-n", NS, "get", "deploy/lakesoul-worker", "-o", "jsonpath={.spec.replicas}") or 0)
    if want == 0:
        return
    pattern = re.compile(r"EndpointSlice refresh ok .*eligible=(\d+)")
    end = time.time() + timeout
    while time.time() < end:
        logs = kubectl("-n", NS, "logs", "deploy/lakesoul-coordinator", "-c", "postgres-lakesoul",
                       "--since=60s", check=False)
        seen = [int(m.group(1)) for m in pattern.finditer(logs)]
        if seen and seen[-1] >= want:
            log(f"coordinator discovery sees {seen[-1]} eligible workers")
            return
        time.sleep(5)
    raise RuntimeError(f"coordinator never reported {want} eligible workers within {timeout}s")


def wait_terminated_pods(timeout: float = 300) -> None:
    """Rollout status returns before old pods are gone; wait for them."""
    end = time.time() + timeout
    while time.time() < end:
        pods = kjson("-n", NS, "get", "pods", "-l", "app.kubernetes.io/name=lakesoul")["items"]
        if all(not p["metadata"].get("deletionTimestamp") for p in pods):
            return
        time.sleep(2)


def first_sampler_line(pod: str, tries: int = 30) -> dict | None:
    for _ in range(tries):
        out = kubectl("-n", NS, "logs", pod, "-c", "sampler", "--tail=1", check=False)
        for line in out.splitlines():
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if d.get("pid"):
                return {k: d.get(k) for k in ("cgroup", "mem_max", "cfs_quota_us", "cfs_period_us", "mem_current", "vmrss_kb")}
        time.sleep(2)
    return None


# ---------------------------------------------------------------- jobs


def render(template: str, **vals: str) -> str:
    # safe_substitute leaves shell `$?` alone; unfilled `${UPPER}` variables
    # are still an error.
    out = string.Template((K8S / "jobs" / template).read_text()).safe_substitute(**vals)
    missing = sorted(set(re.findall(r"\$\{([A-Z_]+)\}", out)))
    if missing:
        raise SystemExit(f"{template}: unset template variables {missing}")
    return out


def job_pod(job: str, timeout: float = 600) -> str:
    end = time.time() + timeout
    while time.time() < end:
        items = kjson("-n", NS, "get", "pods", "-l", f"job-name={job}")["items"]
        for p in items:
            if p["status"].get("phase") in ("Running", "Succeeded", "Failed"):
                return p["metadata"]["name"]
        time.sleep(2)
    raise RuntimeError(f"job {job}: no running pod after {timeout}s")


def cmd_datagen(a) -> None:
    run = Path(a.run_dir)
    job = f"lsbench-datagen-sf{str(a.sf).replace('.', '-')}-{int(time.time())}"
    manifest = render(
        "datagen-job.yaml",
        JOB_NAME=job,
        RUN_ID=run.name,
        BENCH_IMAGE=f"{REGISTRY}/lakesoul-bench:{a.bench_tag}",
        SF=str(a.sf),
        LOCATION=a.location,
        PARTS=a.parts,
        BUCKETS=a.buckets,
        TABLES=a.tables,
        CHUNKS=str(a.chunks),
        LAYOUT=a.layout,
        DATAGEN_THREADS=str(a.threads),
        DATAGEN_CPU_REQ=a.cpu_req,
        DATAGEN_CPU_LIM=a.cpu_lim,
        DATAGEN_MEM_REQ=a.mem_req,
        DATAGEN_MEM_LIM=a.mem_lim,
    )
    (run / "jobs").mkdir(parents=True, exist_ok=True)
    (run / "jobs" / f"{job}.yaml").write_text(manifest)
    t0 = time.time()
    kubectl("apply", "-f", "-", input=manifest)
    pod = job_pod(job)
    log(f"datagen pod {pod}")
    logf = run / f"datagen-sf{a.sf}.log"
    with logf.open("w") as f:
        subprocess.run(["kubectl", "-n", NS, "logs", "-f", pod], stdout=f, stderr=subprocess.STDOUT)
    # `logs -f` returns when the container exits, before the Job controller
    # records the outcome.
    for _ in range(60):
        status = kjson("-n", NS, "get", "job", job)["status"]
        if status.get("succeeded") or status.get("failed"):
            break
        time.sleep(2)
    pod_info = kjson("-n", NS, "get", "pod", pod)
    write_json(
        run / f"datagen-sf{a.sf}.json",
        {"job": job, "start_unix": t0, "end_unix": time.time(), "job_status": status,
         "node": pod_info["spec"].get("nodeName"),
         "container": pod_info["status"].get("containerStatuses")},
    )
    ok = bool(status.get("succeeded"))
    log(f"datagen {'ok' if ok else 'FAILED'} in {time.time() - t0:.0f}s -> {logf}")
    # Logs and status are saved; never leave a failed Job behind.
    kubectl("-n", NS, "delete", "job", job, "--wait=false")
    if not ok:
        sys.exit(1)


def cmd_drive(a) -> None:
    run = Path(a.run_dir)
    args = list(a.driver_args)
    if args and args[0] == "--":
        args = args[1:]
    name = a.name
    job = f"lsbench-{re.sub(r'[^a-z0-9-]', '-', name.lower())[:40]}-{int(time.time()) % 100000}"
    driver_args = " ".join(shlex_quote(x) for x in args + ["--run-id", f"{run.name}/{name}"])
    manifest = render(
        "driver-job.yaml",
        JOB_NAME=job,
        RUN_ID=run.name,
        BENCH_IMAGE=f"{REGISTRY}/lakesoul-bench:{a.bench_tag}",
        DRIVER_ARGS=driver_args,
        DRIVER_CPU_REQ=a.cpu_req,
        DRIVER_CPU_LIM=a.cpu_lim,
    )
    (run / "jobs").mkdir(parents=True, exist_ok=True)
    (run / "jobs" / f"{job}.yaml").write_text(manifest)
    kubectl("apply", "-f", "-", input=manifest)
    pod = job_pod(job)
    log(f"{name}: driver pod {pod}")
    t0 = time.time()
    deadline = t0 + a.timeout
    while True:
        out = kubectl("-n", NS, "logs", pod, "--tail=5", check=False)
        if "LSBENCH_DONE" in out:
            break
        phase = kjson("-n", NS, "get", "pod", pod)["status"].get("phase")
        if phase in ("Failed", "Succeeded"):
            break
        if time.time() > deadline:
            log(f"{name}: driver timeout after {a.timeout}s")
            break
        time.sleep(3)
    t1 = time.time()
    raw = kubectl("-n", NS, "exec", pod, "-c", "driver", "--", "cat", "/results/out.jsonl", check=False, timeout=600)
    rc = kubectl("-n", NS, "exec", pod, "-c", "driver", "--", "cat", "/results/exit", check=False).strip()
    cpu = kubectl("-n", NS, "exec", pod, "-c", "driver", "--", "sh", "-c",
                  "cat /sys/fs/cgroup/cpu,cpuacct/cpu.stat /sys/fs/cgroup/cpu,cpuacct/cpuacct.usage 2>/dev/null || cat /sys/fs/cgroup/cpu.stat",
                  check=False)
    stdout = kubectl("-n", NS, "logs", pod, check=False)
    (run / f"{name}.jsonl").write_text(raw)
    (run / f"{name}.driver.log").write_text(stdout)
    node = kjson("-n", NS, "get", "pod", pod)["spec"].get("nodeName")
    write_json(run / f"{name}.driver.json", {
        "job": job, "pod": pod, "node": node, "args": args, "exit": rc,
        "start_unix": t0, "end_unix": t1, "driver_cgroup_cpu": cpu,
    })
    kubectl("-n", NS, "delete", "job", job, "--wait=false", check=False)
    collect(run, name, t0 - 5, t1 + 5)
    log(f"{name}: exit={rc} samples={raw.count(chr(10))} -> {run / name}.jsonl")


def shlex_quote(s: str) -> str:
    import shlex

    return shlex.quote(s)


# ---------------------------------------------------------------- collect

# Range windows are 4x the 15 s scrape interval (k8s/observability/config/
# prometheus.yml); a [15s] window holds at most one sample, so rate() and
# increase() over it return nothing.
# report.py looks series up by their exact query text: it imports these.
PROM_MEMORY_POOL_RESERVED = 'sum by (pod) (lakesoul_memory_pool_reserved_bytes)'
PROM_MEMORY_POOL_LIMIT = 'sum by (pod) (lakesoul_memory_pool_limit_bytes)'
PROM_MEMORY_RESERVATION_FAILURES = 'sum by (pod) (increase(lakesoul_memory_reservation_failures_total[1m]))'
PROM_SERIES = [
    'sum by (pod, outcome) (rate(lakesoul_query_requests_total[1m]))',
    'histogram_quantile(0.99, sum by (le) (rate(lakesoul_query_duration_seconds_bucket[1m])))',
    'histogram_quantile(0.5, sum by (le) (rate(lakesoul_query_duration_seconds_bucket[1m])))',
    PROM_MEMORY_POOL_RESERVED,
    PROM_MEMORY_POOL_LIMIT,
    PROM_MEMORY_RESERVATION_FAILURES,
    'sum by (pod, operation) (rate(lakesoul_object_store_requests_total[1m]))',
    'sum by (pod, operation) (rate(lakesoul_object_store_bytes_total[1m]))',
    'sum by (pod) (lakesoul_metadata_pool_connections)',
    'sum by (pod, span_name) (rate(traces_spanmetrics_calls_total[1m]))',
    'up',
]


def prom_range(q: str, start: float, end: float, step: int) -> dict:
    qs = urllib.parse.urlencode({"query": q, "start": f"{start:.0f}", "end": f"{end:.0f}", "step": str(step)})
    raw = kubectl("get", "--raw", f"{PROM}/api/v1/query_range?{qs}", check=False)
    try:
        return json.loads(raw).get("data", {})
    except ValueError:
        return {"error": raw[:500]}


def collect(run: Path, name: str, since: float, until: float) -> None:
    out = {}
    pods = pods_snapshot()
    for p in pods:
        comp = p["labels"].get("app.kubernetes.io/component")
        if comp not in CONTAINERS:
            continue
        since_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since))
        logs = kubectl("-n", NS, "logs", p["name"], "-c", "sampler", f"--since-time={since_iso}", check=False, timeout=300)
        lines = []
        for line in logs.splitlines():
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if since <= d.get("ts", 0) <= until:
                lines.append(line)
        (run / f"{name}.sampler.{comp}.{p['name']}.jsonl").write_text("\n".join(lines) + "\n")
        app = kubectl("-n", NS, "logs", p["name"], "-c", CONTAINERS[comp], f"--since-time={since_iso}", check=False, timeout=300)
        (run / f"{name}.app.{comp}.{p['name']}.log").write_text(app)
    step = max(5, int((until - since) / 1000) + 1)
    for q in PROM_SERIES:
        out[q] = prom_range(q, since, until, step)
    write_json(run / f"{name}.prom.json", {"since": since, "until": until, "step": step, "series": out})
    write_json(run / f"{name}.pods.json", pods)


def cmd_collect(a) -> None:
    collect(Path(a.run_dir), a.name, a.since, a.until)


# ---------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("meta")
    m.add_argument("--out", required=True)
    m.set_defaults(fn=cmd_meta)
    d = sub.add_parser("deploy")
    d.add_argument("--run-dir", required=True)
    d.add_argument("--profile", required=True, choices=sorted(p.name for p in (K8S / "profiles").iterdir()))
    d.add_argument("--service-tag", required=True)
    d.add_argument("--bench-tag", required=True)
    d.add_argument("--env", action="append", help="ROLE:KEY=VALUE, ROLE in all|coordinator|worker")
    d.add_argument("--workers", type=int)
    d.set_defaults(fn=cmd_deploy)
    g = sub.add_parser("datagen")
    g.add_argument("--run-dir", required=True)
    g.add_argument("--bench-tag", required=True)
    g.add_argument("--sf", required=True)
    g.add_argument("--location", required=True)
    g.add_argument("--parts", default="", help="tpchgen partitions (default: tpch.sh)")
    g.add_argument("--buckets", default="", help="hashBucketNum (default: tpch.sh)")
    g.add_argument("--tables", default="", help="space-separated subset (default: all)")
    g.add_argument("--chunks", type=int, default=1, help="upserts per table (datagen/tpch.sh CHUNKS)")
    g.add_argument("--layout", default="pk", choices=["pk", "pkmonth", "nopk"])
    g.add_argument("--threads", type=int, default=4)
    g.add_argument("--cpu-req", default="1")
    g.add_argument("--cpu-lim", default="4")
    g.add_argument("--mem-req", default="1Gi")
    g.add_argument("--mem-lim", default="6Gi")
    g.set_defaults(fn=cmd_datagen)
    r = sub.add_parser("drive")
    r.add_argument("--run-dir", required=True)
    r.add_argument("--bench-tag", required=True)
    r.add_argument("--name", required=True)
    r.add_argument("--timeout", type=float, default=3600)
    r.add_argument("--cpu-req", default="250m")
    r.add_argument("--cpu-lim", default="2")
    r.add_argument("driver_args", nargs=argparse.REMAINDER)
    r.set_defaults(fn=cmd_drive)
    c = sub.add_parser("collect")
    c.add_argument("--run-dir", required=True)
    c.add_argument("--name", required=True)
    c.add_argument("--since", type=float, required=True)
    c.add_argument("--until", type=float, required=True)
    c.set_defaults(fn=cmd_collect)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
