import json, os, sys, time
import numpy as np, psycopg

sys.path.insert(0, "/tmp/annbench")
from common import *

name = sys.argv[1]
nprobe = int(sys.argv[2])
d = DATASETS[name]
NQ = d["nq"]
qs = load_queries(name, NQ)
col = d["table"]
op = "<#>" if d["metric"] == "IP" else "<->"


def rss(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            return (
                int(next(line.split()[1] for line in f if line.startswith("VmRSS:")))
                / 1024
            )
    except Exception:
        return None


pid = os.environ.get("PG_SERVICE_PID")
with psycopg.connect(
    host="127.0.0.1", port=6543, user="lakesoul_test", dbname="default", autocommit=True
) as c:
    out = {
        "dataset": name,
        "nlist": None,
        "bits": None,
        "nprobe": nprobe,
        "nq": NQ,
        "metric": d["metric"],
        "interface": "pgvector",
        "service_rss_start_mb": rss(pid),
    }

    def run(q):
        lit = "[" + ",".join(f"{float(x):.8g}" for x in q) + "]"
        return c.execute(
            f"SELECT id FROM {col} ORDER BY vec {op} '{lit}' LIMIT 100"
        ).fetchall()

    c.execute(f"SET lakesoul_vector_search.nprobe = {nprobe}")
    c.execute(f"SET lakesoul_vector_search.rerank = 'index'")
    for q in qs[:5]:
        run(q)
    t0 = time.perf_counter()
    first = run(qs[0])
    out["index_first_ms"] = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    for q in qs:
        run(q)
    out["index_qps"] = NQ / (time.perf_counter() - t0)
    out["index_service_rss_mb"] = rss(pid)
    c.execute("SET lakesoul_vector_search.rerank = 'exact'")
    for q in qs[:5]:
        run(q)
    t0 = time.perf_counter()
    first = run(qs[0])
    out["exact_first_ms"] = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    for q in qs:
        run(q)
    out["exact_qps"] = NQ / (time.perf_counter() - t0)
    out["exact_service_rss_mb"] = rss(pid)
print(json.dumps(out), flush=True)
