import json, os, sys, time
import numpy as np

sys.path.insert(0, "/tmp/bench4")
from common import load, rss_mb, DATASETS

ds, iface, cache, rerank, nprobe, nq = (
    sys.argv[1],
    sys.argv[2],
    sys.argv[3],
    sys.argv[4],
    int(sys.argv[5]),
    int(sys.argv[6]),
)
parallelism = int(sys.argv[7]) if len(sys.argv) > 7 else 1
d = DATASETS[ds]
_, test, gt = load(ds, train=False)
test = test[:nq]
gt = gt[:nq, :100]


def recall(ids):
    return float(
        np.mean(
            [
                len(set(int(x) for x in ids[i]) & set(int(x) for x in gt[i])) / 100
                for i in range(nq)
            ]
        )
    )


def server_rss():
    pid = os.environ.get("PG_SERVICE_PID")
    if not pid:
        return None
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        return None


out = {
    "dataset": ds,
    "kind": "query",
    "interface": iface,
    "cache": cache,
    "rerank": rerank,
    "nprobe": nprobe,
    "nq": nq,
    "parallelism": parallelism,
    "server_rss_mb": server_rss(),
}
if iface in ("python", "datafusion"):
    from lakesoul import LakeSoulCatalog

    cat = LakeSoulCatalog.from_env(namespace="default")
    t = cat.table(d["table"])
    s = cat.session()
    if iface == "python":

        def one(i):
            return (
                t.search(
                    test[i].tolist(),
                    top_k=100,
                    rerank=rerank,
                    nprobe=nprobe,
                    exact_overfetch=1,
                    columns=["id"],
                    metric=d["metric"],
                    session=s,
                )
                .column("id")
                .to_pylist()
            )

        def batch():
            return s.batch_search(
                d["table"],
                test,
                column="vec",
                top_k=100,
                nprobe=nprobe,
                parallelism=parallelism,
                batch_size=nq,
                exact=(rerank == "exact"),
                exact_overfetch=1,
                metric=d["metric"],
            ).ids
    else:
        s.set_vector_search(rerank=rerank, nprobe=nprobe, exact_overfetch=1)

        def one(i):
            return (
                s.sql(t.to_sql(test[i], top_k=100, metric=d["metric"], columns=["id"]))
                .column("id")
                .to_pylist()
            )

        def batch():
            s.set_vector_search(rerank=rerank, nprobe=nprobe, exact_overfetch=1)
            return [
                s.sql(t.to_sql(q, top_k=100, metric=d["metric"], columns=["id"]))
                .column("id")
                .to_pylist()
                for q in test
            ]

    t0 = time.perf_counter()
    one(0)
    first = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    ids = [one(i) for i in range(nq)]
    elapsed = time.perf_counter() - t0
    out["single"] = {
        "first_ms": first,
        "qps": nq / elapsed,
        "mean_ms": elapsed / nq * 1000,
        "recall": recall(ids),
        "rss_mb": rss_mb(),
    }
    try:
        t0 = time.perf_counter()
        b = batch()
        first = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        rounds = 2
        for _ in range(rounds):
            b = batch()
        elapsed = time.perf_counter() - t0
        out["batch"] = {
            "first_ms": first,
            "qps": nq * rounds / elapsed,
            "mean_ms": elapsed / (nq * rounds) * 1000,
            "recall": recall(b),
            "rss_mb": rss_mb(),
        }
    except Exception as error:
        out["batch_error"] = str(error)[:400]
else:
    import psycopg

    op = "<#>" if d["metric"] == "IP" else "<->"
    conn = psycopg.connect(
        "host=127.0.0.1 port=6543 user=lakesoul_test password=lakesoul_test dbname=default",
        autocommit=True,
    )
    cur = conn.cursor()

    def one(i):
        cur.execute(f"SET lakesoul_vector_search.rerank = '{rerank}'")
        cur.execute(f"SET lakesoul_vector_search.nprobe = {nprobe}")
        lit = "[" + ",".join(str(float(x)) for x in test[i]) + "]"
        cur.execute(f"SELECT id FROM {d['table']} ORDER BY vec {op} '{lit}' LIMIT 100")
        return [int(r[0]) for r in cur.fetchall()]

    t0 = time.perf_counter()
    one(0)
    first = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    ids = [one(i) for i in range(nq)]
    elapsed = time.perf_counter() - t0
    out["single"] = {
        "first_ms": first,
        "qps": nq / elapsed,
        "mean_ms": elapsed / nq * 1000,
        "recall": recall(ids),
        "rss_mb": rss_mb(),
        "server_rss_mb": server_rss(),
    }
    t0 = time.perf_counter()
    ids = [one(i) for i in range(nq)]
    first = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    rounds = 2
    for _ in range(rounds):
        ids = [one(i) for i in range(nq)]
    elapsed = time.perf_counter() - t0
    out["batch"] = {
        "first_ms": first,
        "qps": nq * rounds / elapsed,
        "mean_ms": elapsed / (nq * rounds) * 1000,
        "recall": recall(ids),
        "rss_mb": rss_mb(),
        "server_rss_mb": server_rss(),
    }
    conn.close()
print(json.dumps(out), flush=True)
