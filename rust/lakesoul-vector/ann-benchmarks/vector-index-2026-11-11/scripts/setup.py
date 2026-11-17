import json, shutil, sys, time
import numpy as np, pyarrow as pa

sys.path.insert(0, "/tmp/bench4")
from common import load, rss_mb, DATASETS
from lakesoul import LakeSoulCatalog

ds = sys.argv[1]
d = DATASETS[ds]
root = d["root"]
shutil.rmtree(root, ignore_errors=True)
cat = LakeSoulCatalog.from_env(namespace="default")
cat.drop_table(d["table"], if_exists=True)
train, test, gt = load(ds)
n, dim = train.shape
schema = pa.schema(
    [
        pa.field("id", pa.int64(), False),
        pa.field("vec", pa.list_(pa.field("item", pa.float32()), dim), False),
    ]
)
t = cat.create_table(
    d["table"],
    path=f"file://{root}",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=4,
    vector_index=[
        {
            "column": "vec",
            "dim": dim,
            "nlist": 1024,
            "total_bits": 1,
            "metric": d["metric"],
        }
    ],
)
t0 = time.perf_counter()
t.write_arrow(
    pa.table(
        {
            "id": pa.array(np.arange(n, dtype=np.int64)),
            "vec": pa.FixedSizeListArray.from_arrays(
                pa.array(train.reshape(-1), type=pa.float32()), dim
            ),
        }
    ),
    format="vortex",
    auto_build_vector_index=False,
)
print(
    json.dumps(
        {
            "dataset": ds,
            "kind": "setup",
            "rows": n,
            "dim": dim,
            "write_s": time.perf_counter() - t0,
            "write_rss_mb": rss_mb(),
        }
    ),
    flush=True,
)
