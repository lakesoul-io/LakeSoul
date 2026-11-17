import json, os, sys, time
import h5py, numpy as np, pyarrow as pa

sys.path.insert(0, "/tmp/annbench")
from common import *
from lakesoul import LakeSoulCatalog
from lakesoul import index as idx

name = sys.argv[1]
d = DATASETS[name]
cat = LakeSoulCatalog.from_env(object_store_options=OPTIONS)
cat.drop_table(d["table"], if_exists=True)
with h5py.File(f"{FIX}/{d['file']}", "r") as f:
    train = f["train"][:]
    dim = int(train.shape[1])
if d["normalize"]:
    n = np.linalg.norm(train, axis=1, keepdims=True)
    train = np.divide(train, n, out=np.zeros_like(train), where=n > 0)
schema = pa.schema(
    [
        pa.field("id", pa.int64(), False),
        pa.field("vec", pa.list_(pa.field("item", pa.float32()), dim), False),
    ]
)
t0 = time.perf_counter()
t = cat.create_table(
    d["table"],
    path=f"s3://{BUCKET}/{d['table']}",
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
ids = pa.array(np.arange(len(train), dtype=np.int64))
vec = pa.FixedSizeListArray.from_arrays(
    pa.array(train.reshape(-1), type=pa.float32()), dim
)
tbl = pa.table({"id": ids, "vec": vec})
del train, vec, ids
rows = len(ids)
t.write_arrow(tbl, format="vortex", auto_build_vector_index=False)
del tbl
print(
    json.dumps(
        {
            "dataset": name,
            "rows": rows,
            "dim": dim,
            "write_s": time.perf_counter() - t0,
            "table": d["table"],
        }
    ),
    flush=True,
)
# build baseline index for the first group
nlist, bits, _ = d["groups"][0]
config = idx.config_for_column(
    t._index_configs("vector"), "vec", overrides={"nlist": nlist, "total_bits": bits}
)
t1 = time.perf_counter()
print(
    idx.build_table_index(
        "vector",
        table_name=d["table"],
        namespace="default",
        configs=[config],
        store_config=idx.default_object_store_config(catalog=cat, table=t),
        rebuild=True,
    ),
    flush=True,
)
print(
    json.dumps(
        {"baseline": f"nlist={nlist},bits={bits}", "build_s": time.perf_counter() - t1}
    ),
    flush=True,
)
