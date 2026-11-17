import os

BUCKET = "lakesoul-bench"
OPTIONS = {
    "fs.s3a.bucket": BUCKET,
    "fs.s3a.access.key": "minioadmin",
    "fs.s3a.secret.key": "minioadmin",
    "fs.s3a.endpoint": "http://127.0.0.1:9000",
    "fs.s3a.path.style.access": "true",
}
DATASETS = {
    "sift": {
        "file": "sift-128-euclidean.hdf5",
        "metric": "L2",
        "normalize": False,
        "table": "ann_sift",
        "groups": [
            (256, 1, [8, 16, 32]),
            (1024, 1, [16, 32, 64]),
            (1024, 7, [16, 32, 64]),
        ],
        "nq": 200,
    },
    "glove": {
        "file": "glove-200-angular.hdf5",
        "metric": "IP",
        "normalize": True,
        "table": "ann_glove",
        "groups": [
            (256, 1, [8, 16, 32]),
            (1024, 1, [16, 32, 64]),
            (1024, 7, [16, 32, 64]),
        ],
        "nq": 200,
    },
    "gist": {
        "file": "gist-960-euclidean.hdf5",
        "metric": "L2",
        "normalize": False,
        "table": "ann_gist",
        "groups": [
            (256, 1, [8, 16, 32]),
            (1024, 1, [16, 32, 64]),
            (1024, 7, [16, 32, 64]),
        ],
        "nq": 200,
    },
    "deep": {
        "file": "deep-image-96-angular.hdf5",
        "metric": "IP",
        "normalize": True,
        "table": "ann_deep",
        "groups": [(1024, 1, [16, 32]), (4096, 1, [16, 32]), (4096, 7, [16, 32])],
        "nq": 100,
    },
}
FIX = "/home/zhibin/ann-benchmarks"


def load_queries(name, limit=None):
    import h5py, numpy as np

    d = DATASETS[name]
    with h5py.File(f"{FIX}/{d['file']}", "r") as f:
        q = f["test"][:limit]
    if d["normalize"]:
        n = np.linalg.norm(q, axis=1, keepdims=True)
        q = np.divide(q, n, out=np.zeros_like(q), where=n > 0)
    return np.ascontiguousarray(q, dtype=np.float32)
