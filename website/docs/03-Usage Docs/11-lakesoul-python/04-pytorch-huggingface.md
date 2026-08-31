# PyTorch and Hugging Face Datasets

The machine-learning adapters consume a configured `LakeSoulScan`. Table selection, partition pruning, projection, and filtering therefore remain in the core SDK instead of being duplicated in each framework API.

Install the adapter required by the application:

```bash
pip install 'lakesoul[torch]'
pip install 'lakesoul[datasets]'
```

The examples below use the `events` table created and populated in [Core catalog and table IO](02-core-api.md).

## PyTorch

`LakeSoulScan.to_torch()` returns a `torch.utils.data.IterableDataset`. Each iteration yields PyArrow `RecordBatch` objects, allowing the training pipeline to control tensor conversion and batching.

```python
import torch

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
dataset = (
    catalog.scan(
        "events",
        partitions={"event_date": "2026-08-27"},
        columns=["id", "value"],
    )
    .to_torch()
)

for record_batch in dataset:
    features = torch.tensor(
        record_batch["value"].to_pylist(),
        dtype=torch.float32,
    ).unsqueeze(1)
    labels = torch.tensor(record_batch["id"].to_pylist())
    print(features.shape, labels.shape)
```

When `torch.distributed` is initialized and the scan has no explicit shard, the adapter uses the current distributed rank and world size. An explicit `scan.shard(rank, world_size)` takes precedence.

## Hugging Face Datasets

`LakeSoulScan.to_huggingface()` returns a streaming `datasets.IterableDataset`. Its feature schema is inferred from the LakeSoul scan's Arrow schema.

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
dataset = (
    catalog.scan(
        "events",
        partitions={"event_date": "2026-08-27"},
        columns=["id", "value"],
    )
    .to_huggingface()
)

for example in dataset:
    print(example["id"], example["value"])
```

Importing `lakesoul.huggingface` also registers `datasets.IterableDataset.from_lakesoul`, but the registered method accepts a `LakeSoulScan`, not a table name:

```python
import datasets
import lakesoul.huggingface

scan = catalog.scan("events", partitions={"event_date": "2026-08-27"})
dataset = datasets.IterableDataset.from_lakesoul(scan)
```

Prefer `scan.to_huggingface()` in new code because the data flow is explicit and consistent with the other SDK adapters.

Runnable training examples are available under [`python/examples`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples).
