# PyTorch 与 Hugging Face Datasets

机器学习适配器接收已经配置好的 `LakeSoulScan`。表选择、分区裁剪、列裁剪和过滤由核心 SDK 统一处理，不在各框架 API 中重复实现。

安装所需适配器：

```bash
pip install 'lakesoul[torch]'
pip install 'lakesoul[datasets]'
```

以下示例使用[核心 Catalog 与表 IO](02-core-api.md)中创建并写入数据的 `events` 表。

## PyTorch

`LakeSoulScan.to_torch()` 返回 `torch.utils.data.IterableDataset`。每次迭代产生 PyArrow `RecordBatch`，训练代码可以自行控制 Tensor 转换和 batching。

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

当 `torch.distributed` 已初始化且扫描没有显式分片时，适配器会采用当前 rank 和 world size。显式调用 `scan.shard(rank, world_size)` 的配置优先。

## Hugging Face Datasets

`LakeSoulScan.to_huggingface()` 返回流式 `datasets.IterableDataset`，feature schema 从 LakeSoul 扫描的 Arrow schema 推导。

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

导入 `lakesoul.huggingface` 还会注册 `datasets.IterableDataset.from_lakesoul`。这个方法接收 `LakeSoulScan`，不是表名：

```python
import datasets
import lakesoul.huggingface

scan = catalog.scan("events", partitions={"event_date": "2026-08-27"})
dataset = datasets.IterableDataset.from_lakesoul(scan)
```

新代码优先使用 `scan.to_huggingface()`，这样数据流与其他 SDK 适配器一致且更明确。

可运行的训练示例位于 [`python/examples`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples)。
