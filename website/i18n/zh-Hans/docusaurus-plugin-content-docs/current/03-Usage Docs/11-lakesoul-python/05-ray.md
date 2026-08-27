# Ray Data

Ray 适配器将 LakeSoul 扫描分区映射为 Ray Data 读取任务，并通过 LakeSoul 原生 Writer 写入 Ray block。

安装可选依赖：

```bash
pip install 'lakesoul[ray]'
```

以下示例使用[核心 Catalog 与表 IO](02-core-api.md)中创建并写入数据的 `events` 表。

## 读取 LakeSoul 表

`LakeSoulScan.to_ray()` 返回 `ray.data.Dataset`：

```python
import ray

from lakesoul import LakeSoulCatalog

ray.init()
catalog = LakeSoulCatalog.from_env()
ray_dataset = (
    catalog.scan(
        "events",
        partitions={"event_date": "2026-08-27"},
        columns=["id", "value"],
    )
    .to_ray()
)

print(ray_dataset.count())
for batch in ray_dataset.iter_batches(batch_format="pyarrow"):
    print(batch.to_pydict())
```

LakeSoul 先解析表快照和 Merge-on-Read 计划，每个非空 LakeSoul 扫描分区对应一个 Ray 读取任务。

导入 `lakesoul.ray` 还会注册 `ray.data.read_lakesoul`，该函数接收 `LakeSoulScan`：

```python
import ray.data
import lakesoul.ray

scan = catalog.scan("events")
ray_dataset = ray.data.read_lakesoul(scan)
```

新代码优先使用 `scan.to_ray()`，以保持 SDK 适配器的一致性。

## 写入 Ray Dataset

目标 LakeSoul 表必须已存在，schema 必须与 Ray Dataset 一致：

```python
import pyarrow as pa
import ray.data

table = catalog.table("events")
arrow_rows = pa.table(
    {
        "id": [1, 2],
        "event_date": ["2026-08-27", "2026-08-27"],
        "value": [52.0, 81.5],
    },
    schema=table.schema,
)
rows = ray.data.from_arrow(arrow_rows)
table.write_ray(rows)
```

导入 `lakesoul.ray` 也会注册 `Dataset.write_lakesoul`：

```python
import lakesoul.ray

rows.write_lakesoul(table)
```

两种调用都默认使用 `vortex-compact`。需要时传入 `format="vortex"` 或 `format="parquet"`；详见[物理文件格式](../../01-Getting%20Started/05-physical-file-formats.md)。

Ray task 负责写数据文件。所有 task 成功完成后，driver 才会将文件提交到 LakeSoul 元数据。
