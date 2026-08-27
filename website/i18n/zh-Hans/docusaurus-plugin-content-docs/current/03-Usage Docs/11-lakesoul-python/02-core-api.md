# 核心 Catalog 与表 IO

`LakeSoulCatalog` 是 Python SDK 的主入口。它持有元数据连接，并返回 `LakeSoulTable` 和 `LakeSoulScan` 对象。

## 加载表

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")
table = catalog.table("events")

print(table.name)
print(table.schema)
print(table.partition_by)
print(table.primary_keys)
```

表不存在时，`catalog.table()` 会抛出 `TableNotFoundError`。可以用 `catalog.list_tables()` 查询已有表。

## 创建表

创建表需要 PyArrow schema 和存储路径。分区列和主键列必须存在于 schema 中。

```python
import pyarrow as pa

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_date", pa.string()),
        pa.field("value", pa.float64()),
    ]
)

table = catalog.create_table(
    "events",
    path="file:///tmp/lakesoul/events",
    schema=schema,
    partition_by=("event_date",),
    primary_keys=("id",),
    hash_bucket_num=4,
)
```

本地开发时，`path` 也可以是本地路径。

## 构建惰性扫描

扫描对象记录分区裁剪、列裁剪、过滤、批大小和可选的分布式分片配置；只有转换或迭代时才会读取数据。

```python
import pyarrow.compute as pc

scan = (
    catalog.scan("events", partitions={"event_date": "2026-08-27"})
    .select("id", "value")
    .filter(pc.field("value") >= 50)
    .options(batch_size=4096, thread_count=4)
)

for batch in scan.to_batches():
    print(batch.to_pydict())
```

这些参数也可以直接传给 `catalog.scan()` 或 `table.scan()`。调用方自行分配分布式 rank 时，使用 `scan.shard(rank, world_size)`。

`LakeSoulScan` 是不可变对象：`select()`、`filter()`、`with_partitions()`、`shard()` 和 `options()` 都返回新扫描对象。

## 读取为 PyArrow

根据预期数据量选择物化方式：

```python
arrow_dataset = scan.to_arrow_dataset()  # 惰性 Dataset
reader = scan.to_reader()                # 流式 RecordBatchReader
batches = scan.to_batches()              # RecordBatch 迭代器
arrow_table = scan.to_arrow_table()       # 将全部结果载入内存
```

大结果集优先使用 `to_reader()` 或 `to_batches()`。

## 写入 PyArrow 数据

`LakeSoulTable.write_arrow()` 接受 `pyarrow.RecordBatch`、`pyarrow.Table` 或 `pyarrow.RecordBatchReader`。它写入数据文件并提交 LakeSoul 元数据后才返回。

```python
import pyarrow as pa

rows = pa.table(
    {
        "id": [1, 2],
        "event_date": ["2026-08-27", "2026-08-27"],
        "value": [52.0, 81.5],
    },
    schema=table.schema,
)

result = table.write_arrow(rows)
print(result.row_count)
print(result.files)
```

默认物理格式是 `vortex-compact`。传入 `format="vortex"` 或 `format="parquet"` 可以选择其他策略。格式取舍、混合格式行为和升级边界参见[物理文件格式](../../01-Getting%20Started/05-physical-file-formats.md)。

## 对象存储配置

对象存储参数可以配置在 Catalog 上，也可以针对单次扫描或写入覆盖：

```python
catalog = LakeSoulCatalog.from_env(
    object_store_options={
        "fs.s3a.endpoint": "http://localhost:9000",
        "fs.s3a.access.key": "rustfsadmin",
        "fs.s3a.secret.key": "rustfsadmin",
        "fs.s3a.path.style.access": "true",
    }
)

scan = catalog.scan(
    "events",
    object_store_options={"fs.s3a.endpoint": "http://rustfs:9000"},
)
```

单次操作的参数优先级高于 Catalog 参数。

## 删除表

```python
table.drop()

# 只忽略表不存在错误
catalog.drop_table("events", if_exists=True)
```
