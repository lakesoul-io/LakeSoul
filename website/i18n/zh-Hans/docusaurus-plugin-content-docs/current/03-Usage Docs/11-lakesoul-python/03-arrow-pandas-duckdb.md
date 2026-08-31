# PyArrow、Pandas 与 DuckDB

PyArrow 是 SDK 的通用数据交换层。同一个 `LakeSoulScan` 可以流式输出 Arrow batch、为 Pandas 物化表，或向 DuckDB 暴露 Arrow Dataset。

安装可选依赖：

```bash
pip install 'lakesoul[pandas,duckdb]'
```

以下示例使用[核心 Catalog 与表 IO](02-core-api.md)中创建并写入数据的 `events` 表。

## PyArrow Dataset

```python
import pyarrow.compute as pc

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
scan = catalog.scan(
    "events",
    partitions={"event_date": "2026-08-27"},
    columns=["id", "value"],
    filter=pc.field("value") >= 50,
)
dataset = scan.to_arrow_dataset()

for batch in dataset.to_batches():
    print(batch.to_pydict())
```

扫描计划会将分区裁剪下推到 LakeSoul，Arrow scanner 则处理列裁剪和支持的 PyArrow 过滤表达式。

也可以在返回的 Dataset 上配置列和过滤条件：

```python
scanner = dataset.scanner(
    columns=["id"],
    filter=pc.field("value") >= 50,
)
table = scanner.to_table()
```

## Pandas

仅当结果可以放入内存时再执行转换：

```python
dataframe = scan.to_arrow_table().to_pandas()
```

处理大表时，应迭代 `scan.to_batches()`，不要为整张表创建一个 Pandas DataFrame。

## DuckDB

DuckDB 可以查询 LakeSoul 返回的 PyArrow Dataset：

```python
import duckdb

lake_dataset = scan.to_arrow_dataset()
connection = duckdb.connect()
result = connection.sql(
    "SELECT id, value FROM lake_dataset WHERE value >= 75"
)

print(result.fetchall())
```

DuckDB 从 Python 作用域解析 `lake_dataset`，执行查询期间需要保留该变量。

这里使用的是 DuckDB 的 Arrow Dataset 能力，而不是专用的 LakeSoul DuckDB Connector。LakeSoul 负责解析表快照并执行 Merge-on-Read，DuckDB 负责在结果 Arrow Dataset 上执行 SQL。
