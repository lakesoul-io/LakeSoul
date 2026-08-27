# Daft

Daft 适配器将 LakeSoul 暴露为原生 Daft data source 和 sink。LakeSoul 负责解析元数据和扫描分区，Daft 负责调度分布式读写任务。

安装可选依赖：

```bash
pip install 'lakesoul[daft]'
```

以下示例使用[核心 Catalog 与表 IO](02-core-api.md)中创建并写入数据的 `events` 表。

## 读取 LakeSoul 表

`LakeSoulScan.to_daft()` 返回惰性的 `daft.DataFrame`：

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
dataframe = (
    catalog.scan(
        "events",
        partitions={"event_date": "2026-08-27"},
        columns=["id", "value"],
    )
    .to_daft()
)

result = dataframe.where(dataframe["value"] >= 50).collect()
result.show()
```

创建 Daft source 时会保留 `LakeSoulScan` 中的列裁剪和分区裁剪。后续 Daft 操作在执行 DataFrame 前仍保持惰性。

## 写入 Daft DataFrame

目标表必须已存在，schema 必须与 DataFrame 输出一致：

```python
import daft

rows = daft.from_pydict(
    {
        "id": [1, 2],
        "event_date": ["2026-08-27", "2026-08-27"],
        "value": [52.0, 81.5],
    }
)

table = catalog.table("events")
result = table.write_daft(rows)
print(result.row_count)
```

导入 `lakesoul.daft` 也会注册 `DataFrame.write_lakesoul`，该方法接收 `LakeSoulTable`：

```python
import lakesoul.daft

result = rows.write_lakesoul(table)
```

两种调用都默认使用 `vortex-compact`。需要时传入 `format="vortex"` 或 `format="parquet"`；详见[物理文件格式](../../01-Getting%20Started/05-physical-file-formats.md)。

Daft task 使用 LakeSoul 原生 Writer。分布式写入完成后，sink 才会将生成的文件提交到 LakeSoul 元数据。
