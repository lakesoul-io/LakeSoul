# LakeSoul Python SDK

`lakesoul` 是 LakeSoul 的 Python SDK。它用统一的 Catalog API 管理表元数据和 IO，并将同一个 LakeSoul 扫描计划转换为 PyArrow、Pandas、DuckDB、PyTorch、Hugging Face Datasets、Ray Data 或 Daft 对象。

Python SDK 适合以下场景：

- 查询、创建、加载或删除 LakeSoul 表；
- 通过分区裁剪、列裁剪和过滤条件惰性读取表；
- 写入 PyArrow 数据，并将生成的文件提交到 LakeSoul 元数据；
- 将已经配置好的扫描计划交给支持的数据处理或机器学习框架。

## 环境要求与安装

当前 Python 包要求 Python 3.10 或更高版本。

安装核心 SDK：

```bash
pip install lakesoul
```

按需安装生态适配器：

```bash
pip install 'lakesoul[pandas]'
pip install 'lakesoul[duckdb]'
pip install 'lakesoul[torch]'
pip install 'lakesoul[datasets]'
pip install 'lakesoul[ray]'
pip install 'lakesoul[daft]'
```

安装全部可选集成：

```bash
pip install 'lakesoul[all]'
```

## 配置元数据连接

先按照[本地环境搭建](../../01-Getting%20Started/01-setup-local-env.md)启动 LakeSoul 环境，再配置元数据连接：

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

`LakeSoulCatalog.from_env()` 会读取这些变量：

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
print(catalog.list_namespaces())
print(catalog.list_tables())
```

也可以显式传入 PostgreSQL 配置：

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog(
    pg_url="postgresql://localhost:5432/lakesoul_test",
    pg_username="lakesoul_test",
    pg_password="lakesoul_test",
    namespace="default",
)
```

生产环境不要在源码中写入凭据，应使用环境变量或部署平台的密钥管理机制。

## SDK 对象模型

公共 API 由三个主要对象组成：

- `LakeSoulCatalog`：元数据连接，以及 namespace 和表的入口；
- `LakeSoulTable`：已经加载的表，包含 schema、分区信息和写入操作；
- `LakeSoulScan`：不可变、惰性的读取配置，可继续组合并转换为各生态对象。

```text
LakeSoulCatalog
    | table("events") / scan("events")
    v
LakeSoulTable ---- write_arrow / write_ray / write_daft
    |
    | scan(partitions=..., columns=..., filter=...)
    v
LakeSoulScan
    |-- to_arrow_dataset / to_arrow_table / to_batches
    |-- to_torch / to_huggingface
    |-- to_ray
    `-- to_daft
```

接下来先阅读[核心 Catalog 与表 IO](02-core-api.md)，再选择应用所需的生态集成：

- [PyArrow、Pandas 与 DuckDB](03-arrow-pandas-duckdb.md)
- [PyTorch 与 Hugging Face Datasets](04-pytorch-huggingface.md)
- [Ray Data](05-ray.md)
- [Daft](06-daft.md)

Spark SQL 和 DataFrame 的使用方式请参考独立的 [Spark 配置](../02-setup-spark.md)和 [Spark API](../03-spark-api-docs.md)文档。

更多可运行示例位于 [`python/examples`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples)。
