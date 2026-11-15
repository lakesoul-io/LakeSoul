# LakeSoul 向量检索教程

本目录只保留可运行的 Notebook；概念、配置和 API 说明见
[docs/README.md](../docs/README.md)。

## Notebooks

| Notebook | 内容 |
|---|---|
| [minio_end_to_end.ipynb](minio_end_to_end.ipynb) | MinIO/S3 端到端：建表、写数据、查询、缓存配置 |
| [existing_table_vector_index.ipynb](existing_table_vector_index.ipynb) | 已有表补建向量索引、确认 S3 路径、查询 |
| [drop_vector_index.ipynb](drop_vector_index.ipynb) | 删除向量索引、删除后查询回退 |
| [native_python_batch.ipynb](native_python_batch.ipynb) | Python原生批量：Session/Table入口、L2/IP、index/exact、返回结构、基线验证与清理 |
| [native_batch_sql.ipynb](native_batch_sql.ipynb) | 一次SQL原生批量检索、query表/JSON输入、结果验证、可选pgwire与清理 |

## 前置条件

- PostgreSQL 元数据库；
- LakeSoul Python 扩展（需要 `ci-unify` feature）；
- MinIO 或本地路径；
- 可选：`psycopg` + `pgvector`（pgvector 兼容服务示例）。

环境变量与最小示例见 [docs/quickstart.md](../docs/quickstart.md)。
