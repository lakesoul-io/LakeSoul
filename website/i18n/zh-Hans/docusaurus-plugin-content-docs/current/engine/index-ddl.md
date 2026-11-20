---
sidebar_position: 3
sidebar_label: 向量与文本索引 DDL
---

# 向量与文本索引 DDL

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 的向量索引和文本索引都声明在表上，作为派生对象维护。PostgreSQL
端点把这套声明暴露为普通 DDL，因此可以直接在 `psql` 或 PostgreSQL 驱动里
建表、建索引和查询。Python 和 Spark 使用同一组表属性；对应 API 见
[向量检索 API](vector-search/api.md) 和[全文检索](full-text-search/overview.md)。

## 建表

向量索引需要主键和向量列；文本索引需要主键和文本列。可以在
`CREATE TABLE` 时一起声明：

```sql
CREATE TABLE docs (
  id        bigint primary key,
  body      text,
  embedding vector(8)
) WITH (
  vector_index_columns = '[{"column":"embedding","dim":8}]',
  text_index_columns   = '[{"column":"body"}]'
);
```

`vector(n)` 存储为带 `pg.vector` 标记的 `FixedSizeList(Float32, n)`；
裸 `vector` 是变长 `List(Float32)`。向量索引需要维度，所以使用
`vector(n)`，或在声明/属性里提供 `dim`。`vector_index_columns` 和
`text_index_columns` 都是 JSON 数组，可以一次声明多列。

同一组属性也被其他 LakeSoul 引擎识别。Python 通过 `vector_index=` /
`text_index=` 和 `properties` 写入，DataFusion SQL 通过
`vector_index_columns` / `text_index_columns` 选项写入。

### 表选项

| 选项 | 取值 | 含义 |
| --- | --- | --- |
| `vector_index_columns` | JSON 数组 | 向量索引声明，例如 `'[{"column":"embedding","dim":8}]'` |
| `text_index_columns` | JSON 数组 | 文本索引声明，例如 `'[{"column":"body"}]'` |
| `index_maintenance` | `inline`、`deferred` | `deferred` 只提交数据，索引交给后台 runner 构建 |

`index_maintenance` 默认是 `inline`：写入提交数据后立即构建或更新受影响的
索引分片。设为 `deferred` 时，写入只提交数据。

### 写入向量

向量列的 `INSERT ... VALUES` 接受 pgvector 文本形式，裸写或 cast 都可以：

```sql
INSERT INTO docs VALUES
  (1, 'apple banana', '[1,0,0,0,0,0,0,0]'),
  (2, 'banana cherry', '[0,1,0,0,0,0,0,0]'::vector(8));
```

## 给已有表建索引

```sql
CREATE INDEX docs_embedding_idx ON docs USING hnsw (embedding)
  WITH (m = 16, ef_construction = 64);

CREATE INDEX docs_body_idx ON docs USING gin (body);
```

- LakeSoul 只有一种向量索引实现，`USING hnsw` 和 `USING ivfflat` 都会被接受
  并归一化到它；`WITH (lists = ..., m = ..., ef_construction = ...)` 为兼容
  而接受，但会被忽略。
- `USING gin` 声明文本索引；也接受 `USING bm25` / `USING text`。
- 算子类 `vector_l2_ops` / `vector_ip_ops` 选择 L2 / 内积；
  `vector_cosine_ops` 会被拒绝（索引只支持 L2 和 IP）。
- 每条语句只支持一列，不支持表达式、`INCLUDE` 或部分索引 `WHERE`。
- 索引名只为客户端兼容而接受，不会存储。LakeSoul 用 `(table, column)`
  标识索引，因此不支持 `DROP INDEX name` 和 `REINDEX INDEX name`；删除向量
  索引用 `lakesoul_drop_vector_index`。
- `inline` 表上，`CREATE INDEX` 返回前会构建已有数据的索引；`deferred` 表只
  保存声明，由后台 runner 构建。

不写 `USING` 时，`CREATE INDEX` 按列类型推断：浮点列表列声明向量索引，
`text` 列声明文本索引；其他类型会要求显式指定方法。

## 修改索引声明

```sql
ALTER TABLE docs SET (index_maintenance = 'deferred');
ALTER TABLE docs SET (vector_index_columns =
  '[{"column":"embedding","dim":8}]');
ALTER TABLE docs SET (text_index_columns = '[{"column":"body"}]');
ALTER TABLE docs RESET (index_maintenance);
```

`ALTER TABLE ... SET` 只声明或替换索引属性，不构建索引；构建请执行
`REINDEX TABLE`。`SET (vector_index_columns = '[]')` 和
`RESET (vector_index_columns)` 会被拒绝：通过 `ALTER TABLE` 删除声明会留下
索引文件。删除向量索引请使用 `lakesoul_drop_vector_index`。

## 构建与重建

```sql
REINDEX TABLE docs;
SELECT lakesoul_rebuild_index('docs', 'all');  -- all | vector | text
```

`REINDEX TABLE` 会从所有 active 数据文件重建表上的全部索引。
`lakesoul_rebuild_index` 是 SQL 函数形式，kind 参数为 `all`、`vector` 或
`text`。

## Deferred 维护

设置 `index_maintenance = 'deferred'` 后，写入只提交数据文件。索引由独立进程
构建：

- `postgres-lakesoul --index-maintenance-executor local` 会拉起并监督一个
  `--index-maintenance-runner` 子进程；all-in-one 默认使用这个模式。
- `--index-maintenance-executor external` 交给独立 Deployment；Helm 的
  `indexMaintenance.mode=runner` 使用这种形态，`coordinator` 使用本地子进程，
  `off` 关闭 deferred 构建。
- runner 扫描 metadata 中的 deferred 表并构建 pending 分片。它会在
  `maintenance.lease` 中持有租约，因此多个 coordinator 和 runner 不会重复
  构建。

deferred 向量声明初始是不可用的。runner 构建完成前，planner 会保留精确排序
扫描，而不是请求 index-only 排序；构建失败时声明保持不可用，精确扫描继续
返回正确结果。尚未构建的 deferred 文本索引会明确报告索引尚未构建，直到
runner 完成构建。

租约表由 `script/meta_init.sql` 和 metadata migration
`V4000002__index_maintenance_lease.sql` 创建；升级已有 metadata 数据库时运行
`script/metadata_migrate.py`。

## 被拒绝的形式

以下形式以 SQLSTATE `0A000` 拒绝：

- `CREATE INDEX ... USING btree/hash/gist/brin`
- 多列、表达式、`INCLUDE` 和部分索引（`WHERE`）
- `UNIQUE` 索引
- `CREATE INDEX ... USING hnsw` 搭配 cosine 算子类
- `DROP INDEX` 和 `REINDEX INDEX/SCHEMA/DATABASE`
- 通过 `ALTER TABLE ... SET ... '[]'` 或 `RESET` 删除向量声明

`DROP INDEX` 不是删除向量索引的入口。请使用：

```sql
SELECT lakesoul_drop_vector_index(
  'docs',      -- table
  'default',   -- namespace
  'embedding', -- column
  false,       -- keep_declaration
  true,        -- purge_files
  0.0,         -- grace seconds
  true,        -- wait_for_cleanup
  false        -- dry_run
);
```

## 另见

- [PostgreSQL DDL](postgres/ddl.md) — 普通建表和列 DDL。
- [向量检索 API](vector-search/api.md) — Python、SQL 和 pgwire 检索。
- [全文检索](full-text-search/overview.md) — 文本索引和 BM25 API。
- [向量检索快速开始](vector-search/quickstart.md)。
