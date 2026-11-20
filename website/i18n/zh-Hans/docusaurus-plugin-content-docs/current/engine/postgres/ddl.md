---
sidebar_position: 2
sidebar_label: DDL
---

# DDL

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

PostgreSQL 前端支持 `CREATE TABLE` 和 `DROP TABLE`。表通过 LakeSoul catalog 创建；`CREATE TABLE` 需要服务端配置 warehouse 位置（`--warehouse-prefix`）。

## CREATE TABLE

```sql
CREATE TABLE orders (
  id     bigint primary key,
  day    date not null,
  region text not null,
  amount numeric(20,4)
) PARTITIONED BY (day)
  WITH (hash_bucket_num = 8, file_format = 'vortex-compact');
```

- **主键**可以写成列约束（`id bigint primary key`）或表约束（`PRIMARY KEY (id)`）。
- `PARTITIONED BY (col, ...)` 使用已有列，声明范围分区。分区列必须在表中声明，并应设为 `NOT NULL`。
- `WITH (...)` 配置表，见下面的选项。

`PARTITIONED BY` 是 LakeSoul/Hive 形式：它引用已有列。PostgreSQL 的 `PARTITION BY` 子句不被接受。

## 表选项

| 选项 | 取值 | 含义 |
| --- | --- | --- |
| `hash_bucket_num` | `1..=32768` | 主键表的哈希桶数；默认 `4` |
| `file_format` | `parquet`、`vortex`、`vortex-compact` | 物理文件格式；默认 `vortex-compact` |
| `append_only` | `true`、`false` | Append-only 表，不做主键合并 |
| `use_cdc` | `true`、`false` | CDC 表，带变更列；删除和增量 SQL 目标需要它 |
| `lakesoul_cdc_change_column` | 列名 | CDC 变更列；默认 `rowKinds` |
| `vector_index_columns` | JSON 数组 | 向量索引声明，例如 `'[{"column":"embedding","dim":8}]'` |
| `text_index_columns` | JSON 数组 | 文本索引声明，例如 `'[{"column":"body"}]'` |
| `index_maintenance` | `inline`、`deferred` | `deferred` 只提交数据，索引交给后台 runner 构建 |

未知选项会以 `0A000` 拒绝，而不是被忽略。

`use_cdc = true` 需要一个变更列。声明 `rowKinds`，或用 `lakesoul_cdc_change_column` 指定其他列。没有它，删除无法表示，增量 SQL 目标也无法撤回行。

## 类型

`CREATE TABLE` 接受一组固定的列类型：

| PostgreSQL 类型 | 存储为 |
| --- | --- |
| `smallint`、`int2` | `Int16` |
| `integer`、`int`、`int4` | `Int32` |
| `bigint`、`int8` | `Int64` |
| `real`、`float4` | `Float32` |
| `double precision`、`float8` | `Float64` |
| `boolean` | `Boolean` |
| `text`、`varchar`、`char` | `Utf8`；`varchar(n)`/`char(n)` 在 `pg.typmod` 中保留长度 |
| `uuid` | `Utf8`，带 `pg.type=uuid` 标记 |
| `INT[]`、`TEXT[]` 等 | `List<element>`；受支持标量类型的一维数组 |
| `json` | `Utf8`，带 `arrow.json` 扩展 |
| `jsonb` | `Utf8`，带 `arrow.json` 和 `pg.json` 标记 |
| `date` | `Date32` |
| `time(p)` | `p=0/3` 为 `Time32`，`p=6` 为 `Time64` |
| `timestamp(p)` | `p=0/3/6` 为 `Timestamp`，默认 6 |
| `timestamptz(p)` | `p=0/3/6` 的 UTC `Timestamp` |
| `bytea` | Binary |
| `numeric(p, s)`、`decimal(p, s)` | `p <= 38` 为 `Decimal128`，`p <= 76` 为 `Decimal256` |
| `money` | `Decimal128(19,2)`，带 `pg.type=money` 标记 |
| `vector(n)` | `FixedSizeList(Float32, n)`，带 `pg.vector` 标记 |
| `vector` | `List(Float32)`，带 `pg.vector` 标记 |

`numeric` 和 `decimal` 必须显式写出精度和小数位；不带参数的 `numeric` 和精度超过 76
会被拒绝。`time(p)`/`timestamp(p)` 只接受 `p = 0, 3, 6`；其他精度会拒绝。
`interval` 和 `inet` 列不支持（interval 表达式仍然可用）。表以外的类型会以 `0A000` 拒绝。

向量列的 `INSERT ... VALUES` 接受 pgvector 文本形式：裸写 `'[1,2,3]'` 或
`'[1,2,3]'::vector(3)` 都可以。

完整映射和查询侧类型行为见[数据类型](../../reference/data-types.md)。

## 向量索引与文本索引

向量索引需要主键，以及 `vector(n)`（或浮点列表）列：

```sql
CREATE TABLE docs (
  id        bigint primary key,
  body      text,
  embedding vector(8)
) WITH (vector_index_columns = '[{"column":"embedding","dim":8}]',
        text_index_columns   = '[{"column":"body"}]');
```

`CREATE INDEX` 给已有表声明索引。LakeSoul 只有一种向量索引实现，因此
`USING hnsw` 和 `USING ivfflat` 都会被接受并归一化到它；
`WITH (lists = ..., m = ..., ef_construction = ...)` 为兼容而接受，但会被忽略。
文本索引使用 `USING gin`：

```sql
CREATE INDEX docs_embedding_idx ON docs USING hnsw (embedding)
  WITH (m = 16, ef_construction = 64);
CREATE INDEX docs_body_idx ON docs USING gin (body);
```

每条语句只支持一列，不支持表达式、`INCLUDE` 或部分索引 `WHERE`。索引名只为
客户端兼容而接受，不会写入元数据：LakeSoul 用 `(table, column)` 标识索引，
因此不支持 `DROP INDEX name` 和 `REINDEX INDEX name`。

除非表设置了 `index_maintenance = 'deferred'`，`CREATE INDEX` 会同步构建
已有数据的索引；deferred 表只保存声明，由后台 runner 构建。
`ALTER TABLE ... SET (...)` 只声明或替换索引属性，构建请执行 `REINDEX TABLE`：

```sql
ALTER TABLE docs SET (index_maintenance = 'deferred');
REINDEX TABLE docs;
SELECT lakesoul_rebuild_index('docs', 'all');  -- all | vector | text
```

`index_maintenance = 'deferred'` 时，写入只提交数据。以
`--index-maintenance-executor local` 启动的 coordinator 会监督一个
`--index-maintenance-runner` 子进程；Helm 则把它作为独立 Deployment 运行。
runner 会在 `maintenance.lease` 中持有租约，因此多个服务同时运行也不会重复构建。
尚未构建的 deferred 向量索引会回退到精确的排序扫描；文本索引在 runner
构建完成前会明确报告索引尚未构建。

## DROP TABLE

```sql
DROP TABLE orders;
```

支持 `DROP TABLE IF EXISTS`。删除表会移除 catalog 记录；数据文件遵循正常的清理策略。

## 被拒绝的子句

前端会拒绝以下子句，而不是静默忽略：

- `CREATE TABLE AS`、`CREATE TEMPORARY TABLE`、`CREATE EXTERNAL TABLE`、`CREATE OR REPLACE TABLE`
- `LIKE`、`CLONE`、`INHERITS`、`PARTITION OF`、`FOR VALUES`
- `UNIQUE`、`CHECK`、`FOREIGN KEY` 约束，以及列默认值
- `STORED AS` / `LOCATION`（改用 `WITH (file_format = ...)` 和服务端 warehouse）
- 受支持形式之外的 `PRIMARY KEY` 子句
- `CREATE INDEX ... USING btree/hash/gist/brin`、多列/表达式索引、部分索引和 `INCLUDE`
- `DROP INDEX` 和 `REINDEX INDEX/SCHEMA/DATABASE`；删除向量索引用
  `lakesoul_drop_vector_index`，重建用 `REINDEX TABLE`

每次拒绝都返回 SQLSTATE `0A000`，消息里会指出具体子句。

## 下一步

- [写入与 Upsert](insert-and-upsert.md)
- [数据类型](../../reference/data-types.md)
- [物理文件格式](../../operations/physical-file-formats.md)
