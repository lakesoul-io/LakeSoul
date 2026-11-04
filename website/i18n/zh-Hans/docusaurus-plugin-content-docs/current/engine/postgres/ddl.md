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
| `use_cdc` | `true`、`false` | CDC 表，带变更列 |
| `lakesoul_cdc_change_column` | 列名 | CDC 变更列；默认 `rowKinds` |

未知选项会以 `0A000` 拒绝，而不是被忽略。

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
| `text`、`varchar`、`char` | `Utf8` |
| `date` | `Date32` |
| `timestamp` | 微秒精度、无时区时间戳 |
| `timestamptz` | 微秒精度、UTC 时间戳 |
| `bytea` | Binary |
| `numeric(p, s)`、`decimal(p, s)` | `Decimal128(p, s)`，`p <= 38` |

`numeric` 和 `decimal` 必须显式写出精度和小数位；不带参数的 `numeric` 会被拒绝。`timestamp(p)` 只接受默认精度 `6`。表以外的类型会以 `0A000` 拒绝。

完整映射和查询侧类型行为见[数据类型](../../reference/data-types.md)。

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

每次拒绝都返回 SQLSTATE `0A000`，消息里会指出具体子句。

## 下一步

- [写入与 Upsert](insert-and-upsert.md)
- [数据类型](../../reference/data-types.md)
- [物理文件格式](../../operations/physical-file-formats.md)
