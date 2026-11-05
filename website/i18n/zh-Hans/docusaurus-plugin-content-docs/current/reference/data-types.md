---
sidebar_position: 2
sidebar_label: 数据类型
---

# 数据类型

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## DDL 列类型

`CREATE TABLE` 把 PostgreSQL 类型映射到 LakeSoul 的 Arrow schema：

| PostgreSQL 类型 | 存储为 | 说明 |
| --- | --- | --- |
| `smallint`、`int2` | `Int16` | |
| `integer`、`int`、`int4` | `Int32` | |
| `bigint`、`int8` | `Int64` | 有索引的表的主键必须使用它 |
| `real`、`float4` | `Float32` | |
| `double precision`、`float8` | `Float64` | |
| `float(p)` | `p <= 24` 为 `Float32`，`p <= 53` 为 `Float64` | 只接受精度 |
| `boolean`、`bool` | `Boolean` | |
| `text`、`varchar`、`char` | `Utf8` | |
| `date` | `Date32` | |
| `timestamp` | 微秒精度、无时区 | 只接受 `timestamp(6)` |
| `timestamptz` | 微秒精度、UTC | |
| `bytea` | Binary | |
| `numeric(p, s)`、`decimal(p, s)` | `Decimal128(p, s)` | `1 <= p <= 38`，`0 <= s <= p` |

不带精度和小数位的 `numeric` 会被拒绝。`timestamp(p)` 只接受 `p = 6`；其他精度会被拒绝，而不是静默舍入。

## 不能作为列类型的类型

`uuid`、`json`、`jsonb`、`interval`、`time`、`inet`、`money`、枚举和数组列类型不在 DDL 映射里。包含这些列的表会以 `0A000` 拒绝。

数组仍然可以用在表达式和结果里（`ARRAY[...]`、`INTEGER[]`、1-based 下标），wire protocol 也能解码。它们不是受支持的列类型。

## 查询侧行为

- **NULL 语义**遵循 SQL 三值逻辑：`NULL` 与空串不同，`COUNT(*)` 计行数，`COUNT(col)` 跳过 NULL。
- **Decimal** 是精确值。引擎按列的 scale 返回 `NUMERIC`；`DECIMAL`/`NUMERIC` 不经过浮点。
- **时间戳**是微秒精度；`timestamptz` 归一化为 UTC。
- **整数运算**遵循列宽；除法和取余遵循操作数类型的 SQL 语义。

## 已知边界

少数类型差异是有意记录下来的，而不是隐藏：

| 场景 | LakeSoul | PostgreSQL |
| --- | --- | --- |
| `COALESCE(integer_col, -1)` | `INT8` | `INT4` |
| 未 CAST 的 `rank()` / `row_number()` | `NUMERIC` | `INT8` |
| `numeric` 精度 | 最多 38 位 | 更宽的 `NUMERIC` |
| `timestamp` 精度 | 微秒 | 可配置 |

值一致，OID 可能不同。客户端依赖 PostgreSQL 类型时，在查询里显式 CAST。

## 下一步

- [DDL](../engine/postgres/ddl.md)
- [兼容性](../engine/postgres/compatibility.md)
- [SQL 错误](sql-errors.md)
