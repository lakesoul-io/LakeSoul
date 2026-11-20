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
| `text`、`varchar`、`char` | `Utf8` | `varchar(n)`/`char(n)` 在 `pg.typmod` 中保留长度并在写入时校验 |
| `uuid` | `Utf8` + `pg.type=uuid` | 规范化小写带连字符文本；wire 上报告 UUID OID |
| 一维数组，如 `INT[]` | `List<element>` | 元素类型必须是受支持的标量类型；多维数组、数组主键/分区列会被拒绝 |
| `json` | `Utf8` + `arrow.json` | 保留原始 JSON 文本 |
| `jsonb` | `Utf8` + `arrow.json` + `pg.json` | 规范化 JSON 文本；重复 key 和 key 顺序会被归一 |
| `date` | `Date32` | |
| `time(p)` | Arrow `Time32`/`Time64` | `p` 只支持 0、3、6 |
| `timestamp(p)` | Arrow `Timestamp` | `p` 只支持 0、3、6，默认 6；超出单位的精度会被截断 |
| `timestamptz(p)` | Arrow `Timestamp`，UTC | 精度规则相同 |
| `bytea` | Binary | |
| `numeric(p, s)`、`decimal(p, s)` | `p <= 38` 为 `Decimal128`，`39 <= p <= 76` 为 `Decimal256` | `1 <= p <= 76`，`0 <= s <= p` |
| `money` | `Decimal128(19,2)` + `pg.type=money` | wire 上报告 `MONEY` OID |

不带精度和小数位的 `numeric` 会被拒绝；精度超过 76 也会被拒绝，而不是静默截断。
`interval` 和 `inet` 列不支持：Arrow 有 interval 类型但 Parquet/Vortex 无法持久化，
Arrow 没有 inet 类型；interval 表达式仍然可用。

`INSERT`、`UPDATE`、`UPSERT` 和 `COPY` 写入的 `money` 值接受 PostgreSQL 的
`$1,234.56` 格式；表达式内部的 cast（例如 `SELECT '$1.50'::money`）只接受
普通十进制格式，因为该 cast 走 Arrow 的 decimal 解析。

## JSON 列

`json` 和 `jsonb` 以 UTF-8 文本存储，并带有 Arrow canonical `arrow.json` 扩展；PostgreSQL 前端还会写入 `pg.json` 标记，因此 `SELECT` 和 `pg_attribute` 会分别报告 OID 114（`json`）或 3802（`jsonb`）。

- `json` 保留写入时的原始文档。
- `jsonb` 从 PostgreSQL 前端进入时（参数、`COPY ... FROM STDIN`、订阅写入）会解析并规范化：重复 key 保留最后一个值、key 排序、去除无意义空白。
- 两种类型都支持 `->`、`->>`、`#>`、`#>>` 和 `?`。
- Spark 3.5 和 Flink 1.20 会把这类列看作普通字符串，因此同一张表仍可跨引擎读写。

LakeSoul 中 `jsonb` 的相等和排序是规范化文本上的字符串语义，不是 PostgreSQL 的二进制 jsonb 比较；`@>` 等比较操作符尚未实现。

数组既可以作为列类型，也可以用在表达式和结果里（`ARRAY[...]`、`INTEGER[]`、
1-based 下标），wire protocol 会编码和解码。受支持的数组列是一维的，元素必须是
受支持的标量类型。`INSERT`、`UPDATE`、`UPSERT` 和 `COPY` 会逐元素处理数组字面量：
UUID 文本会规范化、接受 `money` 文本、保留 numeric 精度（包括超过 38 位的
`numeric`），并检查 `varchar(n)[]` 的元素长度。数组不能作为主键或分区列。

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
| `numeric` 精度 | 最多 76 位（`Decimal256`） | 更宽的 `NUMERIC` |
| `timestamp` 精度 | 秒/毫秒/微秒（0/3/6） | 可配置 0-6 |
| `char(n)` 补空格 | 只校验长度，不模拟尾部补空格 | 固定长度并在比较时忽略尾部空格 |

值一致，OID 可能不同。客户端依赖 PostgreSQL 类型时，在查询里显式 CAST。

## 下一步

- [DDL](../engine/postgres/ddl.md)
- [兼容性](../engine/postgres/compatibility.md)
- [SQL 错误](sql-errors.md)
