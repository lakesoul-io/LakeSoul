---
sidebar_position: 3.5
---

# COPY

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

PostgreSQL 的 copy 子协议支持双向批量搬运。`COPY ... FROM STDIN` 是导入工具
使用的路径，而不是逐行 `INSERT`；`COPY ... TO STDOUT` 是报表导出和数据搬运工具
使用的导出路径。常见客户端包括 Airbyte、Singer、`psql \copy`、pgjdbc
`CopyManager`、psycopg2 `copy_expert`/`copy_from` 和 psycopg3
`cursor.copy()`。

```sql
COPY orders (id, day, region, amount) FROM STDIN;
```

客户端流式发送数据并结束 copy，服务端返回 `COPY <rows>`。

## 格式

导入方向支持 TEXT（默认）、CSV 和 binary：

```sql
COPY orders FROM STDIN;
COPY orders FROM STDIN WITH (FORMAT csv, HEADER true);
COPY orders FROM STDIN WITH (FORMAT binary);
COPY orders FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL 'NULL');
```

| 选项 | TEXT | CSV | binary（导入） |
| --- | --- | --- | --- |
| `FORMAT text` / `csv` / `binary` | 支持 | 支持 | 支持 |
| `DELIMITER` | 单字节，默认 tab | 单字节，默认逗号 | 拒绝（`42601`） |
| `NULL` | 默认 `\N` | 默认未加引号的空字段 | 字段长度 `-1` |
| `HEADER` | 拒绝（`42601`） | 导入时跳过第一条记录；导出时写列名 | 拒绝（`42601`） |
| `QUOTE`、`ESCAPE` | 拒绝（`42601`） | 单字节，默认 `"` | 拒绝（`42601`） |
| `ENCODING` | 仅 `UTF8` | 仅 `UTF8` | 仅 `UTF8` |

TEXT 值遵循 PostgreSQL 的 COPY 转义：`\b`、`\f`、`\n`、`\r`、`\t`、`\v`、`\\`、
八进制和 `\x` 序列；分隔符前的反斜杠表示它是数据。单独一行 `\.` 表示数据结束。

CSV 字段可以加引号；引号字段内可以包含分隔符、换行和成对引号；接受 `\r\n` 行
结束符。只有未加引号的空字段是 NULL，因此 `""` 是空字符串。使用自定义 `NULL`
字符串时，内容相同的加引号字段仍然是数据。

binary 导入遵循 PostgreSQL 的 `PGCOPY` 流格式：19 字节头（`PGCOPY\n\xff\r\n\0`、
flags 为 0、扩展长度为 0），随后每行一个 `int16` 字段数，每个字段一个 `int32`
长度加负载，以一个 `int16` 的 `-1` 字段数结束。字段长度为 `-1` 表示 NULL。
任意一段都可以跨 `CopyData` 帧拆分。支持的 binary 值类型为核心标量：
`boolean`、`smallint`、`integer`、`bigint`、`real`、`double precision`、
`numeric`、`text`/`varchar`/`char`、`bytea`、`uuid`、`money`、`date`、`time`、
`timestamp`、`timestamptz`、`json` 和 `jsonb`。binary 数组和 pgvector 值以
`0A000` 拒绝。`COPY ... TO STDOUT WITH (FORMAT binary)` 同样拒绝；导出仅支持
TEXT 和 CSV。

列值按目标列的 PostgreSQL 输入规则转换：`boolean`、`smallint`/`integer`/
`bigint`、`real`/`double precision`、`numeric(p,s)`（按列 scale 四舍五入）、
`text`、`varchar(n)`、`char(n)`、`uuid`、`money`、数组、`bytea`（十六进制
`\x...` 或传统转义格式）、`date`、`time(p)`、`timestamp(p)`、`timestamptz(p)`，
以及 `CREATE TABLE` 接受的其他类型。无法解析的值返回 `22P02`
（日期时间返回 `22007`），而不是写入一个不同的值。

## 语义

- 一条 `COPY ... FROM STDIN` 对应一次 LakeSoul commit。重复主键（`23505`）、NOT NULL 违反
  （`23502`）或格式错误的行（`22P02`/`22P04`）不会发布**任何**行，包括失败行
  之前的行。
- 在 `BEGIN`/`COMMIT` 中，COPY 与 `INSERT` 一样先 staged，由 `COMMIT` 发布；
  `ROLLBACK` 删除它。其他连接在提交前看不到数据。
- 不写列清单时，数据流必须按表顺序包含所有列。显式列清单可以省略列：省略的列
  写入 NULL（如果表声明了默认值，则写默认值）。
- 空的 COPY 不写入任何内容，返回 `COPY 0`。
- COPY（任一方向）不能与其他语句共享一个 simple-query batch：batch 在执行前就会
  以 `0A000` 拒绝。
- 客户端可以用 CopyFail 中止；已缓冲的行被丢弃，语句返回 `57014`，连接仍可继续
  使用。
- 行会在内存中缓冲到 `CopyDone`，因为一条语句必须对应一次 commit。超大导入请
  拆成多条 `COPY` 语句。

## 导出：COPY TO STDOUT

```sql
COPY orders TO STDOUT;
COPY orders (id, amount) TO STDOUT WITH (FORMAT csv, HEADER true);
COPY (SELECT id, amount FROM orders WHERE amount > 0 ORDER BY id)
  TO STDOUT WITH (FORMAT csv);
```

服务端逐行流式发送 `CopyData`，最后返回 `COPY <rows>`。

`HEADER` 会与第一行数据一起发送；零行导出不带表头。TEXT 和 CSV 使用与导入
相同的选项。行来自查询计划，因此事务块内的表导出能看到本块 staged 的写入，
查询导出支持 `ORDER BY`、过滤和 join。没有 `ORDER BY` 的表导出不保证行顺序。

## 不支持

以下形式会显式拒绝，而不是近似执行：

- `COPY ... TO` 文件或程序，以及 `COPY ... FROM` 文件或程序
- 导出使用 `FORMAT binary`，导入使用 binary 数组或 vector 值
- `FORMAT binary` 与 TEXT/CSV 专用选项（`DELIMITER`、`NULL`、`HEADER`、
  `QUOTE`、`ESCAPE`）组合
- `FREEZE`、`FORCE_QUOTE`、`FORCE_NOT_NULL`、`FORCE_NULL`
- 语句文本内嵌数据（`COPY t FROM STDIN; ...`）

## 客户端

| 客户端 | 入口 |
| --- | --- |
| psycopg2 | `cursor.copy_expert(...)`、`cursor.copy_from(...)`；导出用 `copy_expert` |
| psycopg3 | `cursor.copy(...)`，配合 `copy.write` / `copy.write_row` / `copy.read` |
| pgjdbc | `connection.unwrap(PGConnection.class).getCopyAPI().copyIn(...)` / `copyOut(...)` |
| psql | `\copy orders FROM 'orders.csv' ...` / `\copy orders TO 'orders.csv' ...` |

```python
with cursor.copy(
    "COPY orders (id, amount) FROM STDIN WITH (FORMAT csv)"
) as copy:
    copy.write("1,42.00\n")
    copy.write("2,7.50\n")

with cursor.copy(
    "COPY orders (id, amount) TO STDOUT WITH (FORMAT csv)"
) as copy:
    for chunk in copy:
        print(chunk)
```
