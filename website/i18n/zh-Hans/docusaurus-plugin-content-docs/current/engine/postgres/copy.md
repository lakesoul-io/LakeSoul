---
sidebar_position: 3.5
---

# COPY FROM STDIN

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`COPY ... FROM STDIN` 通过 PostgreSQL 的 copy 子协议批量导入数据。它是导入工具和
dump/restore 工具使用的路径，而不是逐行 `INSERT`：Airbyte 和 Singer target、
`pg_dump`/`pg_restore`、`psql \copy`、pgjdbc `CopyManager`、psycopg2
`copy_expert`/`copy_from`、psycopg3 `cursor.copy()`。

```sql
COPY orders (id, day, region, amount) FROM STDIN;
```

客户端流式发送数据并结束 copy，服务端返回 `COPY <rows>`。

## 格式

支持 TEXT（默认）和 CSV：

```sql
COPY orders FROM STDIN;
COPY orders FROM STDIN WITH (FORMAT csv, HEADER true);
COPY orders FROM STDIN WITH (FORMAT text, DELIMITER '|', NULL 'NULL');
```

| 选项 | TEXT | CSV |
| --- | --- | --- |
| `FORMAT text` / `FORMAT csv` | 支持 | 支持 |
| `DELIMITER` | 单字节，默认 tab | 单字节，默认逗号 |
| `NULL` | 默认 `\N` | 默认未加引号的空字段 |
| `HEADER` | 拒绝（`42601`） | 跳过第一条记录 |
| `QUOTE`、`ESCAPE` | 拒绝（`42601`） | 单字节，默认 `"` |
| `ENCODING` | 仅 `UTF8` | 仅 `UTF8` |

TEXT 值遵循 PostgreSQL 的 COPY 转义：`\b`、`\f`、`\n`、`\r`、`\t`、`\v`、`\\`、
八进制和 `\x` 序列；分隔符前的反斜杠表示它是数据。单独一行 `\.` 表示数据结束。

CSV 字段可以加引号；引号字段内可以包含分隔符、换行和成对引号；接受 `\r\n` 行
结束符。只有未加引号的空字段是 NULL，因此 `""` 是空字符串。使用自定义 `NULL`
字符串时，内容相同的加引号字段仍然是数据。

列值按目标列的 PostgreSQL 输入规则转换：`boolean`、`smallint`/`integer`/
`bigint`、`real`/`double precision`、`numeric(p,s)`（按列 scale 四舍五入）、
`text`、`bytea`（十六进制 `\x...` 或传统转义格式）、`date`、`timestamp`、
`timestamptz`，以及 `CREATE TABLE` 接受的其他类型。无法解析的值返回 `22P02`
（日期时间返回 `22007`），而不是写入一个不同的值。

## 语义

- 一条 `COPY` 对应一次 LakeSoul commit。重复主键（`23505`）、NOT NULL 违反
  （`23502`）或格式错误的行（`22P02`/`22P04`）不会发布**任何**行，包括失败行
  之前的行。
- 在 `BEGIN`/`COMMIT` 中，COPY 与 `INSERT` 一样先 staged，由 `COMMIT` 发布；
  `ROLLBACK` 删除它。其他连接在提交前看不到数据。
- 不写列清单时，数据流必须按表顺序包含所有列。显式列清单可以省略列：省略的列
  写入 NULL（如果表声明了默认值，则写默认值）。
- 空的 COPY 不写入任何内容，返回 `COPY 0`。
- COPY 不能与其他语句共享一个 simple-query batch：batch 在执行前就会以 `0A000`
  拒绝，因为 copy 子协议会占用连接直到 `CopyDone`。
- 客户端可以用 CopyFail 中止；已缓冲的行被丢弃，语句返回 `57014`，连接仍可继续
  使用。
- 行会在内存中缓冲到 `CopyDone`，因为一条语句必须对应一次 commit。超大导入请
  拆成多条 `COPY` 语句。

## 不支持

以下形式会显式拒绝，而不是近似执行：

- `COPY ... TO STDOUT`
- `COPY ... FROM` 文件或程序
- `COPY (query) ...`
- `FORMAT binary`
- `FREEZE`、`FORCE_QUOTE`、`FORCE_NOT_NULL`、`FORCE_NULL`
- 语句文本内嵌数据（`COPY t FROM STDIN; ...`）

## 客户端

| 客户端 | 入口 |
| --- | --- |
| psycopg2 | `cursor.copy_expert(...)`、`cursor.copy_from(...)` |
| psycopg3 | `cursor.copy(...)`，配合 `copy.write` / `copy.write_row` |
| pgjdbc | `connection.unwrap(PGConnection.class).getCopyAPI().copyIn(...)` |
| psql | `\copy orders FROM 'orders.csv' WITH (FORMAT csv, HEADER true)` |

```python
with cursor.copy(
    "COPY orders (id, amount) FROM STDIN WITH (FORMAT csv)"
) as copy:
    copy.write("1,42.00\n")
    copy.write("2,7.50\n")
```
