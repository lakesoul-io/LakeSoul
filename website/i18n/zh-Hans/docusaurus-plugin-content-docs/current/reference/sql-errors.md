---
sidebar_position: 4
sidebar_label: SQL 错误
---

# SQL 错误

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

PostgreSQL 前端在行为能够清晰映射时返回标准 SQLSTATE，并记录不能映射的边界。

## 错误码

| SQLSTATE | 名称 | 常见原因 |
| --- | --- | --- |
| `0A000` | feature_not_supported | `UPDATE`、`DELETE`、`COPY`、`RETURNING`、不支持的 DDL 子句、事务块内写入 |
| `21000` | cardinality_violation | 标量子查询返回多行 |
| `22007` | invalid_datetime_format | 时间戳或日期字面量无法解析 |
| `22023` | invalid_parameter_value | 非法的 `SET` 值或表选项 |
| `22P02` | invalid_text_representation | 字符串字面量无法转换为目标类型 |
| `23505` | unique_violation | 普通插入遇到重复主键 |
| `25P02` | in_failed_sql_transaction | 失败事务之后的语句，直到 `ROLLBACK` |
| `40001` | serialization_failure | 写入竞争失败；重试该语句 |
| `42601` | syntax_error | SQL 语法错误，或源/目标列不匹配 |
| `42701` | duplicate_column | 列重复声明，包括 `PARTITIONED BY` 中 |
| `42703` | undefined_column | 列不存在 |
| `42804` | datatype_mismatch | 表达式无法赋值转换 |
| `42P01` | undefined_table | 表不存在 |
| `42P07` | duplicate_table | 表已存在 |
| `42P16` | invalid_table_definition | 主键或表定义非法 |
| `3F000` | invalid_schema_name | 限定名中的 schema 非法 |
| `57014` | query_canceled | `statement_timeout` 或 cancel 请求 |
| `XX000` | internal_error | 引擎错误，或启动时 database 不存在 |

## 映射边界

以下差异是记录在案的，而不是静默不同：

| 场景 | LakeSoul | PostgreSQL |
| --- | --- | --- |
| 非法整数转换 | `XX000` | `22P02` |
| 启动时 database 不存在 | `FATAL XX000` | 不同的消息和错误码 |
| `rank()` / `row_number()` 类型 | `NUMERIC` | `INT8` |
| `COALESCE(integer_col, -1)` 类型 | `INT8` | `INT4` |

在上表说明匹配的地方按 SQLSTATE 匹配；只在兜底时解析消息。

## 重试

- `40001` 可重试：语句与并发写者竞争失败。等对方提交后重试。
- `23505` 不能原样重试：主键已经存在。如果要 upsert，用 `ON CONFLICT`。
- `57014` 是取消，不是数据错误；连接仍然可用。

## 下一步

- [兼容性](../engine/postgres/compatibility.md)
- [写入与 Upsert](../engine/postgres/insert-and-upsert.md)
- [数据类型](data-types.md)
