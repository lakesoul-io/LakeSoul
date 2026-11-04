---
sidebar_position: 3
sidebar_label: 索引管理
---

# 文本索引管理

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

文本索引通过 Python SDK 声明和构建。索引存放在表路径下，并随写入维护。

## 声明索引

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    text_index=[{
        "column": "body",
        "tokenizer": "jieba",
        "with_positions": True,
    }],
)
```

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `column` | 必填 | `Utf8` / `Utf8View` 列 |
| `tokenizer` | `jieba` | `jieba`、`default`、`en_stem`、`whitespace`、`raw` |
| `with_positions` | `True` | 存储位置，用于短语查询 |
| `stored` | `False` | 在索引里存原文 |
| `rebuild_mode` | `auto` | `auto` 或 `none` |
| `max_delta_ratio` | `1.0` | 触发 compaction 的 delta/base 比例 |

表需要 `Int64` 或 `UInt64` 主键。配置存在表属性上，`write_arrow` 在数据提交后更新索引。

## 构建或重建

```python
from lakesoul.text_index import build_table_text_index

build_table_text_index(
    table_name="documents",
    namespace="default",
    text_column="body",
    tokenizer="jieba",
)
```

按分区：

```python
from lakesoul.text_index import build_partition_text_index

build_partition_text_index(
    table_name="documents",
    namespace="default",
    partition_desc="range=2026-01-01",
    text_column="body",
    tokenizer="jieba",
)
```

`rebuild=True` 会从活跃数据文件重建每个 shard。`store_config` 接受对象存储凭证；省略时 SDK 读取 `LAKESOUL_OBJECT_STORE_*`。

## Shard 与维护

- 索引按（分区，哈希 bucket）shard 构建。
- 新行作为 delta segment 追加；搜索会合并 base 和 delta 结果。
- delta/base 比例超过 `max_delta_ratio` 时，shard 会被 compact。设置 `rebuild_mode="none"` 可关闭自动 compaction，改由你手动运行。

## 下一步

- [文本 SQL 参考](sql-reference.md)
- [Benchmark](benchmarks.md)
