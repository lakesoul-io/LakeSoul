---
sidebar_position: 3
---

# Text index management

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Text indexes are declared and built through the Python SDK. The index lives under
the table path and is maintained on write.

## Declare an index

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

| Key | Default | Meaning |
| --- | --- | --- |
| `column` | required | `Utf8` / `Utf8View` column |
| `tokenizer` | `jieba` | `jieba`, `default`, `en_stem`, `whitespace`, `raw` |
| `with_positions` | `True` | Store positions for phrase queries |
| `stored` | `False` | Store the original text in the index |
| `rebuild_mode` | `auto` | `auto` or `none` |
| `max_delta_ratio` | `1.0` | Delta-to-base ratio that triggers compaction |

The table needs an `Int64` or `UInt64` primary key. The configuration is stored on
the table, and `write_arrow` updates the index after the data commit.

## Build or rebuild

```python
from lakesoul.text_index import build_table_text_index

build_table_text_index(
    table_name="documents",
    namespace="default",
    text_column="body",
    tokenizer="jieba",
)
```

Per partition:

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

`rebuild=True` rebuilds every shard from its active data files. `store_config`
takes object store credentials; when omitted, the SDK reads
`LAKESOUL_OBJECT_STORE_*`.

## Shards and maintenance

- The index is built per (partition, hash bucket) shard.
- New rows are appended as delta segments; searches merge base and delta results.
- When the delta-to-base ratio exceeds `max_delta_ratio`, the shard is compacted.
  Set `rebuild_mode="none"` to disable automatic compaction and run it yourself.

## Next

- [Text SQL reference](sql-reference.md)
- [Benchmarks](benchmarks.md)
