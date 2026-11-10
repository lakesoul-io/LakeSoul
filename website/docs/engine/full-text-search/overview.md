---
sidebar_position: 1
sidebar_label: Overview
---

# Full-text search

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul keeps a Tantivy full-text index next to the table data and ranks results
with BM25. The index is maintained on write, and a top-k query is answered from
the index instead of a full scan.

## What you get

- **`text_match(column, query)`** as a filter predicate.
- **`text_score(column, query)`** in `ORDER BY` for BM25 ranking.
- **Top-k pushdown**: `WHERE text_match(...) ORDER BY text_score(...) DESC LIMIT k`
  returns the global top-k from the index.
- **Same engine**: join, filter and aggregate full-text results with vector search
  and ordinary SQL.

## Example

```sql
SELECT id, title, body
FROM documents
WHERE text_match(body, 'lakehouse')
ORDER BY text_score(body, 'lakehouse') DESC
LIMIT 10;
```

## Index creation

The text index is declared through the Python SDK, next to the table:

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    text_index=[{"column": "body", "tokenizer": "jieba"}],
)
```

The PostgreSQL endpoint then queries the table with `text_match` and `text_score`.

## Next

- [Text SQL reference](sql-reference.md)
- [Text index management](index-management.md)
- [Benchmarks](benchmarks.md)
