---
sidebar_position: 2
---

# Text SQL reference

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Functions

| Function | Returns | Use |
| --- | --- | --- |
| `text_match(column, query)` | Boolean | A filter predicate |
| `text_score(column, query)` | BM25 score | Only in `ORDER BY` |

`text_match` evaluates the query against the column. `text_score` supplies the
ranking; it cannot be projected into the result set yet.

## Top-k query

```sql
SELECT id, title
FROM documents
WHERE text_match(body, 'lakehouse')
ORDER BY text_score(body, 'lakehouse') DESC
LIMIT 10;
```

The planner recognises the pattern and asks the text index for the global top-k.
The `LIMIT` is required for the index path.

## Filter without ranking

```sql
SELECT id, title
FROM documents
WHERE text_match(body, 'lakehouse');
```

Without an `ORDER BY text_score(...)`, the predicate is evaluated exactly over the
scan. This is correct but does not use the top-k path.

## Rules

- `text_score` must appear in `ORDER BY ... DESC`; projecting it is not supported.
- Without a finite `LIMIT`, the planner falls back to an exact scan.
- The column must have a text index declared through the Python SDK.
- The query string is a scalar expression; parameters are supported.
- Multiple `text_match` predicates are evaluated as ordinary boolean filters.

## Tokenizers

The index is built with one tokenizer per column:

| Tokenizer | Behaviour |
| --- | --- |
| `jieba` | Chinese word segmentation (default) |
| `default` | Tantivy's default tokenizer |
| `en_stem` | English stemming |
| `whitespace` | Whitespace splitting |
| `raw` | One token for the whole value |

The tokenizer is fixed when the index is created.

## Next

- [Text index management](index-management.md)
- [Benchmarks](benchmarks.md)
- [Vector search](../vector-search/overview.md)
