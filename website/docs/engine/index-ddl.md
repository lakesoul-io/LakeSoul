---
sidebar_position: 3
sidebar_label: Vector & text index DDL
---

# Vector and text index DDL

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul vector and text indexes are declared on the table and maintained as
derived objects. The PostgreSQL endpoint exposes the declarations through
ordinary DDL, so a table can be created, indexed and searched without leaving
`psql` or a PostgreSQL driver. Python and Spark use the same table properties;
see [Vector search API](vector-search/api.md) and
[Full-text search](full-text-search/overview.md) for those APIs.

## Create a table

A vector index needs a primary key and a vector column. A text index needs a
primary key and a text column. Declare the indexes in the same `CREATE TABLE`:

```sql
CREATE TABLE docs (
  id        bigint primary key,
  body      text,
  embedding vector(8)
) WITH (
  vector_index_columns = '[{"column":"embedding","dim":8}]',
  text_index_columns   = '[{"column":"body"}]'
);
```

`vector(n)` is stored as `FixedSizeList(Float32, n)` with the `pg.vector`
marker; a bare `vector` is a variable-length `List(Float32)`. A vector index
needs a dimension, so use `vector(n)` or pass `dim` in the declaration/property.
`vector_index_columns` and `text_index_columns` are JSON arrays, so several
columns can be declared at once.

The same properties are understood by every LakeSoul engine. In Python they are
written by `vector_index=` / `text_index=` and `properties`, and in DataFusion
SQL by the `vector_index_columns` / `text_index_columns` options.

### Table options

| Option | Values | Meaning |
| --- | --- | --- |
| `vector_index_columns` | JSON array | Vector index declarations, e.g. `'[{"column":"embedding","dim":8}]'` |
| `text_index_columns` | JSON array | Text index declarations, e.g. `'[{"column":"body"}]'` |
| `index_maintenance` | `inline`, `deferred` | `deferred` commits data only and leaves index builds to the background runner |

`index_maintenance` defaults to `inline`: writes build or update the affected
index shards after the data commit. With `deferred`, writes commit data only.

### Insert vectors

`INSERT ... VALUES` accepts the pgvector text form for a vector column, either
bare or cast:

```sql
INSERT INTO docs VALUES
  (1, 'apple banana', '[1,0,0,0,0,0,0,0]'),
  (2, 'banana cherry', '[0,1,0,0,0,0,0,0]'::vector(8));
```

## Create an index on an existing table

```sql
CREATE INDEX docs_embedding_idx ON docs USING hnsw (embedding)
  WITH (m = 16, ef_construction = 64);

CREATE INDEX docs_body_idx ON docs USING gin (body);
```

- LakeSoul has one vector-index implementation, so `USING hnsw` and
  `USING ivfflat` are accepted and normalized to it. `WITH (lists = ..., m =
  ..., ef_construction = ...)` is accepted for compatibility and ignored.
- `USING gin` declares a text index. A text index also accepts the
  full-text spellings `USING bm25` / `USING text`.
- An operator class `vector_l2_ops` / `vector_ip_ops` selects L2 / inner
  product. `vector_cosine_ops` is refused because the index supports only L2
  and IP.
- One column per statement is supported, without expressions, `INCLUDE` or a
  partial `WHERE`.
- Index names are accepted for client compatibility but not stored. LakeSoul
  identifies an index by `(table, column)`, so `DROP INDEX name` and
  `REINDEX INDEX name` are not available. Use
  `lakesoul_drop_vector_index` to drop a vector index.
- On an `inline` table, `CREATE INDEX` builds the index over existing rows
  before returning. On a `deferred` table it stores the declaration and the
  background runner builds it.

`CREATE INDEX` infers the kind from the column when `USING` is omitted:
a float-list column declares a vector index and a `text` column declares a text
index. Any other type asks for an explicit method.

## Change declarations

```sql
ALTER TABLE docs SET (index_maintenance = 'deferred');
ALTER TABLE docs SET (vector_index_columns =
  '[{"column":"embedding","dim":8}]');
ALTER TABLE docs SET (text_index_columns = '[{"column":"body"}]');
ALTER TABLE docs RESET (index_maintenance);
```

`ALTER TABLE ... SET` only declares or replaces index properties; it does not
build them. `REINDEX TABLE` builds them. `SET (vector_index_columns = '[]')`
and `RESET (vector_index_columns)` are refused: removing a declaration through
`ALTER TABLE` would leave index files behind. Use
`lakesoul_drop_vector_index` instead.

## Build and rebuild

```sql
REINDEX TABLE docs;
SELECT lakesoul_rebuild_index('docs', 'all');  -- all | vector | text
```

`REINDEX TABLE` rebuilds every declared index of the table from all active data
files. `lakesoul_rebuild_index` is the SQL function form; its kind argument is
`all`, `vector` or `text`.

## Deferred maintenance

With `index_maintenance = 'deferred'`, writes commit data files only. The index
is built by a separate process:

- `postgres-lakesoul --index-maintenance-executor local` starts and supervises
  one `--index-maintenance-runner` child. This is the all-in-one default.
- `--index-maintenance-executor external` leaves it to a separate Deployment.
  The Helm chart's `indexMaintenance.mode=runner` uses this shape; `coordinator`
  uses the local child, and `off` disables deferred builds.
- The runner scans metadata for deferred tables and builds pending shards. It
  holds a lease in `maintenance.lease`, so several coordinators and runners do
  not duplicate the work.

A deferred vector declaration starts unavailable. Until the runner builds it,
the planner keeps the exact ordered scan instead of requesting index-only
ordering. If a build fails, the declaration stays unavailable and the exact
scan keeps answering. Text search on a not-yet-built deferred declaration
reports that the index is not built until the runner has built it.

The lease table is created by `script/meta_init.sql` and the metadata migration
`V4000002__index_maintenance_lease.sql`; run `script/metadata_migrate.py` when
upgrading an existing metadata database.

## Refused forms

These are refused with SQLSTATE `0A000`:

- `CREATE INDEX ... USING btree/hash/gist/brin`
- multi-column, expression, `INCLUDE` and partial (`WHERE`) indexes
- `UNIQUE` indexes
- `CREATE INDEX ... USING hnsw` with a cosine operator class
- `DROP INDEX` and `REINDEX INDEX/SCHEMA/DATABASE`
- removing a vector declaration with `ALTER TABLE ... SET ... '[]'` or `RESET`

`DROP INDEX` is not the vector drop path. Use:

```sql
SELECT lakesoul_drop_vector_index(
  'docs',      -- table
  'default',   -- namespace
  'embedding', -- column
  false,       -- keep_declaration
  true,        -- purge_files
  0.0,         -- grace seconds
  true,        -- wait_for_cleanup
  false        -- dry_run
);
```

## See also

- [PostgreSQL DDL](postgres/ddl.md) — ordinary table and column DDL.
- [Vector search API](vector-search/api.md) — Python, SQL and pgwire search.
- [Full-text search](full-text-search/overview.md) — text index and BM25 API.
- [Vector search quick start](vector-search/quickstart.md).
