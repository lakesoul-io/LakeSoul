<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

<img src='https://github.com/lakesoul-io/artwork/blob/main/horizontal/color/LakeSoul_Horizontal_Color.svg' alt="LakeSoul" height='200'>

<img src='https://github.com/lfai/artwork/blob/main/other/lfaidata-project-badge/sandbox/color/lfaidata-project-badge-sandbox-color.svg' alt="LF AI & Data Sandbox Project" height='180'>

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![PG wire](https://img.shields.io/badge/PG%20wire-DDL%20%2B%20INSERT%20%2B%20query-green.svg)](#postgres-native)
[![pgvector](https://img.shields.io/badge/pgvector-compatible%20SQL-purple.svg)](#vector-native)
[![Python](https://img.shields.io/badge/Python-3.11%2B-yellow.svg)](#python-native)
[![Daft](https://img.shields.io/badge/Daft-multimodal-orange.svg)](#python-native)
[![Release](https://img.shields.io/github/v/release/lakesoul-io/LakeSoul?include_prereleases&sort=semver)](https://github.com/lakesoul-io/LakeSoul/releases)

[中文介绍](README-CN.md)

# LakeSoul

**Postgres-native. Vector-native. Python-native.**<br>
An open lakehouse engine — a shared catalog, ACID upserts, SQL, incremental
compute, vector search and a Python SDK, in Rust, over your object storage.

Point the tools you already use at it — `psql`, Python, pgvector-compatible SQL — and
get an incremental lakehouse underneath.

[Quick Start](#quick-start) · [Architecture](#architecture) · [Docs](https://lakesoul-io.github.io/docs/engine/overview)

## Postgres-native

Point any PostgreSQL client at LakeSoul. `CREATE TABLE`, `INSERT ... ON CONFLICT`
and queries run in the lakehouse engine, against object storage — not in a
sidecar, and not through a CDC pipeline.

```sql
CREATE TABLE orders (
  id     bigint primary key,
  region text,
  amount numeric(20,4)
) WITH (hash_bucket_num = 8);

INSERT INTO orders VALUES (1, 'APAC', 42.00)
ON CONFLICT (id) DO UPDATE
SET region = EXCLUDED.region,
    amount = EXCLUDED.amount;

SELECT region, sum(amount) FROM orders GROUP BY 1;
```

The wire protocol, the extended query protocol, `pg_catalog` and `psql \d` all
work. This is not a drop-in PostgreSQL: `UPDATE`, `DELETE`, `COPY`, `RETURNING`
and non-`READ COMMITTED` isolation are refused with `0A000`, and writes must be
autocommit.

### PostgreSQL subscriptions

The same endpoint can subscribe to a source PostgreSQL database and keep a
LakeSoul CDC table in sync. Define the subscription with SQL; the endpoint
validates the publication and target, takes a managed snapshot, and starts a
supervised local subscriber process.

```sql
CREATE SUBSCRIPTION users_from_app
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
WITH (
  source_table     = 'public.users',
  target_table     = 'users',
  snapshot         = 'managed',
  auto_create      = true,
  schema_evolution = 'auto'
);
```

The source needs logical WAL, a publication, and `REPLICA IDENTITY FULL`; the
target is a LakeSoul CDC table that can be created automatically from the source
schema or pre-created; a managed snapshot requires it to be empty. The
subscriber adds nullable columns and widens safe types when the source schema
changes. The initial copy runs in parallel across CTID ranges (`snapshot_parallelism`, `snapshot_memory_bytes`).
`ALTER SUBSCRIPTION ... DISABLE/ENABLE` and `DROP SUBSCRIPTION` control the
process. A subscription can map one or more source tables; each table snapshots
and commits independently. See
[PostgreSQL subscriptions](https://lakesoul-io.github.io/docs/engine/postgres/subscription)
for the full setup and SQL reference.

## Incremental SQL

With incremental SQL enabled for the session, an `INSERT ... SELECT` issued from
`psql` updates its target incrementally: each run reads only the commits since its
cursor and writes only the changed rows. The result is identical to a full
recompute.

```sql
-- The target is a keyed table carrying the CDC change column "rowKinds".
-- This switch enables incremental SQL for the session.
SET lakesoul.ivm.enabled = on;

INSERT INTO daily_revenue
SELECT date_trunc('day', ts) AS day, region, sum(amount)
FROM orders
GROUP BY 1, 2;
```

- SQL-level incremental, not just storage-level incremental read/write
- Replay-safe: every consumed window is recorded, so a retry skips data that is already written

## Vector-native

pgvector-compatible SQL over ANN indexes that live next to the data. New rows update
the index; there is no rebuild and no second system to operate.

```sql
-- documents.embedding is vector(768)
SELECT id, body, embedding <-> '[0.12, 0.98, ...]' AS distance
FROM documents
WHERE tenant_id = 'acme' AND category = 'contract'
ORDER BY embedding <-> '[0.12, 0.98, ...]'
LIMIT 10;
```

- IVF + RaBitQ index on object storage, maintained on write
- Structured filters and ANN in one plan
- BM25 full-text search (`text_score`) in the same engine

## Python-native

The Python SDK reads object storage directly through the Rust IO layer. No
gateway, no JDBC hop — Daft and Ray shard the read across workers, PyTorch gets a
dataset, Pandas and DuckDB consume Arrow.

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
scan = catalog.scan("documents").select("id", "body")
table = scan.to_arrow_table()   # or .to_daft(), .to_ray(), .to_torch()
```

The same table your BI dashboard queries is the table your training job reads.
Video frames, robot episodes and tensors live as blobs beside their metadata.

## Quick Start

### Option A — the PostgreSQL endpoint

```bash
# 1. Metadata database
docker run -d --name lakesoul-pg -p 5432:5432 \
  -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test \
  -e POSTGRES_DB=lakesoul_test postgres:18
docker cp script/meta_init.sql lakesoul-pg:/
docker exec -i lakesoul-pg sh -c \
  "PGPASSWORD=lakesoul_test psql -U lakesoul_test -f meta_init.sql"

# 2. The PostgreSQL endpoint: database "default" is the LakeSoul namespace
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix file:///tmp/lakesoul/warehouse

# 3. Connect with any PostgreSQL client
psql "postgresql://lakesoul_test:lakesoul_test@127.0.0.1:5433/default"
```

```sql
CREATE TABLE t (id bigint primary key, v text);
INSERT INTO t VALUES (1, 'hello');
SELECT * FROM t;
```

### Option B — Python

```bash
pip install lakesoul
```

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
print(catalog.scan("t").to_arrow_table().num_rows)
```

See the [documentation](https://lakesoul-io.github.io/docs/get-started/quickstart-postgres) for object
storage configuration and the full local setup.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  psql · JDBC · BI tools      Python SDK · Daft · Ray · Torch │
└───────────────┬──────────────────────────┬───────────────────┘
                │ PG wire                  │ native IO / Arrow
┌───────────────▼──────────────────────────▼───────────────────┐
│                        LakeSoul engine                       │
│  PostgreSQL front end   ·   incremental SQL                  │
│  vector + full-text indexes   ·   vectorized execution       │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  object storage (S3 / OSS / HDFS)  +  PostgreSQL metadata    │
└──────────────────────────────────────────────────────────────┘
```

- **PostgreSQL front end** — wire protocol, `pg_catalog`, DDL and `INSERT`,
  executed by the engine rather than by a Postgres extension
- **Incremental SQL** — each run consumes the table's own commit log and writes
  only changed rows, replay-safe
- **Vector and full-text indexes** — IVF + RaBitQ and Tantivy BM25, maintained on
  write
- **Native IO** — Parquet, Vortex and Vortex Compact over object storage, with
  merge-on-read for primary-key tables

## How it compares

A table format only describes file layout: no engine, no indexes, no incremental
state. A database ships an engine, but owns the storage. LakeSoul is neither: the
catalog, the storage format, the execution engine and the indexes are one
codebase, while the data stays in your object storage, in open files.

| | Postgres extension | Postgres + columnar mirror | Managed Postgres | LakeSoul |
| --- | --- | --- | --- | --- |
| **Data lives in** | your Postgres tables, or Iceberg through an FDW | Postgres WAL plus a columnar copy | the provider's storage | **your object storage, in open files** |
| **Executes in** | the Postgres process | Postgres and the mirror | the provider's service | **the lakehouse engine** |
| **Write model** | Postgres MVCC | logical decoding into the mirror | Postgres MVCC | **primary-key upsert + merge-on-read** |
| **Incremental compute** | — | — | — | **incremental SQL over the commit log** |
| **Vector index** | pgvector on heap | — | pgvector | **lake-resident, maintained on write** |
| **Self-hosted** | yes | yes | no | **yes, Apache 2.0** |

Categories, not vendor scorecards: examples are pg_duckdb and pg_lake,
pg_mooncake, and Lakebase. The dated, sourced comparison lives in the docs.

## Works with your stack

- **PostgreSQL clients** — `psql`, pgAdmin, DBeaver, JDBC, libpq
- **Python** — Pandas, PyArrow, DuckDB, Daft, Ray, PyTorch, Hugging Face Datasets
- **Engines** — Spark 3.5, Flink 1.20, Presto 0.296
- **BI** — Superset, Metabase, and anything else that speaks PostgreSQL

## Community

- [Discord](https://discord.gg/WJrHKq4BPf) for questions and design discussions
- [GitHub issues](https://github.com/lakesoul-io/LakeSoul/issues) for bugs and
  feature requests — look for `good first issue` to start contributing
- [Community guidelines](dev/community/guideline.md)
- Mailing list: [lakesoul-technical-discuss@lists.lfaidata.foundation](mailto:lakesoul-technical-discuss@lists.lfaidata.foundation)
- Roadmap: [ROADMAP.md](ROADMAP.md)

LakeSoul is a sandbox project of [LF AI & Data](https://lfaidata.foundation/).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
