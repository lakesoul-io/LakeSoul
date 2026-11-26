# Agents Need Their Own Lakehouse, Not a More Complex Data Stack

**LakeSoul 4.0 series: one PostgreSQL entry point, with CDC, ETL, OLAP, vector search, and governance in one engine.**

An agent trying to answer “does this customer have a refund risk?” may wake up at least five systems: CDC extracts orders from the operational database, ETL cleans them into OLAP, a vector database stores contract embeddings, a full-text index stores another copy, and a permissions system sits somewhere else. Each has its own API, deployment, credentials, and consistency model. The agent asked one question; the platform made it traverse a data stack.

This is not heavy. It is fragmented. Heavy is not necessarily the problem; fragmented is: data is copied multiple times, permissions are split, the agent has to learn multiple APIs, and one broken link makes the answer untrustworthy.

This is the thesis: agents need their own lakehouse. “Their own” does not mean a separate data silo per agent. It means the same lakehouse, accessed, governed, and billed in the way agents work.

<!-- truncate -->

LakeSoul 4.0 series puts that into one protocol entry point and four capabilities: a standalone PostgreSQL wire endpoint; SQL-level incremental maintenance; lake-resident vector and BM25 indexes; and the transaction, subscription, time-travel, and JWT/RBAC foundation that makes an agent safe to use.

## 1. Complexity is not an adjective; it is a structure

A typical data platform's complexity can be counted:

- Ingestion: CDC, connectors, ETL, one pipeline per source.
- Compute: OLAP, vector, full-text, several engines and query interfaces.
- Operations: deploy, scale, upgrade, monitor, and credential every system.
- Governance: permissions, audit, and lineage spread across systems; least privilege for an agent is hard to define.

Every extra system adds a data copy, a permission model, a failure domain, and an API the agent must learn. The hard part is not the systems themselves; it is the seams between them: how data moves, how permissions sync, how schemas align, how failures retry. Every agent question crosses those seams.

Heavy platforms are not wrong; they encapsulate complexity in a platform team. For teams without one, that complexity lands on users and agents. DuckDB solves single-node analytics, but not shared access, continuous writes, unified governance, or lake-resident indexes. The missing piece is a continuous path: the same data, the same entry point, and the same permissions from one machine to a cluster.

The entry point should be thin, and the engine thick.

## 2. LakeSoul 4.0: The Lakehouse Engine

Zoom out: LakeSoul 4.0 is not a PostgreSQL plugin, and not another siloed data platform. It is a lakehouse engine: data lives in object storage, metadata lives in PostgreSQL, and SQL, incremental maintenance, indexes, and distributed execution run in a Rust/DataFusion engine.

![LakeSoul 4.0 architecture](/img/blog/lakesoul-4.0-architecture.png)

*Figure: LakeSoul 4.0's three interfaces, one engine, PostgreSQL metadata, and object storage.*

From the outside, it has three interfaces:

- **PostgreSQL wire**: psql, JDBC, psycopg, SQLAlchemy, and BI tools connect directly; the agent learns no new API.
- **Python native**: Arrow, Pandas, DuckDB, Daft, Ray, PyTorch, and HuggingFace read and write the same table in shards, without a SQL gateway.
- **Connectors**: Spark, Flink, and Presto join through the LakeSoul connector; existing batch and streaming jobs do not move.

From the inside, the design reduces to five pieces:

- **One distributed execution engine**: DataFusion SQL, incremental maintenance, vector/full-text search, merge-on-read, cache, and coordinator/worker distributed execution.
- **One control plane**: PostgreSQL stores the catalog, partition versions, commits, snapshots, and index leases.
- **One storage layer**: object storage holds Parquet/Vortex files and vector/text index segments.
- **One governance boundary**: JWT, RBAC, domain isolation, table-level `GRANT`/`REVOKE`, and TLS take effect at the protocol entry point.
- **One ingestion layer**: foreign tables (FDW) map external databases directly, and CDC subscriptions write source changes into LakeSoul CDC tables, without a separate pipeline for every source.

This is not five systems stuffed into one process. It is those capabilities sharing one entry point, one catalog, and one table; the compute layer still scales out.

The separation pays off directly in elasticity: metadata lives in PostgreSQL, data lives in object storage, and compute is stateless coordinator/worker. Ingestion, ETL, and OLAP can each spin up new compute clusters or endpoints and scale independently; multiple compute clusters can share one catalog, so concurrent agents can be isolated by endpoint or domain instead of competing for one database process.

Python native is a major part of LakeSoul 4.0. It is not “one more SDK”: it reads object storage directly, so the same table can be sharded across Daft, Ray, and PyTorch without exporting data to another system. Embodied data does not need a second storage system either: LeRobot datasets and MCAP recordings become ordinary LakeSoul tables, and video, state, action, and training windows share the same catalog, permissions, snapshots, and incremental machinery.

The same data can also be read as of a past instant: `SET lakesoul.read.time_travel = '2026-11-20T10:00:00+08:00'` moves the connection back to that point, so yesterday's conclusion can be reproduced.

## 3. Incremental SQL: cost follows data change

Agents amplify query frequency. If every query rescans the full table, cost grows linearly with the agent's loop count. That is the easiest economic problem to miss, and one of the most damaging.

LakeSoul's incremental SQL turns `INSERT ... SELECT` into a continuously maintained statement. The first run builds the target; later runs read only the commits after the source cursor and write only the rows that changed. The target commit and the cursor advance in one step, so a retry skips a window that was already applied. For supported query shapes, the result is identical to a full recompute; unsupported shapes fall back explicitly or fail, rather than silently returning a wrong answer.

It does not add materialized views, stream tables, dynamic tables, or refresh operations. The target is an ordinary table: turn on incremental maintenance, run the same `INSERT ... SELECT`, and the engine applies only the delta. Airflow, cron, or your application still decides when to run it; the table stays a table, and the SQL stays SQL.

```sql
-- daily_revenue is already created as an incremental target
SET lakesoul.ivm.enabled = on;
INSERT INTO daily_revenue (day, region, revenue)
SELECT date_trunc('day', ts), region, sum(amount)
FROM orders GROUP BY 1, 2;
```

IVM is not a universal cache. It maintains the derived table you declared; an agent's exploratory queries still scan normally. It solves repeated daily reports, aggregates, and joins, not “every query becomes cheap.”

## 4. Vector and text: the index belongs to the table

Agents need vectors for RAG, semantic search, and multimodal understanding. The usual approach copies them into a separate vector database, which brings data movement, dual-write consistency, and a split governance boundary. LakeSoul's choice is different: a vector is not another system; it is a secondary index on a lakehouse table.

Rows stay in the table files; the index is IVF + RaBitQ, stored in object storage, and maintained incrementally on write. It lives with the table, without a separate vector database to operate.

On the client side, the LakeSoul endpoint is directly compatible with pgvector's Python and Java clients: the `vector(n)` type and `<->`, `<#>`, `<=>` operators work unchanged, so a RAG application can connect without rewriting its SQL. Structured filters, vector recall, and full-text search can be combined in one SQL statement, without glue between systems.

```sql
SELECT id, title, embedding <-> $1::vector(8) AS distance
FROM documents
WHERE tenant_id = 'acme' AND category = 'contract'
ORDER BY embedding <-> $1::vector(8)
LIMIT 10;
```

The same engine also has BM25 full-text search: `text_match` for filtering and `text_score` for ranking. It is not a vector database and not a full-text search engine; it is an index on a lakehouse table, governed and maintained with the table.

## 5. Python native: the same data, straight into Daft

A typical embodied-data workflow is import → quality checks → split → training windows. The example below covers import and read; the remaining checks, splits, and windowing continue with Daft expressions/UDFs, without leaving the lakehouse. It needs `pip install 'lakesoul[embodied,daft]'`; see the Embodied data docs for the full setup.

```python
from lakesoul import LakeSoulCatalog
from lakesoul.embodied.daft import import_lerobot_gop

catalog = LakeSoulCatalog.from_env(namespace="default")

# Import a LeRobot dataset through Daft into ordinary LakeSoul tables
import_lerobot_gop(
    "/data/lerobot/pusht",
    table="robot_episodes",
    path="s3://bucket/lakesoul/robot_episodes",
    catalog=catalog,
)

# The same table is sharded through Daft, with no SQL gateway
episodes = catalog.scan("robot_episodes").to_daft()
episodes.where(episodes["episode_id"] == "ep000001").show()
```

There is no “export to another system” step: import, read, and shard on the same lakehouse data. Daft handles scheduling; LakeSoul handles metadata, snapshots, permissions, and IO; the same scan can also go to Ray or PyTorch.

## 6. One chain: a real agent task

Put the pieces together in one task. A support agent receives: “Which refund clauses in APAC contracts over the last 30 days could affect renewals? Sort by order amount.”

1. CRM `users` and `orders` flow into LakeSoul through a PostgreSQL subscription, continuously catching up. A human configures it once; the agent never touches a CDC pipeline.
2. `contracts` has `embedding` and `body`; one SQL statement combines tenant/region filters, vector recall, and BM25 filtering, then orders by order amount.
3. `daily_revenue` is maintained by incremental SQL, so the agent does not recompute the full aggregate every time it asks about the last 30 days.
4. The agent's Python tool reads the same table through the LakeSoul SDK and shards contract text to Daft or Ray for multimodal extraction; heavy compute does not go through the SQL gateway.
5. To reproduce yesterday's answer, `SET lakesoul.read.time_travel` returns the orders and daily revenue to that instant. Long jobs can use `lakesoul_submit` to detach from the connection; after a disconnect, another connection can check status or cancel.

There are no three pipelines and no three copies. The table a BI dashboard queries is the table a Python job reads and the table an agent queries over PostgreSQL. Control and ad-hoc queries go through PG; heavy compute reads directly from Python. One table, three interfaces. Existing Spark, Flink, and Presto jobs do not need to move either; they read and write the same table through the LakeSoul connector.

## Closing

Agents do not need five systems. They need a protocol they already speak, a table they can read and write, and one permission boundary.

Thin entry: agents connect over PostgreSQL without learning a new API. Thick engine: incremental maintenance, vector and full-text search, federation, CDC, transactions, and permissions in one engine. Continuous path: from a single-node all-in-one to a distributed cluster over object storage, the SQL and table format stay the same; once the data is in object storage, scaling means adding compute endpoints or workers, not moving data or rewriting SQL.

> Agents do not need a more complex data platform. They need a lakehouse that speaks their language, thinks incrementally, and grows with them.

## Quick start

```bash
# PostgreSQL endpoint: build the all-in-one image first (see Quick Start)
nix build .#all-in-one-image
./result | docker load
docker compose -f docker/lakesoul-all-in-one/compose.yaml up -d
psql "postgresql://lakesoul:lakesoul@127.0.0.1:5433/default"

# Python SDK: read the same tables directly
pip install lakesoul
```

- Quick Start: https://lakesoul-io.github.io/docs/get-started/quickstart-postgres
- Time travel: https://lakesoul-io.github.io/docs/engine/postgres/time-travel
- Incremental SQL: https://lakesoul-io.github.io/docs/engine/incremental-sql/overview
- Vector search: https://lakesoul-io.github.io/docs/engine/vector-search/api
- Embodied data: https://lakesoul-io.github.io/docs/python/multimodal/overview
- GitHub: https://github.com/lakesoul-io/LakeSoul
- Discord: https://discord.gg/WJrHKq4BPf
