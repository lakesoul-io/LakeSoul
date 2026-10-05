<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

# LakeSoul IVM (incremental materialized views)

`lakesoul-ivm` maintains materialized views (MVs) incrementally from the
LakeSoul changelog: a refresh consumes only the source commits since the last
cursor and writes just the changed MV rows. Every consumed window is recorded
in `ivm.epochs` (see [EPOCH.md](EPOCH.md)), so retries and concurrent
behaviour are replay safe. [PLAN.md](PLAN.md) holds the design notes and the
implementation history.

The crate has three layers:

| Layer | Module | Responsibility |
|---|---|---|
| Runtime | [`runtime`](src/runtime.rs) | Typed view kinds (`SumCountView`, `WindowView`, `JoinView`, ...), cursors, epochs, MV and state tables. |
| SQL entry | [`sql`](src/sql.rs) + [`executor`](src/executor.rs) | Analyzes `INSERT INTO <mv> SELECT ...` into a view kind and decides bootstrap / incremental / rebuild. |
| Providers | [`provider`](src/provider.rs) | A DataFusion `TableProvider` for logical reads (current, as-of, pinned epoch, raw). |

## SQL entry

```sql
INSERT INTO <mv_table> SELECT ...;      -- maintain the view
INSERT OVERWRITE <mv_table> SELECT ...; -- recompute the view from scratch
```

The statement is interpreted as **incremental maintenance** of the target
table, not as a plain data insert: the first execution bootstraps the view from
the current source state, later executions consume the source changelog and
apply only the delta. `INSERT OVERWRITE` keeps full-overwrite semantics and
recomputes the view from the current source state.

* The target MV table must already exist and its schema must match the view
  (the executor validates it before any side effect).
* The view id is the target table id. A changed definition (a different
  `SELECT`) bumps the generation, resets the cursors, drops and recreates any
  generated state tables and rebuilds the view.
* The execution reports what it did (`IvmExecution::action`):
  `bootstrap`, `incremental`, `rebuild` or `overwrite`.

Ordinary SQL execution is untouched; callers opt in by going through
[`IvmSqlExecutor`](src/executor.rs).

## Supported query shapes

Sources are keyed (a LakeSoul primary key, i.e. upsert semantics) or
append-only. Aggregates and windows follow SQL NULL semantics; primary keys
used as row identities must be non-nullable.

| Kind | SQL shape | Materialized columns |
|---|---|---|
| Projection / filter | `SELECT [expr AS c, ...] FROM src [WHERE p]` | projected columns, `rowKinds`, `__ivm_epoch` |
| SELECT DISTINCT | `SELECT DISTINCT c, ... FROM src [WHERE p]` | the distinct columns, `count_v`, kinds, epoch |
| SUM / COUNT / AVG | `SELECT k, SUM(v), COUNT(*), AVG(v) FROM src [WHERE p] GROUP BY k [HAVING h]` | keys, `sum_v`, `count_v`, `__ivm_nonnull_count` (`avg_v` for AVG), kinds, epoch |
| MIN / MAX | `SELECT k, MIN(v) FROM src GROUP BY k` | keys, `value`, kinds, epoch (+ value-count state table) |
| COUNT / SUM DISTINCT | `SELECT k, COUNT(DISTINCT v) FROM src GROUP BY k` | keys, `value`, kinds, epoch (+ state) |
| Variance / stddev | `VAR_SAMP`, `VAR_POP`, `STDDEV_SAMP`, `STDDEV_POP`, `STDDEV` | keys, `variance_v` / `stddev_v`, kinds, epoch |
| MEDIAN | `SELECT k, MEDIAN(v) FROM src GROUP BY k` | keys, `median_v`, kinds, epoch |
| STRING_AGG | `SELECT k, STRING_AGG(v, ',' ORDER BY o) FROM src GROUP BY k` | keys, `string_agg_<v>` or `string_agg_value`, kinds, epoch |
| ARRAY_AGG | `SELECT k, ARRAY_AGG(v ORDER BY o) FROM src GROUP BY k` | keys, `array_agg_<v>` or `array_agg_value`, kinds, epoch |
| Window | `SELECT k, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) FROM src` | partition keys, source primary keys, one column per function, kinds, epoch |
| TOP-K | `SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn FROM src) t WHERE rn <= k` | projected columns, kinds, epoch |
| Inner join | `JOIN` on equality keys, both sides keyed or both append-only | join keys, `left_value`, `right_value`, `__left_pk_*`, `__right_pk_*`, kinds, epoch |
| Lookup join | `LEFT JOIN` where the right side is keyed by the join keys (they may differ in name) | join keys, `left_value`, `right_value`, left primary keys, kinds, epoch |
| CROSS JOIN | `CROSS JOIN` / `FROM a, b`, both sides keyed | `left_value`, `right_value`, `__left_pk_*`, `__right_pk_*`, kinds, epoch |
| LEFT / FULL / RIGHT JOIN | outer equi-joins, both sides keyed | as the inner join, with nullable unmatched identities |
| UNION ALL | `SELECT ... UNION ALL SELECT ...` | the projected columns, `__ivm_source`, kinds, epoch |
| UNION | `SELECT ... UNION SELECT ...` | the projected columns (the CDC column is excluded), `count_v`, kinds, epoch |
| Semi / anti join | `WHERE [NOT] EXISTS (SELECT ...)` / `x IN (SELECT ...)` | the projected left columns, kinds, epoch |

Supported within the shapes above:

* **group keys** may be scalar expressions with a `SELECT` alias
  (`SELECT v % 10 AS bucket, SUM(v) ... GROUP BY bucket`);
* **aggregate arguments** may be scalar expressions for all value aggregates,
  including `SUM(v * 2)`, `VAR_SAMP(v * 2)` and
  `STRING_AGG(CAST(v AS VARCHAR), '|')`;
* **ordering** may be a scalar expression in windows, TOP-K and the ordered
  aggregates (`ORDER BY v % 10`);
* **aggregate `FILTER (WHERE ...)`**, `HAVING`, and per-branch `WHERE` in set
  operations;
* **set-operation projections**: union branches may prune, rename and compute
  columns as long as every branch has the same output schema; a keyed
  `UNION ALL` branch must keep its primary keys as plain columns, and the CDC
  change column cannot be part of a `UNION` output;
* **semi/anti joins** through correlated `EXISTS` / `NOT EXISTS` / `IN`
  subqueries, including extra comparison conditions between the two sides;
* a **lookup `LEFT JOIN`** may reference a differently named right key
  (`ON fact.dim_id = dim.id`), as long as the right source is keyed by it;
* **CTEs and derived tables** that the planner can inline (`WITH ... SELECT`,
  `SELECT ... FROM (SELECT ...) t`), including CTEs whose body aggregates or
  windows; the view is maintained as the inlined shape;
* window `PARTITION BY`/`ORDER BY` columns or expressions, custom frames,
  `FILTER (WHERE ...)` and `IGNORE NULLS`;
* **several window clauses** with different `PARTITION BY`/`ORDER BY` in one
  statement (chained windows): the MV is then keyed by the source primary keys
  and materializes each clause's partition keys as value columns.

### Derived column names

Aggregates and internal columns are named deterministically, so the MV can be
queried without guessing:

| Expression | Column |
|---|---|
| `SUM(v)` | `sum_v` |
| `COUNT(*)`, `COUNT(v)` | `count_v` |
| `AVG(v)` | `avg_v` (and `__ivm_nonnull_count`) |
| `MIN(v)` / `MAX(v)`, `COUNT(DISTINCT v)` | `value` (state tables use `value` / `value_count`) |
| `VAR_*` / `STDDEV_*` | `variance_v` / `stddev_v` |
| `MEDIAN(v)` | `median_v` |
| `STRING_AGG(v, ...)` | `string_agg_v`; `string_agg_value` for an expression argument |
| `ARRAY_AGG(v ...)` | `array_agg_v`; `array_agg_value` for an expression argument |
| `ROW_NUMBER`/`RANK`/`DENSE_RANK`/`NTILE`/`PERCENT_RANK`/`CUME_DIST` | the `SELECT` alias, otherwise the function name |
| `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` | the `SELECT` alias, otherwise `lag_v`, `lead_v`, `first_value_v`, `last_value_v`, `nth_value_v` |
| `UNION ALL` branch index | `__ivm_source` |
| `UNION` occurrence count | `count_v` |
| keyed join identities | `__left_pk_<key>` / `__right_pk_<key>` |

Every MV additionally carries the bookkeeping columns `rowKinds`
(`insert` / `delete`) and `__ivm_epoch`. A refresh writes the retraction of a
key before its new value in one commit, and the internal stable sort keeps the
two adjacent so merge-on-read resolves the key to the new version.

The MV primary keys are the logical identity: group keys for aggregates,
partition keys plus source primary keys for windows, the projected identities
for row/semi/anti views, the join identities for joins, and the data columns
for `UNION` (or the data columns plus `__ivm_source` for `UNION ALL`).

## Sources

* A **keyed** source (LakeSoul primary key) supports upserts, deletes and
  join-key changes: the refresh reads the changed keys' previous state as of
  the window start and retracts it.
* An **append-only** source only accumulates rows. Joins require both sides to
  be keyed or both append-only; `UNION` branches must all be keyed or all
  append-only.
* A source may declare a CDC change column (`lakesoul_cdc_change_column`,
  [`IvmTableOptions::with_cdc_column`](src/table.rs)): `delete` retracts and
  `update_before`/`update_after` pair an update. Without one the internal
  `rowKinds` column is used. The change column is never part of a `UNION`
  distinct key.
* Tables consumed by an IVM view must keep LakeSoul's default retention: the
  refresh needs the `partition_info` / `data_commit_info` history back to the
  cursors. See the retention notes in [`lib.rs`](src/lib.rs).

## Reading materialized views

[`IvmRuntime::table_provider`](src/runtime.rs) builds a DataFusion provider
over an internal table:

| Mode | Meaning |
|---|---|
| `Current` | the logical current state; CDC tombstones (`delete`) are hidden |
| `AsOf(ms)` | the state at a timestamp |
| `AtVersions(...)` | the state pinned to an epoch's partition versions |
| `Raw` | the physical merge-on-read rows, including tombstones |

Logical reads drop tombstone rows by default, so consumers see the MV as an
ordinary table.

## Refresh semantics

* Every source partition has a cursor (last consumed version/timestamp). A
  refresh collects the changelog window per partition, applies the delta and
  advances the cursors.
* The window identity (source, partition, from/to versions) is recorded in
  `ivm.epochs`. Re-applying the same window is a no-op; a partially written
  window is completed by the retry because the affected keys are rewritten.
* A view whose source history is no longer consumable (for example a compaction
  that removed the cursor versions) must be rebuilt; the metadata signals this
  through `requires_rebuild`.
* One writer per view: refreshes of the same view serialize through the epoch
  protocol.

## Observability

The runtime and the SQL entry emit [`metrics`](https://docs.rs/metrics)
counters and histograms, so any recorder (for example the Prometheus exporter
used by the IO layer) can export them:

| Metric | Labels | Meaning |
|---|---|---|
| `lakesoul_ivm_statements_total` | `action` (`bootstrap` / `incremental` / `rebuild` / `overwrite` / `error`) | maintenance statements |
| `lakesoul_ivm_statement_duration_seconds` | `action` | statement duration |
| `lakesoul_ivm_refreshes_total` | `kind`, `result` (`applied` / `noop`) | view refreshes |
| `lakesoul_ivm_refresh_duration_seconds` | `kind` | refresh duration |
| `lakesoul_ivm_rebuilds_total` | `kind` | full rebuilds |
| `lakesoul_ivm_rebuild_duration_seconds` | `kind` | rebuild duration |
| `lakesoul_ivm_epochs_total` | `kind` | committed epochs |

The `kind` label is the view kind (`sum_count`, `min_max`, `window`, `join`,
`union_distinct`, ...); view and table ids are never used as labels.

## Rust API

```rust
use lakesoul_ivm::{IvmRuntime, IvmTableOptions, SumCountView, sum_count_mv_schema_for};

// `schema` is the Arrow schema of the view (the source schema with the
// derived aggregate column).
let runtime = IvmRuntime::from_env().await?;      // LAKESOUL_PG_* configuration
let source = runtime.open_table("orders", "default").await?;
let mv = runtime.create_table(
    IvmTableOptions::new("orders_mv", "file:///tmp/orders_mv", schema)
        .with_primary_keys(vec!["customer".into()]),
).await?;

let view = SumCountView::new(
    "orders_view",
    source,
    mv,
    "customer",
    Some("amount".to_string()),
);
runtime.refresh_sum_count(&view).await?;          // incremental
runtime.rebuild_sum_count(&view).await?;          // full recompute
```

The typed views mirror the SQL shapes above; `register_*` persists the spec,
`refresh_*` consumes the changelog and `rebuild_*` recomputes from the full
source state.

## Limitations

The following shapes are currently rejected (see [PLAN.md](PLAN.md) §10.5 for
the backlog):

* range-partitioned source tables: the reads do not carry the partition values
  through the IO layer yet, and the join / row / union / TOP-K views reject them
  explicitly. Supporting them needs a dedicated change (see the backlog);
* `CROSS JOIN` with a `WHERE` clause, three or more table joins, non-equality
  join keys, differently named keys outside the lookup `LEFT JOIN`, and
  multiple payload columns per side;
* scalar subqueries (`(SELECT ...)` in the select list or in a comparison,
  e.g. `WHERE x = (SELECT ...)`) and computed columns above an aggregate
  (`SELECT s * 2 FROM (SELECT SUM(v) AS s ...) t`); correlated `EXISTS` /
  `IN` subqueries are supported as semi/anti joins;
* `GROUPING SETS` / `ROLLUP` / `CUBE`, `COUNT(DISTINCT a, b)`;
* `SELECT DISTINCT ON`.

## Tests

The crate has three test layers:

* SQL entry end-to-end scripts (`tests/slt/*.slt`) driven by `tests/sqllogic.rs`;
* differential oracles against full recomputes (`tests/sql_oracle.rs`);
* typed runtime tests (e.g. `tests/join_keyed.rs`, `tests/window_refresh.rs`);
* typed coverage for `Float64` / `Decimal128` / `Date32` / `Boolean` inputs
  (`tests/slt/typed_*.slt`), including NULL handling and the non-NULL count.

```sh
# PostgreSQL is required; the tests use the LAKESOUL_PG_* environment.
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
cargo test -p lakesoul-ivm -- --test-threads=1
```
