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

Every source is keyed (a LakeSoul primary key, i.e. upsert semantics);
append-only sources are rejected (see [Sources](#sources)). Aggregates and
windows follow SQL NULL semantics; primary keys
used as row identities must be non-nullable.

| Kind | SQL shape | Materialized columns |
|---|---|---|
| Projection / filter | `SELECT [expr AS c, ...] FROM src [WHERE p]`; `p` may compare against an uncorrelated scalar subquery (`WHERE v > (SELECT AVG(v) FROM dim)`), whose tables are watched alongside the source (a keyed source only) | projected columns, `rowKinds`, `__ivm_epoch` |
| SELECT DISTINCT | `SELECT DISTINCT c, ... FROM src [WHERE p]` | the distinct columns, `count_v`, kinds, epoch |
| DISTINCT ON | `SELECT DISTINCT ON (g) g, v FROM src [ORDER BY ...]` over a keyed source; the picked row is deterministic (the source primary keys break ties) | keys, one column per picked column (`first_value_v`), kinds, epoch |
| SUM / COUNT / AVG | `SELECT k, SUM(v), COUNT(*), AVG(v) FROM src [WHERE p] GROUP BY k [HAVING h]`; the same aggregates without a `GROUP BY` (global, single-row MV) | keys, `sum_v`, `count_v`, `__ivm_nonnull_count` (`avg_v` for AVG), kinds, epoch |
| MIN / MAX | `SELECT k, MIN(v) FROM src GROUP BY k`; without a `GROUP BY` a global MIN/MAX | keys, `value`, kinds, epoch (+ value-count state table) |
| GROUPING SETS / ROLLUP / CUBE | `GROUP BY GROUPING SETS ((a, b), (a), ())` over a keyed source; plain or aliased computed keys; any mix of the supported aggregates (`SUM`/`COUNT`/`AVG` keep the incremental layout, other mixes are recomputed per set) | `__ivm_grouping`, every flat key (nullable), one column per aggregate (`sum_v`/`count_v`/`__ivm_nonnull_count`/`avg_v` for the `SUM`/`COUNT`/`AVG` layout), kinds, epoch |
| COUNT / SUM DISTINCT | `SELECT k, COUNT(DISTINCT v) FROM src GROUP BY k`; globally without a `GROUP BY`; multi-column `COUNT(DISTINCT a, b)` needs a `GROUP BY` | keys, `value`, kinds, epoch (+ state) |
| Variance / stddev | `VAR_SAMP`, `VAR_POP`, `STDDEV_SAMP`, `STDDEV_POP`, `STDDEV` | keys, `variance_v` / `stddev_v`, kinds, epoch |
| MEDIAN | `SELECT k, MEDIAN(v) FROM src GROUP BY k` | keys, `median_v`, kinds, epoch |
| BOOL_AND / BOOL_OR | `SELECT k, BOOL_AND(flag) FROM src GROUP BY k` over a Boolean column or expression | keys, `bool_and_<v>` / `bool_or_<v>` (`bool_and_value` for expressions), kinds, epoch |
| APPROX_DISTINCT | `SELECT k, APPROX_DISTINCT(v) FROM src GROUP BY k` | keys, `approx_distinct_<v>` (`approx_distinct_value` for expressions, `UInt64`), kinds, epoch |
| APPROX_PERCENTILE_CONT | `SELECT k, APPROX_PERCENTILE_CONT(v, 0.5) FROM src GROUP BY k`; also `APPROX_MEDIAN(v)` and the weighted `APPROX_PERCENTILE_CONT_WITH_WEIGHT(v, w, p)` | keys, `approx_percentile_cont_<v>` (`Float64`), kinds, epoch |
| Mixed aggregates | `SELECT g, SUM(v), MIN(v), MAX(v), COUNT(*) FROM src GROUP BY g` — any mix of the supported aggregate functions in one statement (a lone DISTINCT aggregate keeps its dedicated view; DISTINCT in a mix is rejected) | keys, one column per aggregate (`sum_v`, `min_v`, ...; COUNT(*) is `count`), kinds, epoch |
| Scalar aggregates | `BIT_AND`/`BIT_OR`/`BIT_XOR(v)`; `CORR`/`COVAR_SAMP`/`COVAR_POP(y, x)`; `REGR_SLOPE`/`REGR_INTERCEPT`/`REGR_COUNT`/`REGR_R2`/`REGR_AVGX`/`REGR_AVGY`/`REGR_SXX`/`REGR_SYY`/`REGR_SXY(y, x)`; `PERCENTILE_CONT(v, p)` | keys, `<function>_<arguments>` (the value type for the bit aggregates, `Float64`/`UInt64` otherwise), kinds, epoch |
| STRING_AGG | `SELECT k, STRING_AGG(v, ',' ORDER BY o) FROM src GROUP BY k` | keys, `string_agg_<v>` or `string_agg_value`, kinds, epoch |
| ARRAY_AGG | `SELECT k, ARRAY_AGG(v ORDER BY o) FROM src GROUP BY k` | keys, `array_agg_<v>` or `array_agg_value`, kinds, epoch |
| Window | `SELECT k, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) FROM src` | partition keys, source primary keys, one column per function, kinds, epoch |
| TOP-K | `SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn FROM src) t WHERE rn <= k` | projected columns, kinds, epoch |
| Inner join | `JOIN` on equality keys (the two sides may name them differently; several payload columns per side become wide output columns), both sides keyed, optional side filters and payload conditions | join keys (the left names), `left_value`/`right_value` or one column per selected payload (the alias, or the source name), `__left_pk_*`, `__right_pk_*`, kinds, epoch |
| Lookup join | `LEFT JOIN` where the right side is keyed by the join keys (they may differ in name, optional filters on either input; several payload columns per side become wide output columns) | join keys, `left_value`/`right_value` or one column per selected payload, left primary keys, kinds, epoch |
| Multi-way join | inner `JOIN`s over three to eight sources (all keyed), optional side filters and cross-source conditions; a source without a join key is cross joined, so `FROM a, b, c` and mixed keyless steps work | one column per selected payload (the alias, or the source name), `__pk<i>_<key>` per source row identity, kinds, epoch |
| Lookup chain | a left-deep `[LEFT] JOIN` chain over a keyed base where every step joins a source keyed by its join keys (`a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.v = b.v` — a step key may reference any earlier source), optional step filters | one column per selected payload (aliases name the step payloads), kinds, epoch |
| CROSS JOIN | `CROSS JOIN` / `FROM a, b`, both sides keyed, optional side filters and cross-side predicates; several payload columns per side become wide output columns | `left_value`/`right_value` or one column per selected payload, `__left_pk_*`, `__right_pk_*`, kinds, epoch |
| LEFT / FULL / RIGHT JOIN | outer equi-joins, both sides keyed, the keys may be named differently, optional filters on either input; several payload columns per side become wide output columns | as the inner join (or one column per selected payload), with nullable unmatched identities |
| UNION ALL | `SELECT ... UNION ALL SELECT ...` | the projected columns, `__ivm_source`, kinds, epoch |
| UNION | `SELECT ... UNION SELECT ...` | the projected columns (the CDC column is excluded), `count_v`, kinds, epoch |
| Semi / anti join | `WHERE [NOT] EXISTS (SELECT ...)` / `x IN (SELECT ...)`, optional side filters; a correlated scalar subquery compares against one aggregate row per correlated key | the projected left columns, kinds, epoch |
| Left aggregate | `SELECT ..., (SELECT AGG(w) FROM dim u WHERE u.k = s.k) AS m FROM src s` (a select-list correlated scalar subquery) | the projected left columns, the aggregate column, kinds, epoch |
| INTERSECT / EXCEPT | over unique-per-row join columns (the distinct and `ALL` variants); a NULL-capable join column matches `NULL` with `NULL` | the projected left columns, kinds, epoch |

Supported within the shapes above:

* **`GROUPING SETS` / `ROLLUP` / `CUBE`**: every grouping set is
  materialized in one MV keyed by `(__ivm_grouping, keys...)` — the keys a set
  does not group by are NULL — and a refresh recomputes the affected groups of
  every set, so a row moving in or out of a set (including the grand total)
  updates it; a `GROUPING(key)` column is materialized per set (0 for the sets
  that group by the key, 1 for the sets that aggregate it away, named after
  its alias or `grouping_<key>`); a `SUM`/`COUNT`/`AVG` statement keeps the
  incremental layout and any other mix of the supported aggregates is
  recomputed per set; keys may be plain columns or aliased expressions
  (`ROLLUP(g, v % 10 AS bucket)`);
* **`APPROX_DISTINCT`** and **`APPROX_PERCENTILE_CONT(v, p)`** (a literal
  percentile): the affected groups are recomputed from their current rows, and
  DataFusion's sketch updates are order independent, so the estimates are
  consistent with a full rebuild;
* **`BOOL_AND` / `BOOL_OR`** over booleans (a column or an expression such as
  `flag OR backup`): the affected groups are recomputed from their current
  rows, so NULL inputs are ignored exactly like DataFusion's implementation;
* **deterministic scalar aggregates**: `BIT_AND`/`BIT_OR`/`BIT_XOR`,
  `CORR`/`COVAR_SAMP`/`COVAR_POP`, the `REGR_*` family, `PERCENTILE_CONT`,
  `APPROX_MEDIAN` and the weighted approximate percentile: the affected groups
  are recomputed from their current rows (order-independent accumulators), so
  an incremental refresh matches a full rebuild;
* **global aggregates**: every supported aggregate without a `GROUP BY`
  (`SUM`/`COUNT`/`AVG`, `MIN`/`MAX`, `COUNT(DISTINCT)`/`SUM(DISTINCT)` and the
  variance, median, `STRING_AGG` and `ARRAY_AGG` families) keeps a single-row
  MV that is recomputed on every refresh; an empty source keeps the single
  aggregate row (`NULL`/`0`) and a failing `HAVING` leaves the MV empty;
* **group keys** may be scalar expressions with a `SELECT` alias
  (`SELECT v % 10 AS bucket, SUM(v) ... GROUP BY bucket`), including inside
  `GROUPING SETS` / `ROLLUP` / `CUBE`;
* **aggregate arguments** may be scalar expressions for all value aggregates,
  including `SUM(v * 2)`, `VAR_SAMP(v * 2)` and
  `STRING_AGG(CAST(v AS VARCHAR), '|')`;
* a **multi-column `COUNT(DISTINCT a, b)`** counts distinct tuples per group:
  the affected groups are recomputed from their current source rows (like the
  other unmergeable aggregates), so duplicates, updates and deletes move the
  count; it needs a `GROUP BY`, supports `HAVING` over the count and does not
  support `FILTER`;
* **ordering** may be a scalar expression in windows, TOP-K and the ordered
  aggregates (`ORDER BY v % 10`);
* **aggregate `FILTER (WHERE ...)`**, `HAVING`, and per-branch `WHERE` in set
  operations;
* **set-operation projections**: union branches may prune, rename and compute
  columns as long as every branch has the same output schema; a keyed
  `UNION ALL` branch must keep its primary keys as plain columns, and the CDC
  change column cannot be part of a `UNION` output;
* **semi/anti joins** through correlated `EXISTS` / `NOT EXISTS` / `IN`
  subqueries, including extra comparison conditions between the two sides and
  filters on either side (the outer `WHERE` filters the left source and a
  subquery predicate the right source): a row entering or leaving either
  filter gains or loses its match;
* **uncorrelated scalar subqueries** in a projection/filter view's `WHERE`
  compare a column against a global aggregate over other tables
  (`WHERE v > (SELECT AVG(v) FROM dim)`); the subquery tables are registered,
  watched and delete-filtered alongside the source, the source keeps the
  delta path and any change to a subquery table re-evaluates every key; the
  subquery must be a single global aggregate (no `GROUP BY`) over a keyed
  source, and select-list scalar subqueries or correlated ones are rejected;
* **correlated scalar subqueries** in a `WHERE` comparison
  (`WHERE v > (SELECT AVG(w) FROM dim u WHERE u.k = s.k)`) are maintained as
  a semi join against one aggregate row per correlated key: a change to the
  aggregate inputs re-evaluates only the left rows of the affected keys, a
  key without rows never matches (the subquery is NULL) and the comparison
  keeps SQL NULL semantics; the subquery must be one plain aggregate over a
  single table, and the left rows must be unique per correlated key; a
  **select-list** correlated scalar subquery
  (`SELECT ..., (SELECT AVG(w) FROM dim u WHERE u.k = s.k) AS m FROM src s`)
  is maintained as a left join against the same per-key aggregate, with NULL
  for a key without rows;
* **`INTERSECT` / `EXCEPT`** (the distinct and `ALL` variants) when the
  semi/anti semantics coincide with the set operation: the left rows are
  unique per join tuple (their primary key is covered), which also covers
  null-aware `IS NOT DISTINCT FROM` semi/anti predicates; a NULL-capable join
  column makes the view null-safe, so `NULL` matches `NULL` exactly as the
  set operations require;
* an **inner join**, a **cross join** and a pair-keyed **`LEFT JOIN`** may
  filter their sides (`WHERE fact.amount > 0 AND dim.active`): the analyzer
  keeps the predicates pushed below the join and a row entering or leaving
  its filter adds, retracts or NULL-pads its pairs;
* **`DISTINCT ON`**: one row per grouping key, the first row of the ordering
  (the source primary keys are appended as a deterministic tie-break; a
  key-only `SELECT DISTINCT ON (key) key` is rejected with a hint to write
  `SELECT DISTINCT` instead); the affected groups are recomputed from their
  current rows;
* **lookup chains**: a left-deep `[LEFT] JOIN` chain over a keyed base whose
  steps are keyed 1:1 lookups (the star-schema shape): a step key may
  reference any earlier source's column, so `c` can be looked up by a value
  `b` produced; the view keeps one row per base key: a refresh re-evaluates
  the base rows the changed keys touch (the base delta plus, per changed
  step, the base rows matching its old and new keys through the prefix
  chain; when an earlier source changed in the same window every base row is
  re-evaluated) by replaying the chain, so a missing step row NULLs its
  payload and a removed base row drops its row;
* **mixed aggregate kinds**: a statement may combine the supported aggregate
  functions (`SUM(v), MIN(v), MAX(v), COUNT(*)`, `AVG`, the variance family,
  `MEDIAN`, `APPROX_DISTINCT`, `STRING_AGG`, `ARRAY_AGG`, the bit / regression
  functions, ...) in one view; the affected groups are recomputed from their
  current rows, and a SUM/COUNT/AVG-only mix keeps the incremental sum/count
  view;
* **multi-way inner joins** (three to eight sources): the join tree flattens
  into one chained inner join; a source without a key pair is cross joined
  (the chain's `FROM a, b, c` and mixed keyless steps) and the MV is keyed by
  every source's row identity (`__pk0_*`, `__pk1_*`, ...); a refresh rewrites
  the tuples of the rows that changed on any source; side filters and
  non-equality conditions between any two sources are maintained;
* **wide join outputs**: an inner join, a cross join, a lookup `LEFT JOIN`
  or an outer join may select more than one column per side; each becomes an
  MV column under its select alias (or source name), at least one column per
  side is required, and for the inner and cross joins a non-equality
  condition may compare any two materialized columns (`ON l.k = r.k AND
  l.amount < r.limit`, or a cross join's `WHERE l.lo <= r.hi`); the compact
  single-payload `left_value` / `right_value` shape stays for one payload per
  side, and the outer / lookup wide outputs pad the unmatched side with NULLs;
* **non-equality join conditions over the payloads** (`ON l.k = r.k AND
  l.amount < r.limit`, or a cross join's `WHERE l.lo <= r.hi`): the condition
  is evaluated on each joined pair, so a payload change adds or retracts the
  affected pairs;
* an **inner join** and a **lookup `LEFT JOIN`** may reference differently
  named right keys (`ON fact.dim_id = dim.id`);
* a **lookup `LEFT JOIN`** may filter the fact side
  (`WHERE fact.amount > 0`), as long as the right source is keyed by its side
  of the keys;
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

**You declare the MV primary key at `CREATE TABLE` and it is your
responsibility that it identifies a logical row** (like Flink's
`PRIMARY KEY ... NOT ENFORCED`): the refresh writes delete markers whose merge
relies on that key, and a key that is not unique makes the view resolve rows
last-writer-wins.  The executor only enforces that a statement targets an MV
**with** a key (a global aggregate, whose single row is rewritten wholesale,
needs none).  A typical trap: the two branches of a
`UNION ALL` can produce the same key values, so project a branch column
(`'a' AS src`) and include it in the MV key.

## Sources

* A **keyed** source (LakeSoul primary key) supports upserts, deletes and
  join-key changes: the refresh reads the changed keys' previous state as of
  the window start and retracts it.
* Every source must be **keyed**: the refresh folds the changelog through
  merge-on-read and retracts by key, so an append-only source (with or without
  a change column) is rejected when the statement is analyzed.
* A source may declare a CDC change column (`lakesoul_cdc_change_column`,
  [`IvmTableOptions::with_cdc_column`](src/table.rs)): `delete` retracts and
  `update_before`/`update_after` pair an update. Without one the internal
  `rowKinds` column is used. The change column is never part of a `UNION`
  distinct key.
* A source may be **range partitioned** (LakeSoul `PARTITIONED BY`): the
  partition columns belong to the logical schema but not to the data files,
  and every read injects their values from the partition descriptor —
  changelog window, current state, before state and rebuild baseline alike —
  so filters, group keys and join keys can reference them. Merge-on-read runs
  within a partition, and as in LakeSoul a key must not span partitions (the
  partition is also the unit of the cursors). Dropping a partition is not
  incremental yet: rebuild the view when a partition disappears.
* The **keyed CDC contract** is exactly four markers: `insert`, `update_after`,
  `update_before`, `delete` (any other value is treated as a live version).
  A keyed source folds markers through merge-on-read, so the final state is the
  highest version per key:
  - an `update_before` + `update_after` pair in one window folds to the new
    version; a lone `update_before` retracts the row until its `update_after`
    arrives (windows may interleave);
  - if the pair arrives out of order (`update_after` first), the retraction is
    applied last and wins, exactly like a full recompute over the same table;
  - a **primary key change is a `delete` of the old key plus an `insert` of the
    new one**; an update that only changes the key's value leaves the old row in
    place;
  - several live versions for one key resolve to the latest (the writer keeps
    the input order for a key), so a source should keep its key unique.
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
ordinary table: no `"rowKinds" = 'insert'` filter is needed (the internal
`rowKinds` / `__ivm_epoch` columns are an implementation detail), and a scan
that materializes no column (`SELECT COUNT(*) FROM mv`) works too.

## Refresh semantics

* Every source partition has a cursor (last consumed version/timestamp). A
  refresh collects the changelog window per partition, applies the delta and
  advances the cursors. The before state of a window is read per partition from
  its own pinned version, so partitions whose cursors moved apart stay
  consistent.
* **Cascading views**: a statement only reads the current state of the views
  it references.  A scheduler advances a whole chain with
  `IvmRuntime::refresh_view_chain(view_id)` (upstream first, cycle-safe), and
  `IvmSqlExecutor::with_refresh_upstream(true)` opts a statement into
  refreshing the views it reads first.
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

* non-equality join conditions over columns that the join does not
  materialize (a pair carries `left_value` / `right_value` or the wide output
  columns) and join keys outside the equality support;
* outer joins inside a multi-way chain outside the lookup-chain shape: a
  right/full step, a bushy tree, a step source not keyed by its join keys
  (a 1:N right side), or more than eight sources;
* `INTERSECT`/`EXCEPT` and null-aware join predicates (`IS NOT DISTINCT FROM`)
  outside the maintained subset: they plan as *null-aware* joins, and the
  `ALL` variants also count the matches on both sides, while the maintained
  views keep one row per left row, so the shapes whose match counts can
  differ are rejected rather than silently returning different rows;
* scalar subqueries outside the maintained subset (a correlated `(SELECT ...)`
  with a `GROUP BY`, a `DISTINCT` aggregate or
  several aggregates, and a correlated value inside a computed expression)
  and computed columns above an aggregate
  (`SELECT s * 2 FROM (SELECT SUM(v) AS s ...) t`); correlated `EXISTS` /
  `IN` subqueries are supported as semi/anti joins;
* append-only sources (with or without a change column): incremental views
  need keyed sources and reject them when the statement is analyzed. The same applies to a projection/filter view: a `delete` /
  `update_before` marker is never materialized as a row, but the matching
  insert row stays (there is no key to retract it), and views that recompute
  from the current state are not maintained over such sources yet (see the
  CDC plan in `PLAN.md`).

## Tests

The crate has three test layers:

* SQL entry end-to-end scripts (`tests/slt/*.slt`) driven by `tests/sqllogic.rs`;
* differential oracles against full recomputes (`tests/sql_oracle.rs`),
  including the join shapes (differently named inner keys, non-equality pair
  conditions) and the filtered semi/anti and multi-column distinct views. The
  mutation stream is deterministic per oracle; vary it with
  `IVM_ORACLE_SEED=7 IVM_ORACLE_ROUNDS=30 cargo test -p lakesoul-ivm --test
  sql_oracle` (a custom seed whose data stays empty no longer fails the
  coverage guard);
* typed runtime tests (e.g. `tests/join_keyed.rs`, `tests/window_refresh.rs`);
* typed coverage for `Float64` / `Decimal128` / `Date32` / `Boolean` inputs
  (`tests/slt/typed_*.slt` and `union_types.slt`), including NULL handling and
  the non-NULL count.

```sh
# PostgreSQL is required; the tests use the LAKESOUL_PG_* environment.
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
cargo test -p lakesoul-ivm -- --test-threads=1
```
