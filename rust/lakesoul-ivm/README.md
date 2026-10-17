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
append-only sources are rejected (see [Sources](#sources)) — unless the
statement falls back to a full recompute (Tier-2, see
[Refresh semantics](#refresh-semantics)). Aggregates and
windows follow SQL NULL semantics; primary keys
used as row identities must be non-nullable.

| Kind | SQL shape | Materialized columns |
|---|---|---|
| Projection / filter | `SELECT [expr AS c, ...] FROM src [WHERE p]`; `p` may compare against an uncorrelated scalar subquery (`WHERE v > (SELECT AVG(v) FROM dim)`), whose tables are watched alongside the source (a keyed source only) | projected columns, the source primary keys a projection omits (a keyed source identifies its rows by them, as the top-k layout does), `rowKinds`, `__ivm_epoch` |
| SELECT DISTINCT | `SELECT DISTINCT c, ... FROM src [WHERE p]` | the distinct columns, `count_v`, kinds, epoch |
| DISTINCT ON | `SELECT DISTINCT ON (g) g, v FROM src [ORDER BY ...]` over a keyed source; the picked row is deterministic (the source primary keys break ties) | keys, one column per picked column (`first_value_v`), kinds, epoch |
| SUM / COUNT / AVG | `SELECT k, SUM(v), COUNT(*), AVG(v) FROM src [WHERE p] GROUP BY k [HAVING h]`; the same aggregates without a `GROUP BY` (global, single-row MV) | keys, `sum_v`, `count_v`, `__ivm_nonnull_count` (`avg_v` for AVG), kinds, epoch |
| MIN / MAX | `SELECT k, MIN(v) FROM src GROUP BY k`; without a `GROUP BY` a global MIN/MAX | keys, `value`, kinds, epoch (+ value-count state table) |
| GROUPING SETS / ROLLUP / CUBE | `GROUP BY GROUPING SETS ((a, b), (a), ())` over a keyed source; plain keys and several `ROLLUP`/`CUBE`/`GROUPING SETS` expressions may mix (the planner expands them to the cross-product member sets); plain or aliased computed keys; any mix of the supported aggregates (`SUM`/`COUNT`/`AVG` keep the incremental layout, other mixes are recomputed per set) | `__ivm_grouping`, every flat key (nullable), one column per aggregate (`sum_v`/`count_v`/`__ivm_nonnull_count`/`avg_v` for the `SUM`/`COUNT`/`AVG` layout), kinds, epoch |
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
| TOP-K | `SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS rn FROM src) t WHERE rn <= k`, or a global `SELECT ... FROM src ORDER BY o LIMIT k` / `LIMIT k OFFSET m` (the window is `m < rank <= m + k`; the ordering may be a scalar expression, and so may a select item — `v * 2 AS w` is materialized and `ORDER BY w` orders by it; boundary ties are broken by the source primary keys) | projected columns, the source primary keys a projection omits (the view identifies a row by them), kinds, epoch |
| Inner join | `JOIN` on equality keys (the two sides may name them differently; several payload columns per side become wide output columns), both sides keyed, optional side filters and payload conditions; `IS NOT DISTINCT FROM` keys match NULL with NULL (plain columns only) | join keys (the left names), `left_value`/`right_value` or one column per selected payload (the alias, or the source name), `__left_pk_*`, `__right_pk_*`, kinds, epoch |
| Lookup join | `LEFT JOIN` where the right side is keyed by the join keys (they may differ in name, optional filters on either input; several payload columns per side become wide output columns) | join keys, `left_value`/`right_value` or one column per selected payload, left primary keys, kinds, epoch |
| Multi-way join | inner `JOIN`s over three to thirty-two sources (all keyed), optional side filters and cross-source conditions; a source without a join key is cross joined, so `FROM a, b, c` and mixed keyless steps work | one column per selected payload (the alias, or the source name), `__pk<i>_<key>` per source row identity, kinds, epoch |
| Lookup chain | a left-deep `[LEFT] JOIN` chain over a keyed base where every step joins a source keyed by its join keys (`a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.v = b.v` — a step key may reference any earlier source), optional step filters | one column per selected payload (aliases name the step payloads), kinds, epoch |
| CROSS JOIN | `CROSS JOIN` / `FROM a, b`, both sides keyed, optional side filters and cross-side predicates; several payload columns per side become wide output columns | `left_value`/`right_value` or one column per selected payload, `__left_pk_*`, `__right_pk_*`, kinds, epoch |
| LEFT / FULL / RIGHT JOIN | outer equi-joins, both sides keyed, the keys may be named differently, optional filters on either input; several payload columns per side become wide output columns; `IS NOT DISTINCT FROM` keys match NULL with NULL (plain columns only; a lookup right side keyed by the join keys stays strict) | as the inner join (or one column per selected payload), with nullable unmatched identities |
| UNION ALL | `SELECT ... UNION ALL SELECT ...` | the projected columns, `__ivm_source`, kinds, epoch |
| UNION | `SELECT ... UNION SELECT ...` | the projected columns (the CDC column is excluded), `count_v`, kinds, epoch |
| Semi / anti join | `WHERE [NOT] EXISTS (SELECT ...)` / `x IN (SELECT ...)` / `x op ANY (SELECT ...)` / `x op ALL (SELECT ...)`, optional side filters; a correlated scalar subquery compares against one aggregate row per correlated key (a `COUNT` family aggregate keeps its empty-set semantics: a key without rows counts 0) | the projected left columns, kinds, epoch |
| Left aggregate | `SELECT ..., (SELECT AGG(w) FROM dim u WHERE u.k = s.k) AS m FROM src s` (a select-list correlated scalar subquery), a computed expression over it (`... * 2 + 1 AS m`) or one that also reads the row's own columns (`s.v + (SELECT SUM(w) ...) AS m`), and the `COUNT` family (`COUNT(*)` / `COUNT(w)`) which counts zero for a key without rows | the projected left columns, the aggregate column, the computed columns, kinds, epoch |
| INTERSECT / EXCEPT | the distinct and `ALL` variants; a repeated left of an `ALL` variant competes the per-tuple match counts (`min(count_l, count_r)` copies for `INTERSECT ALL`, the surplus for `EXCEPT ALL`); a NULL-capable join column matches `NULL` with `NULL` | the projected left columns (plus the left key), kinds, epoch |

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
* a **global `ORDER BY ... LIMIT k`** is a top-k without a partition: the
  rows within the limit stay materialized, and the boundary ties are broken
  by the source primary keys (a raw `LIMIT`'s own tie-break is arbitrary), so
  the maintained rows are the
  `ROW_NUMBER() OVER (ORDER BY <ordering>, <pks>) <= k` ones;
* **window frames** are evaluated by the engine on the recomputed partition:
  `ROWS`, `RANGE` (numeric or temporal offsets) and `GROUPS` frames all work
  (`RANGE BETWEEN 2 PRECEDING AND CURRENT ROW`, `GROUPS BETWEEN 1 PRECEDING
  AND CURRENT ROW`);
* **aggregate `FILTER (WHERE ...)`**, `HAVING` (including an aggregate form
  the select list does not carry, which switches the view to the
  multi-aggregate layout and materializes every aggregate form as its own
  column, and a `HAVING` over an unmergeable aggregate the select list
  carries, which is materialized as a derived step), and per-branch `WHERE`
  in set operations;
* **a `HAVING` subquery** over another source is joined into the recomputed
  groups before the predicate runs: `GROUP BY g HAVING SUM(v) >
  (SELECT MAX(d.v) FROM dim d WHERE d.g = src.g)` keeps the per-key lookup
  (a keyed left join) and `HAVING SUM(v) > (SELECT MAX(v) FROM dim)` a global
  one (a cross join).  A lookup change re-evaluates every group (global) or
  the groups whose keys changed (per-key); the two-view pattern
  (`INSERT INTO groups ...; INSERT INTO filtered SELECT ... WHERE sum_v >
  (SELECT ...)`) remains available and is refreshed in dependency order;
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
  keeps SQL NULL semantics; a **COUNT family** aggregate (`COUNT(x)`,
  `COUNT(*)`, `COUNT(DISTINCT x)`) keeps the empty-set semantics: a key
  without rows counts 0, so its rows may still compare above zero (DataFusion
  guards the decorrelated comparison, and the maintained predicate evaluates
  the guard over the aggregate column, which the runtime pads with NULL for
  such a key); a `COUNT(DISTINCT x)` subquery resolves the two-level rewrite
  back to `count(distinct x)`, and several aggregates stay rejected; a
  **`DISTINCT`** aggregate other than the `COUNT` family is rejected with a
  message naming it (the optimizer splits it into a dedup group-by whose
  internals a correlated view does not maintain), for both the `WHERE` and
  the select-list form; the
  subquery must be one plain aggregate over a
  single table, and the left rows must be unique per correlated key; a
  **select-list** correlated scalar subquery
  (`SELECT ..., (SELECT AVG(w) FROM dim u WHERE u.k = s.k) AS m FROM src s`)
  is maintained as a left join against the same per-key aggregate, with NULL
  for a key without rows; a computed select-list expression over the value
  (`(SELECT MAX(w) FROM dim u WHERE u.k = s.k) * 2 + 1 AS m`) appends a view
  column computed from the per-key aggregate (the raw value stays
  materialized, named `__ivm_agg` when the select list does not carry it);
* **quantified comparisons** (`x op ANY (S)`, `x op ALL (S)` and their
  `NOT (...)` complements): rewritten before planning into an `EXISTS`
  (`x op y`) or `NOT EXISTS` (`(x op y) IS NOT TRUE`) predicate, so they are
  maintained as a theta semi/anti join whose comparison is the only
  condition.  A NULL-capable comparison keeps the quantifier's three-valued
  filter semantics (`> ALL` over an empty set is true, a NULL comparison
  makes the result unknown and the row drops); a `NOT` directly above a
  quantifier is pushed into it with the complementary operator
  (`NOT (x > ANY S)` is `x <= ALL S`);
* **`INTERSECT` / `EXCEPT`**: the distinct variants need left rows unique per
  join tuple (their primary key is covered), which also covers null-aware
  `IS NOT DISTINCT FROM` semi/anti predicates with a repeated left (every row
  is checked on its own, plain membership).  The `ALL` variants keep the
  standard per-tuple match counts: a repeated left row is ranked by its
  primary key and stays while `rank <= count_r` (`INTERSECT ALL`) or
  `rank > count_r` (`EXCEPT ALL`), so a left insertion or deletion
  re-evaluates its whole tuple.  A NULL-capable join column makes the view
  null-safe, so `NULL` matches `NULL` exactly as the set operations require
  (DataFusion evaluates a raw `INTERSECT ALL`/`EXCEPT ALL` as a plain
  membership join, without the counts);
* an **inner or outer join** may use null-safe keys (`a JOIN b ON a.v IS NOT
  DISTINCT FROM b.v`, plain columns only): NULL then matches NULL on both
  sides, and the join key columns of the output are nullable.  A `LEFT JOIN`
  right side keyed by the join keys stays a strict lookup (its keys are a
  non-nullable primary key, so a NULL left key can never match).  The
  remaining key shapes (multi-table chains, computed keys) stay rejected
  rather than silently dropping the NULL matches;
* **derived views**: an operator whose source is a compound subplan is
  maintained by materializing the subplan as an internal derived view (named
  `<target>__ivm_step0`, marked `lakesoul.ivm.internal`, registered as a view
  under its table id).  The materialized steps are an inner **join** (for an
  aggregate / DISTINCT / window / TOP-K / UNION reading it), a
  **sum/count/avg aggregate**, a **`MIN`/`MAX` or `DISTINCT` aggregate**
  (the value-count layouts; their step state tables are created next to the
  step MV), a **mixed aggregate** (any aggregate mix the multi-aggregate view
  maintains, materialized one column per aggregate), a **roster window** (a
  single ranking or value window, keyed like the window MV by its partition
  keys plus the source primary keys), a **window chain** (several window
  clauses, whose columns are mapped per chain node), an **inner-join side**
  (the right or left side of a join is the subquery form of another join: the
  side is materialized with its plain-column projection, so a bushy tree works
  for every join type — `a LEFT JOIN (b JOIN c ON …) x ON x.k = a.k`,
  `a LEFT JOIN (b LEFT JOIN c ON …) x ON x.k = a.k` and the **full**-join form
  — and a payload the select list renamed keeps that name in the step MV),
  a **right/full chain step** (`a LEFT JOIN b ON … RIGHT JOIN c ON …`
  materializes the inner join as the step; `a RIGHT JOIN b ON … LEFT JOIN c
  ON …` materializes the right-join step itself, so a chain with a right or
  full step stays incremental), and a **`UNION ALL`** (a keyed branch the consuming operator does not select
  its primary keys from is widened with them again).  An outer-join side
  materializes a **non-null row identity** (`__ivm_pair_id`: both sides'
  primary keys encoded as a collision-free string, the same length-prefixed
  form the lookup chain uses for its 1:N steps), so the consumer can read the
  step as a keyed source; a left-join step is keyed by the aliased left
  identities plus that column, a full-join step by the column alone.  An aggregate argument that names a projection
  above the step (for example `SUM(rn)` over a window step) resolves to the
  step's materialized column, so the rendered calls run against the step MV.
  The statement is rewritten to read the derived MV (a compact join widens
  its payloads to the subplan's column names, and an aggregate step's
  materialized columns such as `sum_v` or `value`, or a window step's
  function columns, are mapped back to the subplan output they replace by
  renaming the plan's column references), the step refreshes before the
  statement through the same orchestration (so the chain includes it and
  `refresh_view_chain` advances both), and a changed definition reuses the
  step table or drops it when the definition shrinks.  A subquery the
  statement only orders (`ORDER BY` without a limit) is read through, and an
  `ORDER BY … LIMIT` subquery materializes as a top-k step when the statement
  consumes the source primary keys (a top-k row is identified by them).  The remaining step
  kinds (a mixed aggregate, a multi-window chain, and a union whose branches
  need different key columns) and a derived view that would not expose a
  subplan column stay rejected with a message pointing at explicit views;
* an **inner join**, a **cross join** and a pair-keyed **`LEFT JOIN`** may
  filter their sides (`WHERE fact.amount > 0 AND dim.active`): the analyzer
  keeps the predicates pushed below the join and a row entering or leaving
  its filter adds, retracts or NULL-pads its pairs;
* a **single-sided inner join** whose right side is keyed by the join keys is
  maintained as the semi shape: the match is unique, so the view is exactly
  the left rows that have one (changes on either side add and remove them);
  a left primary key that is also the join key is materialized once, as that
  key column;
* **`DISTINCT ON`**: one row per grouping key, the first row of the ordering
  (the source primary keys are appended as a deterministic tie-break; a
  key-only `SELECT DISTINCT ON (key) key` is rejected with a hint to write
  `SELECT DISTINCT` instead); the affected groups are recomputed from their
  current rows;
* **lookup chains**: a left-deep `[LEFT] JOIN` chain over a keyed base (the
  star-schema shape): a step key may reference any earlier source's column
  or be a scalar expression over one of them, so `c` can be looked up by a
  value `b` produced or by `b.v + 1` (the expression is materialized on the
  frame of the source it reads). A step whose keys are the
  source key is a **1:1 lookup**; any other key is a **1:N lookup**, and the
  MV materializes one extra column per 1:N step (`__step<source>_id`, the
  matched row's primary key encoded as a non-null string, `n...n` for an
  unmatched LEFT step), so the MV primary key must contain the base key plus
  those identity columns. A refresh re-evaluates the base rows the changed
  keys touch (the base delta plus, per changed step, the base rows matching
  its old and new keys through the prefix chain; when an earlier source
  changed in the same window every base row is re-evaluated) by replaying
  the chain, so a 1:N step emits one row per match, a missing step row NULLs
  its payload and a removed base row drops its rows;
* **mixed aggregate kinds**: a statement may combine the supported aggregate
  functions (`SUM(v), MIN(v), MAX(v), COUNT(*)`, `AVG`, the variance family,
  `MEDIAN`, `APPROX_DISTINCT`, `STRING_AGG`, `ARRAY_AGG`, the bit / regression
  functions, ...) in one view; the affected groups are recomputed from their
  current rows, and a SUM/COUNT/AVG-only mix keeps the incremental sum/count
  view.  A **computed select expression over the aggregates** (`SELECT g,
  SUM(v) * 2 AS s2 ... GROUP BY g`) routes to the same general view: the raw
  aggregate columns stay materialized (HAVING and the expression reference
  them) and each computed expression appends one view column.  A computed
  output needs an alias; the DISTINCT split, a subquery or window function
  output and a computed column over a plain `SELECT DISTINCT` or a
  `GROUPING SETS` statement are rejected;
* **multi-way inner joins** (three to thirty-two sources): the join tree flattens
  into one chained inner join (a join key may be a scalar expression over
  exactly one of the flattened sources, materialized under a hidden column
  before the join); a source without a key pair is cross joined
  (the chain's `FROM a, b, c` and mixed keyless steps) and the MV is keyed by
  every source's row identity (`__pk0_*`, `__pk1_*`, ...); a refresh rewrites
  the tuples of the rows that changed on any source; side filters and
  non-equality conditions between any two sources are maintained;
* **wide join outputs**: an inner join, a cross join, a lookup `LEFT JOIN`
  or an outer join may select more than one column per side; each becomes an
  MV column under its select alias (or source name), at least one column per
  side is required, and for the inner and cross joins a non-equality
  condition may compare any column of either side (`ON l.k = r.k AND
  l.amount < r.limit`, or a cross join's `WHERE l.lo <= r.hi`); a compared
  column the select list does not carry is materialized as a hidden payload
  column named `__ivm_cond_<side>_<column>`; the compact single-payload
  `left_value` / `right_value` shape stays for one payload per side, and the
  outer / lookup wide outputs pad the unmatched side with NULLs (a lookup
  join materializes the left primary keys itself, so selecting one does not
  widen the payloads);
* **non-equality join conditions** (`ON l.k = r.k AND
  l.amount < r.limit`, or a cross join's `WHERE l.lo <= r.hi`): the condition
  is evaluated on each joined pair, so a change adds or retracts the
  affected pairs, and the compared columns need not be materialized in the
  select list;
* **computed join keys** (`ON l.x + 1 = r.y`, an equality whose sides are not
  both plain columns): the two-source inner join renders each side, evaluates
  it on that source before the join, and stores the value under a hidden key
  column (`__ivm_key_<n>`), so the MV stays keyed by its pair identities.  A
  **lookup `LEFT JOIN`** maintains a computed key on the fact side as long
  as the expression resolves to a plain column of the dimension, which
  stays keyed by it (`ON r.k = l.fk + 1`): the fact side is probed by the
  computed value, the dimension is read by its key column, and the hidden key
  column holds the computed value (compact and wide lookups alike).  The
  pair-keyed `LEFT` / `FULL` joins render each side the same way, and their
  affected-row probe materializes the computed keys before it joins by key
  name (the MV stays keyed by both row identities);
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
last-writer-wins.  A refresh batch orders its deletes before the inserts of
the same key (it is sorted by the MV's primary keys), so a key that is
coarser than the group keys still resolves to the replacement row.  The executor only enforces that a statement targets an MV
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
* Columns may be **nested** (`Struct`, `List`, `Map`): they are opaque
  payloads the refresh copies through, so row views, aggregates, joins, unions
  and windows all carry them (including NULL, empty and nested-NULL values),
  single fields are readable in expressions (`s['a']`, `l[1]`), and a nested
  column may be a **group key** or a **join key** (structs and lists match by
  value; NULL never matches).  `array_agg` over a struct column materializes
  a list of structs.  A nested **join key** and an outer join or chain step
  whose key cannot be cast to a string stay rejected.
* A source may declare a CDC change column (`lakesoul_cdc_change_column`,
  [`IvmTableOptions::with_cdc_column`](src/table.rs)): `delete` retracts and
  `update` asserts the new version of the key. Without one the internal
  `rowKinds` column is used. The change column is never part of a `UNION`
  distinct key.
* A source may be **range partitioned** (LakeSoul `PARTITIONED BY`): the
  partition columns belong to the logical schema but not to the data files,
  and every read injects their values from the partition descriptor —
  changelog window, current state, before state and rebuild baseline alike —
  so filters, group keys and join keys can reference them. Merge-on-read runs
  within a partition, and as in LakeSoul a key must not span partitions (the
  partition is also the unit of the cursors).  A **dropped partition**
  (a delete commit that clears its snapshot) cannot be retracted file by file.
  When the view's group keys carry the partition columns — the dropped
  partition's output rows are then exactly the MV rows matching its values —
  the refresh retracts those rows under its own epoch and advances the
  partition's cursor to the deletion version: the statement reports an
  incremental cleanup, the dropped rows disappear and the rest of the window
  still applies.  Every other view rebuilds from the current source state
  instead (the statement reports a rebuild).  A **statement chain** treats
  either case like any other refresh action.
  An MV may be range partitioned the same way
  (`IvmTableOptions::with_range_partition_columns`): its primary keys must
  contain every partition column (the statement is rejected otherwise),
  the refresh writes each output partition into its own directory, and a
  `SUM`/`COUNT` refresh reads only the partitions the window touched when the
  group keys carry the partition columns and the source's primary keys carry
  all of its partition columns (a key then cannot move between partitions).
* The **keyed CDC contract** is exactly three markers — the values the
  LakeSoul Flink CDC ingestion writes: `insert`, `update`, `delete` (any other
  value is treated as a live version).  A keyed source folds markers through
  merge-on-read, so the final state is the highest version per key:
  - `insert` and `update` assert the row; `update` carries the after image of a
    change, so a same-key update supersedes the old version without needing a
    before image;
  - `delete` retracts the key; a later `insert`/`update` is a newer version and
    resurrects it;
  - an update that changes the key must be written as a `delete` of the old key
    plus an `update`/`insert` of the new one (the ingestion represents the
    before image as a `delete` row); a plain `update` carrying a new key
    asserts that key and leaves the old row in place;
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
  `IvmRuntime::refresh_view_chain(view_id)` (`refresh_view_chains` covers
  several roots in one traversal): the dependency graph is discovered from
  the registered specs, the views refresh upstream first and each view once,
  and a cycle is rejected with its path.  The default `ChainPolicy` retries a
  failing view once, rebuilds it from the full source state when the
  incremental refresh keeps failing (or when the view is marked `dirty`),
  and aborts the chain -- leaving the view `dirty` with `last_error` set --
  when even the rebuild fails, so downstream views never read an upstream
  that stayed behind.  `IvmSqlExecutor::with_refresh_upstream(true)` opts a
  statement into bringing the views it reads up to date first (same policy,
  one traversal), and a `dirty` target view is rebuilt by the statement
  itself.  `IvmMetadata::list_views` lists the views with their status,
  interval and last error.
* The window identity (source, partition, from/to versions) is recorded in
  `ivm.epochs`. Re-applying the same window is a no-op; a partially written
  window is completed by the retry because the affected keys are rewritten.
* A view whose source history is no longer consumable (for example a
  compaction that removed the cursor versions, or an **update commit** whose
  window needs a snapshot) must be rebuilt; the metadata signals this through
  `requires_rebuild`, and a statement that hits it — or a cursor window that
  no longer aligns with the committed history — **rebuilds the view
  automatically** and reports `Rebuild`, so the view stays correct without a
  manual rebuild; a statement chain treats it like any other rebuild and skips
  the retry.  The same applies to a dropped source partition.
* One writer per view: refreshes of the same view serialize through the epoch
  protocol.
* **Two-tier refresh contract**: a statement whose shape the incremental
  analyzer does not maintain defines a **fallback** view instead of being
  rejected — the whole statement is recomputed on every refresh (the result
  is exactly the full recompute over the current source state) and the
  execution reports `IvmExecutionAction::Fallback` (`action = "fallback"` in
  the metrics; `list_views` marks the view and carries the rejection reason).
  A fallback refresh first collects the change windows of every source: when
  none changed it is a no-op, otherwise the statement runs against the current
  state and the result is written in one epoch.  When only range-partitioned
  sources changed and the statement is **partition local** (the executor
  records that decision with the view; several partitioned sources are fine
  when they are partitioned by the same columns and every join between them
  equates those columns), only the changed partitions are recomputed: their
  rows are retracted and rewritten, and every other partition keeps its rows
  and versions.  Any other
  case — a change in an unpartitioned source, a global operator such as
  `LIMIT`, a view without a primary key — recomputes the whole view, which
  truncates the MV before writing.  The
  result is computed before the MV is touched, so a failing statement leaves
  the previous state in place.  A fallback MV may be **append-only**; when it
  declares primary keys the result must be unique on them (a repeated key
  would be collapsed by merge-on-read) and a violation is reported
  explicitly.  The differential oracle covers a fallback view across random
  insert/update/delete rounds, so its rows are checked against the reference
  recompute every round, and the sqllogic harness accepts
  `/*ivm-expect-action fallback*/` (`tests/slt/fallback.slt`).  Create the target with `with_allow_fallback(false)` to keep
  the strict behavior (unsupported shapes are then rejected, as before), and
  note that a Tier-1 view must not read a fallback view: the fallback rewrites
  its MV, so a downstream incremental refresh cannot see what it removed.

## Observability

The runtime and the SQL entry emit [`metrics`](https://docs.rs/metrics)
counters and histograms, so any recorder (for example the Prometheus exporter
used by the IO layer) can export them:
A fallback refresh is visible as `action="fallback"` on the statement
counter and `kind="fallback"` on the refresh/rebuild counters, and the view
metadata records the fallback mode and its reason.

| Metric | Labels | Meaning |
|---|---|---|
| `lakesoul_ivm_statements_total` | `action` (`bootstrap` / `incremental` / `rebuild` / `overwrite` / `error`) | maintenance statements |
| `lakesoul_ivm_statement_duration_seconds` | `action` | statement duration |
| `lakesoul_ivm_refreshes_total` | `kind`, `result` (`applied` / `noop`) | view refreshes |
| `lakesoul_ivm_refresh_duration_seconds` | `kind` | refresh duration |
| `lakesoul_ivm_rebuilds_total` | `kind` | full rebuilds |
| `lakesoul_ivm_rebuild_duration_seconds` | `kind` | rebuild duration |
| `lakesoul_ivm_epochs_total` | `kind` | committed epochs |
| `lakesoul_ivm_fallbacks_total` | — | Tier-2 fallback recomputes |
| `lakesoul_ivm_fallback_rows` | — | rows written by fallback recomputes (a cost proxy) |

The `kind` label is the view kind (`sum_count`, `min_max`, `window`, `join`,
`union_distinct`, ...); view and table ids are never used as labels.  A
fallback view's kind is `fallback`, so its refreshes and rebuilds are also
visible per view kind.

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

The following shapes are **not maintained incrementally**.  By default a
statement of such a shape still defines the view: the executor recomputes it
on every refresh (Tier-2, see
[Refresh semantics](#refresh-semantics)) — in full, or only the changed
partitions when the statement is partition local — and the analyzer's
rejection becomes the fallback reason (which records the partition-pruning
decision too).  Create the target with `with_allow_fallback(false)`
(the `lakesoul.ivm.allow_fallback = false` table property) to have the shapes
rejected outright; the notes below describe the incremental analyzer's
boundary (see [PLAN.md](PLAN.md) §10.5 for the backlog):

* a non-equality join condition is maintained for the two-source inner and
  cross joins (a compared column the select list does not carry becomes a
  hidden payload column); an outer join or a chain step with a residual
  condition stays rejected;
* `COUNT(DISTINCT ...)` as a select-list correlated scalar subquery:
  DataFusion plans it as two aggregate levels, so the maintained subquery
  would correlate on the distinct key too;
* a single-sided inner join over a side without a primary key (the pair
  shape its duplicates need cannot key it): a right side keyed by the join
  key stays the semi (membership) shape, and otherwise the pair join
  materializes the missing side's row identity as its payload (a right side
  with a non-unique join key, or a select list that takes only the right
  side); a single-sided **outer** join stays rejected;
* a **list or map primary key** on a side of an outer-join or chain **step**:
  the step's row identity encodes both sides' primary keys as length-prefixed
  text - a struct key is encoded field by field **before the join**, so the
  write path maintains it (a map or list key is rejected with a clear reason;
  join keys themselves may be nested, they compare and materialize by value);
* `INTERSECT`/`EXCEPT` and null-aware join predicates (`IS NOT DISTINCT FROM`)
  outside the maintained subset: a `DISTINCT` set-op variant whose left rows
  repeat a join tuple is rejected rather than silently returning duplicate
  rows (the `ALL` variants count the matches on both sides through the count
  mode, and a null-aware predicate on a repeated left keeps plain membership
  semantics); a null-safe key in a multi-table chain or on a computed
  expression is rejected (a single two-source join over plain columns is
  maintained, inner or outer; a **uniform** null-safe chain keeps correct
  plan markers and is a recorded follow-up, while a mixed chain does not -
  DataFusion overwrites the inner join's marker with the outer's, see PLAN
  §10.165);
* quantified comparisons in a position that is not a plain predicate: a
  quantifier below a wider `NOT` or inside a disjunction keeps DataFusion's
  null-aware mark-join plan and is rejected rather than mistranslated (a
  `NOT` directly above a quantifier is rewritten with the complementary
  operator, and `= ANY` / `<> ALL` keep the exact `IN` / `NOT IN` forms); a
  multi-column `(a, b) IN (...)` is rejected by the planner itself;
* a top-k outside the maintained subset: `LIMIT` without `ORDER BY` (the
  kept rows are arbitrary); a window result (single- or multi-clause), an
  inner join and an aggregate source are materialized as a step
  automatically;
* a decimal `SUM`/`AVG` whose precision leaves no headroom for the
  accumulator: DataFusion saturates the result type at 38 digits
  (`SUM(DECIMAL(p, s))` is `DECIMAL(min(38, p + 10), s)`) and wraps the
  accumulator silently on overflow, so a source precision above 28 (`AVG`
  above 34) is rejected instead of maintaining a wrong value;
* scalar subqueries outside the maintained subset (a correlated `(SELECT ...)`
  with a `GROUP BY` over columns other than its correlated keys (DataFusion's
  own analyzer rejects those), several aggregates, a select-list `COUNT`
  family (the guarded rewrite is only maintained in a `WHERE` comparison),
  a computed value over left columns beside one, a
  `HAVING` subquery with several aggregates / nested subqueries / correlated
  on a non-group key, and a computed column over the lookup value),
  a subquery or window function **above** an aggregate, and the derived-table
  form of a computed aggregate output
  (`SELECT s * 2 FROM (SELECT SUM(v) AS s ...) t`, which needs two chained
  views; the direct `SELECT SUM(v) * 2 ... GROUP BY g` form is maintained);
  correlated `EXISTS` / `IN` subqueries are supported as semi/anti joins;
* append-only sources (with or without a change column): incremental views
  need keyed sources and reject them when the statement is analyzed. The same applies to a projection/filter view: a `delete`
  marker is never materialized as a row, but the matching insert row stays
  (there is no key to retract it), and views that recompute from the current
  state are not maintained over such sources yet (see the CDC plan in
  `PLAN.md`).

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
