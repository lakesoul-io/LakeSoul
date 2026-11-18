# Pinned PostgreSQL regression sources

This directory contains unmodified files from PostgreSQL **REL_18_6**, commit
`724edf9bde9d356724ad384a2e196edc3c9f80f7`, from
<https://github.com/postgres/postgres>. `SOURCE.toml` records each original path,
SHA-256, selected source ranges and the destinations of the derived SLT records.
`COPYRIGHT` contains the upstream license and notices. The full source and
expected-output files are retained for review; only the selections below run.
Tests use the checked-in SLT files and never fetch PostgreSQL at runtime.

| Source | Selection | Normal gate | Strict known defects |
| --- | --- | --- | --- |
| `subselect.sql` | Parentheses, scalar/array subqueries, IN/NOT IN, nested and correlated IN | 15 queries + 9 setup statements | 4 queries + a copy of the 9 setup statements |
| `join.sql` | Table/column aliases, cross/inner/outer/natural joins, USING/ON, three-table and derived-table joins | 33 queries + 32 setup statements | 8 queries + a copy of the 32 setup statements |
| `aggregates.sql` | Ordered and DISTINCT array aggregates, aggregate FILTER and outer references | 9 queries | 2 queries |
| `window.sql` | ROWS boundaries and all-NULL frames, RANGE offsets, lag/lead, sliding boolean aggregates | 3 queries | 8 queries |

Both selections were made before running LakeSoul. Of their **82 queries and
41 setup statements**, 60 queries enter `tests/slt/pg/upstream/`; the remaining
22 keep PostgreSQL's correct answers in `tests/known_defects/upstream/`. No
selected query is discarded or rewritten to avoid a failure. Fixture statements
are copied into the defect files so they can run independently.

These are two batches, not ports of entire upstream regression tests.
Other source sections include shared regression fixtures (such as `tenk1`),
EXPLAIN plans, psql commands, temporary tables/views, composite values and other
features outside this selection. They are unported, not reported as passing or
as confirmed defects.

## Conversion rules

- Every selected SQL statement is copied unchanged, including its semicolon.
  Each SLT record names its source and expected-output line numbers, relative
  to this directory.
- Query answers come from the corresponding aligned table in upstream `.out`,
  not LakeSoul. Remove only psql's header, separators, padding and row count.
  Blank cells in these selections are SQL NULL (none is an empty text value).
  Retain column position, duplicate rows and NULLs. Check the runner's cell
  encoding when importing new text values; psql output in general needs more
  handling than these selected tables.
- Use I/R/T categories as required by the runner. Integer arrays are rendered
  with their element order and NULL elements intact; `rowsort` cannot fix an
  unspecified order inside an array. The unordered `array_agg(DISTINCT a)` at
  `aggregates.sql:721–722` was not selected. Categories do not certify exact
  PostgreSQL OIDs or all implicit type inference.
- All imported query records use `rowsort`, even when the original SQL has
  ORDER BY. They assert result multisets, including multiplicity, rather than
  incidental ordering or unspecified ties. Existing handwritten SLTs retain
  dedicated order-sensitive checks.
- DDL expects success and each original single-row INSERT expects count 1.
  The shared harness removes the selected tables at teardown. Source DROP
  statements outside the selected ranges are not added to the imported SQL.
- The same normal SLT SQL/answers run against reference PostgreSQL and LakeSoul
  in simple/extended protocols and standalone/distributed modes. Some plans
  execute on the coordinator; the distributed runner separately proves real
  two-worker execution with its mandatory remote-write script.

## Confirmed incompatibilities

These retain successful PostgreSQL results, not LakeSoul's current errors or
wrong answers:

| Source lines | Difference |
| --- | --- |
| `subselect.sql:45`, `70-73` | Simple-protocol FLOAT text uses `1.0` where PostgreSQL prints `1`; binary decoded values agree. This is an exact text-format difference, not a wrong numeric value. |
| `subselect.sql:21-22` | Parenthesized UNION as a scalar subquery is rejected by the parser. |
| `join.sql:125-127` | SELECT * with USING on a non-leading column returns the wrong column order. |
| `join.sql:210-211` | An unqualified merged USING column in WHERE is incorrectly ambiguous. |
| `join.sql:243`, `262-266`, `280-283`, `292-297`, `299-306`, `308-315` | FULL JOIN USING / NATURAL FULL JOIN fails to coalesce the unmatched right key; chained joins can also split rows that should match. |
| `aggregates.sql:970–972`, `973–975` | A scalar subquery with an outer reference in the aggregate FILTER reaches physical planning as an unsupported scalar expression. |
| `window.sql:157–158`, `572–588` | `FROM generate_series(...) x` exposes `x.value` rather than accepting bare `x` as the scalar result; planning fails before the window behavior can be evaluated. |
| `window.sql:2041–2043` | A bounded sliding frame with boolean aggregates is rejected: `bool_and` has no `retract_batch` implementation. |

The named ignored Rust test validates **all 22** answers on reference PostgreSQL
and reports LakeSoul query failures for each of the four protocol/execution
pairs, with normal resource cleanup and worker proof. There are 22 failed
queries in each simple run and 20 in each extended run (the two FLOAT cases
pass extended):

```sh
cargo -q test --profile test-fast -p postgres-lakesoul --test sqllogic upstream_postgresql_known_defects -- --exact --ignored --nocapture
```

It must currently fail. After fixing a query, move that unchanged SQL/answer
back to the normal gate and update `SOURCE.toml`; remove the ignored test when
no upstream defects remain. Ignored reproductions do not count as passing
compatibility coverage.

## Updating the pin

Make version updates in a separate reviewed change:

1. Select an explicit full PostgreSQL commit. Replace all original SQL and
   expected-output files together, retain the upstream `COPYRIGHT`, and update
   the tag/commit and hashes in `SOURCE.toml`.
2. Review the upstream SQL and `.out` diffs. Update selected ranges and per-record
   provenance. Derive answers from the new upstream outputs; keep intentional
   LakeSoul deviations separate and named.
3. Validate every imported answer on reference PostgreSQL, then run both merge
   implementations through the four normal protocol/execution runners. Run
   strict known defects explicitly and record their current failures.

The tag is for humans; the full commit and hashes identify the actual source.
Upgrading a local PostgreSQL installation does not change this fixture pin.
