# UPDATE and general UPSERT acceptance evidence

The behavior contract and the support/refusal matrix live in
[README.md](README.md) under "Phase nine: UPDATE" and "General UPSERT". This
file keeps what a one-line contract row cannot carry: the PostgreSQL oracle
differential, the numbered acceptance matrix, the gate results, and the
boundaries that were deliberately not implemented.

Status: both features are implemented and accepted.

| Feature | Implementation | Entry point |
|---|---|---|
| `UPDATE` on non-CDC primary-key tables | `3473b68ef` | `src/update.rs`, `planner::update` |
| General `ON CONFLICT ... DO UPDATE` | `07fc8f102`; source semantics and prepared target identity fixed in `7cafc2c1b` | `src/insert.rs`, `planner::upsert`, `datasource::upsert_exec` |

Regressions: `tests/update_contract.rs`, `tests/upsert_contract.rs` (coordinator,
two real Flight workers with fallback disabled, vector/text indexes), and the
native `tests::update_concurrency_tests` and `tests::sql_upsert_tests`.

## 1. PostgreSQL oracle differential

A private temporary cluster (`initdb` under `/tmp`, port 55432, loopback only;
no shared instance or database was touched), `psql -X` with
`VERBOSITY=verbose`, PostgreSQL 14.23. Each case used its own schema; setup and
the statement under test ran separately, the statement alone in autocommit.
After every error a **new session** read the relation, proving the statement
was not partially applied. The values below are measurements, not expectations.

| Cases | Scenario | Measured result | Consequence for the implementation |
|---|---|---|---|
| S01–S04b, S21 | Partial `INSERT` column list plus partial `SET`; values of unassigned columns and of `EXCLUDED` | `INSERT 0 1` / `INSERT 0 2`; unassigned columns keep the old value (S04a); with a full-column `INSERT` the unassigned `label` still keeps the old `keep` (S21); an unlisted `n` is NULL inside `EXCLUDED` (S04b); new keys are padded per `INSERT` rules (S02) | The conflict branch is old row plus `SET`; `EXCLUDED` is the complete would-be inserted row after the existing `INSERT` padding, not only the explicitly listed columns |
| S05 | Explicit `SET label = NULL` | `INSERT 0 1`, overriding the old non-NULL value | "Unassigned" and "explicit NULL" must be distinguished; NULL cannot signal an unassigned column |
| S06a, S06b | Qualified `SET` target `t.n`, `s06.t.n` | `42703` both times, relation unchanged | A `SET` target accepts only a bare column name; report `42703` like PostgreSQL |
| S07, S08 | Alias `AS x` with `x.n`; quoted and mixed-case columns | `INSERT 0 1`; `n=15`, `Weird Col='old/new'` | Aliases and quoted identifiers must be supported |
| S18a, S18c | Original name while an alias exists; referencing the source alias | `42P01` both times | Binding follows PostgreSQL: the alias replaces the original name, and the source relation is not visible in the conflict action |
| S18b, S18b2, S09b, S19 | Bare column names in `SET` or `WHERE` (VALUES and SELECT sources) | `42702` four times | Bare names must be reported ambiguous, never silently resolved |
| S18d, R04, R08 | Table actually named `excluded`, with and without a different alias | With `AS x` / `AS t` the old row and `EXCLUDED` are distinguishable; without an alias `EXCLUDED.n` gives `42P09`, relation unchanged | Same-name relations must be disambiguated correctly; "EXCLUDED always wins" is not a rule |
| S09 | Multi-column simultaneous evaluation (qualified swap) | `INSERT 0 1`; `a=20`, `b=10` | Read the old row once and project all assignments together |
| S10a, S10b | Same-value assignment in two spellings | `INSERT 0 1` both times | Count actually executed updates, not value changes |
| S12a–S12d | Conflict `WHERE` true / false / NULL / comparing old row with incoming | 1 / `INSERT 0 0` / `INSERT 0 0` / 1 | false and NULL skip the row without counting it |
| S14a, S14c | Constant false, and a runtime-false predicate with `SET 100/(t.n-10)` | `INSERT 0 0` both times, no `22012` | `SET` is evaluated lazily per row; constant folding or eager projection is not allowed |
| S14b, S14d | Constant true and runtime-true predicate | `22012` both times, relation unchanged | Runtime errors under a true condition fail the whole statement |
| S13 | Conflict `WHERE` false with mixed old and new keys | `INSERT 0 1`; the new key is inserted, the old key is unchanged | The conflict `WHERE` does not filter the insert branch |
| S11, D20a | VALUES and SELECT sources | Same semantics; `ORDER BY` can decide the duplicate-key winner | Both sources share semantics and preserve source order |
| S15, D16 | `DO UPDATE` without a conflict target | `42601` both times | The general path rejects it; the previous `conflict_plan` accepting `None` was a source-level gap |
| S16a–S16c | Conflict target without a matching unique constraint; non-unique column; `DO NOTHING` | `42P10` twice; `DO NOTHING` gives `INSERT 0 1` | No primary key keeps `42P10`; a non-primary-key target keeps `0A000` as a recorded difference |
| P01 | `ON CONFLICT ON CONSTRAINT t_pkey` | `INSERT 0 1` | Keep the existing `0A000`; no named-unique-constraint support |
| S17a, S17b2, S17b3 | `SET` on the conflict key: same value, free key, occupied key | `INSERT 0 1`; key 1→2; `23505` with whole-statement rollback | PostgreSQL allows key changes; LakeSoul keeps `0A000` (deliberate difference) |
| S20 | Mixed update, insert and `WHERE` skip | `INSERT 0 2` | N = actual inserts + actual updates |
| D01, D11, D21 | Target exists; a later input hits a row the command already modified | `21000` three times, relation unchanged | The `21000` condition is "hits a row this command already modified", not "duplicate key in the source" |
| D02, D18 | Update first, then a later input whose runtime `WHERE` is false | Still `21000` both times | The `21000` check precedes the conflict `WHERE`; `WHERE` cannot exempt an already-modified row |
| D03, D05, D14, D15, D20b | Skipped by `WHERE` first, later same-key input updates | `INSERT 0 1` | An unmodified row can still be updated by later input |
| D04, D13R | Both inputs skipped by `WHERE` | `INSERT 0 0` | No output and no error |
| D06, D07, D19 | Target absent; this statement inserts the key first; a later input is skipped or has a different predicate | `21000` three times (identical for `WHERE` true, constant false and runtime false) | Rows inserted by this statement also count as modified; `WHERE` cannot exempt them |
| D08 | Duplicate source key with `DO NOTHING` | `INSERT 0 1` | Skip mode raises no `21000`, matching the previous behavior |
| D20a vs D20b | Pre-existing `(id,n)=(1,10)`, source `(1,5),(1,6)`, `WHERE EXCLUDED.n=5`, only `ORDER BY n ASC/DESC` differs | ASC updates then hits again → `21000`; DESC skips first then updates → `INSERT 0 1`, `n=5` | Input order and per-row state must be preserved; no last-wins and no global dedup |
| E01 | Empty source | `INSERT 0 0`, zero rows | No file and no publication (U20) |
| N01, N10, R09 | Incoming type error plus a skipped branch / `DO NOTHING`; runtime `CAST` of a real source column with `WHERE` false | `22P02` three times, relation unchanged; the first two are invalid literals, R09 is a runtime source expression | Incoming evaluation cannot be omitted because of a conflict; type errors can occur at analysis or at execution |
| N02, N03, N09, N11 | Incoming `NOT NULL` violation with `WHERE` false/true, `DO NOTHING`, and the insert branch | `23502` four times | Incoming rows are validated before the conflict action; a skipped row does not swallow the error |
| N06 | Incoming `CHECK` violation with `WHERE` false | `23514` | Same ordering; LakeSoul does not gain `CHECK` support from this |
| N04, N05, N07 | Output-row `NOT NULL`: `SET` produces NULL / `WHERE` false / second row fails late | `23502`; `INSERT 0 0` with no error; `23502` with both rows unchanged | Output-row constraints are checked only when the row is produced; a late error rolls back the whole statement |
| N08a, N08b | Same row pair, only source order differs (update branch `22012` vs insert branch `23502`) | `22012` / `23502` | Error precedence is **not** a contract |

### 1.1 Not a contract, not measured

- Error precedence between a runtime `SET` error and an output constraint error:
  N08a/N08b show the same row pair producing different SQLSTATEs purely from
  source order. Only typed errors and whole-statement zero publication are
  required.
- Duplicate-key state across batches, source partitions, workers and spill runs:
  PostgreSQL only provides a single-process sequential reference; the executor
  gate covers these directly.
- Conflict targets and partition-local uniqueness on PostgreSQL declarative
  `RANGE` partitioned tables were not measured; LakeSoul keeps `0A000`.
- Whether CDC tables should accept the general form was not measured and is not
  opened; existing CDC event writes and the restricted all-column form are
  unchanged by this work.
- Non-primary-key conflict targets: PostgreSQL `42P10`, LakeSoul stays `0A000`.
- Single source evaluation and bounded/spill behaviour cannot be shown by an
  oracle: they are executor properties, evidenced by the native tests below.

Two case labels in the first raw evidence dump were wrong (D13's constant
`WHERE false` applies to both rows; S17b's bound key did not conflict with an
existing row). They were replaced by D13R and S17b2; the raw outputs were kept
but their conclusions are not cited.

## 2. Numbered acceptance matrix

| ID | Scenario | Required observation |
|---|---|---|
| U01 | `UPDATE` with partial columns, constants, old-value expressions | Values correct, unassigned columns unchanged, explicit NULL applied |
| U02 | Multi-column swap, same-value assignment | `SET` uses the same old row; same-value matches still count |
| U03 | Non-CDC table, absent key, `WHERE` false | `UPDATE 0`, no file and no metadata publication |
| U04 | A historical version satisfies `WHERE`, the current one does not | No update; no historical row resurrected |
| U05 | Prepared statement rebound several times, data changes between executions | Each execution uses its own parameters and a new snapshot; the connection recovers after an error |
| U06 | UPSERT with mixed old and new keys, partial `INSERT`/`SET` columns | New keys follow `INSERT` rules; unassigned columns keep the old values |
| U07 | UPSERT old-value and `EXCLUDED` expressions, conflict `WHERE` | Expression results and row counts match PostgreSQL |
| U08 | Duplicate source keys including conflict-`WHERE` boundaries | Matches PostgreSQL `21000`/skip behaviour; a failure publishes nothing |
| U09 | VALUES/SELECT source evaluated once | Proved with an observable source: no duplicated scan side effects, no divergent branch results |
| U10 | Type, `NOT NULL` and expression failures discovered late | The whole statement fails, no partial result is exposed, cleanup does not delete published objects |
| U11 | Two connections incrementing the same old value concurrently | Both cannot succeed while only one increment lands; version conflict is `40001` and a whole-statement retry produces the correct result |
| U12 | Cross-coordinator and competing with a plain writer | No process lock; same-partition conflicts are reported honestly, other partitions are not refused for no reason |
| U13 | Multi-range-partition update with one conflicting partition | All partitions publish or none does; no partially visible metadata |
| U14 | `DROP` / same-name rebuild interleaved with prepared UPDATE/UPSERT | Nothing publishes to the wrong `table_id`; lifecycle contract and orphan-object checks hold |
| U15 | Key change, no primary key, non-default merge-op, transaction/batch | Explicit SQLSTATE, no persisted side effect |
| U16 | Querying an indexed table after an update | No superseded candidate leaks; index failure does not report a data rollback or delete committed files |
| U17 | Input above the memory budget | Operators spill or fail explicitly; correctness never depends on collecting everything |
| U18 | `UPDATE` on a CDC table, including empty table, `WHERE` false, explicit marker column and prepared Execute | Server-side `0A000`, no scan, no file and no metadata publication; the connection stays usable |
| U19 | Metadata `COMMIT` response lost, actually committed / definitely not committed | Unknown outcome retains files and does not claim a rollback; committed relations and files stay intact; cleanup only after a proven non-publication |
| U20 | UPSERT with an empty source, or every conflict `WHERE` false/NULL | `INSERT 0 0`, no new data file, no metadata publication; keys without a conflict are not filtered |
| U21 | Two coordinators UPSERTing the same key absent from their snapshot | No silent "key absent" branch overwriting; CAS conflict `40001`, whole-statement retry re-branches on the new snapshot |

Rejections and failures are checked not only by SQLSTATE but by reading primary
metadata, the actual files and the current relation. Concurrency tests use row
locks and barriers for deterministic interleavings, never sleep-based
collisions. Orphan checks query by the known `table_id`, not only through a
still-existing `table_info` join.

`U19`–`U21` are not included in the UPDATE gate counts below. `U13` was not
extended to range-partitioned UPSERT, and `U18` remains the UPDATE CDC refusal
contract.

## 3. Acceptance results

### 3.1 UPDATE

`test-fast` profile, `--jobs 4`, an isolated temporary metadata database
initialized from `script/meta_init.sql`. Native `libtest` initialization clears
that database, so it must not point at development data and must not run in
parallel with PG targets sharing it.

| Suite | legacy | v2 |
|---|---:|---:|
| PG `ddl_contract`, `insert_contract`, `session_contract`, `sql_compat`, `update_contract`, `vector_contract` | 20 passed | 20 passed |
| Native `tests::update_concurrency_tests` | 13 passed | 13 passed |

These are Rust target executions including shared-harness internal tests, not
distinct SQL cases. Metadata `commit_occ`, `update_commit`, `partition_as_of`,
`incremental_read` and `table_changelog` contributed 34 passes, independent of
the merge implementation.

- Protocol matrix: simple and extended, parameter rebinding, old-value
  expressions, quoted columns, CDC refusal, transaction/batch refusal,
  `DROP`/rebuild, execution-time errors and same-connection recovery.
- Concurrency matrix: real workers writing files, fixed snapshot, same-partition
  conflict, independent partitions, multi-partition failure with zero
  publication, `DROP` race and failed-object cleanup.
- A 64 KiB execution budget makes a large input fail with typed
  `ResourcesExhausted` while metadata and active files stay unchanged; this is
  not a process RSS bound or a long-running performance baseline.
- Vector and text indexes are verified to return the new value and not leak the
  superseded one; the vector index-only case confirms the new vector is indexed.
- An injected index-storage failure makes `UPDATE` return a server error while
  the commit count still advances by one, the new vector is visible, and other
  rows and all committed files are retained. It passes in both merge
  implementations, proving a post-commit index error is not a data rollback.
- The `MERGE` modifying CTE added last is refused by the guard with `0A000`
  instead of falling through to DataFusion `XX000`.

Independent `psql` smoke against the rebuilt service binary, without the
integration harness:

- `(1,10,10,20,'7','ordinary')` becomes `(1,11,20,10,NULL,'ordinary')` after an
  increment, a column swap and an explicit NULL, returning `UPDATE 1`; an
  ordinary column named `rowKinds` is not treated as CDC.
- Both old Parquet files keep their bytes and one new file is appended;
  `UPDATE 0` changes neither files nor metadata.
- Non-key `NOT NULL`, a runtime `CAST` and a multi-row scalar subquery return
  `23502`, `22P02` and `21000`; a modifying `UPDATE` CTE and `WHERE false` on an
  empty CDC table return `0A000`. After each error the same `psql` connection
  reads the old result, with no publication and no new file.
- The modifying `MERGE` CTE changed from `XX000` before the fix to `0A000`
  after it, the target value stayed 10, and a rebuilt binary again produced
  `11,'keep'` for a plain `UPDATE`.

### 3.2 General UPSERT (C7)

| Suite | legacy | v2 |
|---|---:|---:|
| PG `upsert_contract` (local, simple + extended) | passed | passed |
| PG `distributed_upsert_contract` (two real Flight workers, fallback disabled) | passed | passed |
| PG `indexed_upsert_contract` (vector/text indexes plus post-commit index failure) | passed | passed |
| PG `insert_contract` (plain `INSERT` regression, 3 tests) | passed | passed |
| Native `tests::sql_upsert_tests` (9 tests at C7 time) | passed | passed |

`upsert_contract` covers partial `INSERT`/`SET`, retention of unassigned
columns, explicit NULL, old-value and `EXCLUDED` expressions, simultaneous
assignment, same-value counting, conflict `WHERE` true/false/NULL, new keys
outside the filter, the repeated-key matrix (D02/D03/D06 and both D20
directions, R10), `ORDER BY` on a non-projected column, incoming vs output
constraint order, prepared rebinding on a new snapshot, `DROP` + same-name
rebuild, empty source and all-skipped sources, a user column sharing an
internal field name, CDC full-row compatibility, and the refusal matrix.

Native coverage: single source evaluation counter, duplicate-key source order,
1500 rows crossing the output batch boundary, empty/skipped sources publishing
nothing, a real spill inside a 64 MB budget (`spilled_bytes>0`), typed
`ResourcesExhausted` with zero publication when the budget is too small, and
three `40001` races (first insert into an empty table, existing key, plain
writer of the same partition) with whole-statement retry.

`7cafc2c1b` later grew that file to 14 tests (spill success, budget exhaustion
and the three `40001` races). The 9 above are what the C7 run covered; the
five additions have no recorded execution here and are not claimed as run.

### 3.3 COMMIT outcome unknown (F1)

A controlled PostgreSQL wire proxy intercepted metadata `COMMIT`: the database
committed but the response was dropped, and separately `COMMIT` was not
forwarded and the transaction was aborted. Both were replayed against a rebuilt
binary for `UPDATE` and for general UPSERT.

| Scenario | Client | Actual commit | Relation afterwards | Files |
|---|---|---|---|---|
| Committed, response lost | `XX000` outcome unknown | `commit +1` | The new value is visible | Existing files byte-identical, the new file retained |
| Not forwarded, transaction aborted | `XX000` outcome unknown | `commit` unchanged | The old value is kept | Existing files unchanged, the unpublished new file conservatively retained |

No automatic retry of an unknown-outcome statement and no orphan GC were added.
U19's two outcomes therefore have real evidence on the existing UPDATE/CAS path;
the raw proxy transcripts were session-local and are not committed here.

## 4. Scope of these results

- The counts above are recorded gate runs, not a claim that every PostgreSQL
  form is supported. Behavior boundaries and the refusal matrix stay in
  [README.md](README.md) under "Phase nine: UPDATE" and "General UPSERT".
- `U13` was exercised for single-table UPDATE only and is not extended to
  range-partitioned `ON CONFLICT`; `U18` remains the UPDATE CDC refusal
  contract.
- Raw artifacts referenced by earlier working notes (the C1 `psql` transcript,
  the C1 review, the F1 proxy JSON) were session-local and are not part of the
  repository; this file is the committed record.

Re-running the acceptance gates is described in
[README.md](README.md) under "General UPSERT" (C7 entry point) and
"Phase nine: UPDATE".
