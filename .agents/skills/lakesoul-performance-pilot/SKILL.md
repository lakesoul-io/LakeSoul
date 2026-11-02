---
name: lakesoul-performance-pilot
description: Run LakeSoul IVM/performance pilots consistently: Vortex-only data files, jemalloc plus cgroup limits, cargo test with test-fast, and compaction before any measurement. Use when launching or comparing a LakeSoul pilot benchmark.
---
# LakeSoul Performance Pilot

Use this checklist before every LakeSoul pilot run or performance comparison.
Skipping one of these steps makes the numbers incomparable.

## 1. All table files must be Vortex

Source tables, target/materialized-view tables, and every internal state or
derived-step table must use Vortex. Do not benchmark a mix of Parquet and
Vortex.

- Create pilot source/target tables with `PhysicalFormat::Vortex`.
- Keep `LAKESOUL_IVM_INTERNAL_FILE_FORMAT` unset or set it to `vortex`; do not
  set it to `parquet`.
- After a run, verify every data file under the table root:

```bash
find <table-root> -type f \( -name '*.parquet' -o -name '*.vortex' \) \
  | sed 's/.*\.//' | sort | uniq -c
```

The result must contain only `vortex`. If any `parquet` file appears, fix the
table creation options before measuring.

## 2. Run pilots with jemalloc and a cgroup

Use jemalloc and an explicit cgroup for CPU and memory. Run the pilot inside
the scope so the limits apply to the whole process tree:

```bash
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
export MALLOC_CONF=background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0

systemd-run --user --scope --collect --unit=<pilot-unit> \
  -p CPUQuota=1600% \
  -p MemoryMax=48G \
  -p MemorySwapMax=0 \
  -- <pilot-command>
```

- Keep the same cgroup limits for every side of a comparison.
- `taskset` alone is not a memory limit; use the cgroup.
- Sample `/sys/fs/cgroup/<path>/memory.current` during the run when
  `memory.peak` is unavailable, and report the sampled peak.
- Include `LAKESOUL_CACHE=1` and an explicit cache size/path for keyed lookups.

## 3. Use the `test-fast` profile

Always run tests with:

```bash
cargo -q test --profile test-fast ...
```

Do not use the default test profile for pilot validation. Use
`cargo -q clippy -p <crate> --all-targets` for lint checks.

## 4. Compaction must be running

Before any performance number is recorded, start `NewCompactionTask` with the
tuned settings:

```bash
./ops/start_tuned_compaction.sh > /tmp/compaction.log 2>&1 &
```

- Verify compaction commits are actually happening, not only that the listener
  process exists.
- Include `%__partials` in list-based compaction table filters, or use
  percentage-based discovery so partial state tables are covered.
- Compact source and hot MV/state tables between windows.
- Do not compare a compacted run with an uncompacted run.
- Run compaction as a separate, bounded phase.  Put the listener in its own
  cgroup (for example `CPUQuota=400%`, `MemoryMax=8G`), let it drain pending
  notifications, then stop it before the timed refresh.  Do not let a listener
  started on the whole machine process notifications while the pilot is being
  measured: a fresh table root can replay dozens of notifications and saturate
  every CPU.
- Stop the external compaction listener before running the automated test
  suites.  Concurrent compaction of temporary test tables can race with the
  tests; it is a pilot-only service.

## 5. Validate before benchmarking

- Confirm no persistent lookup fallback:

```bash
RUST_LOG=warn <pilot-command> 2>&1 \
  | rg 'persistent pk lookup unavailable|failed to build pk index'
```

No such line should appear for the path being measured. A fallback means the
number reflects the reader path, not the intended keyed lookup.

- Confirm the index/cache path is enabled and report cache hit/miss deltas.
- Confirm all tables use Vortex with the file-extension check above.
- Confirm compaction is active and has committed at least once during or
  between windows.

## 6. Clean state between experiments

When reusing a table root, clear stale IVM metadata and data directories first.
Otherwise old commits, partitions, or cache entries leak into the next run.

- Reset the test metadata with the repository cleanup script.
- Remove old table roots, spill directories, and per-run cache directories.
- Use a separate cache path per experiment so one run cannot warm another.
- Re-bootstrap only when creating a new base; reuse a base only with a new
  change window so versions do not accumulate.

## 7. Report consistently

Report bootstrap and incremental refresh separately. Measure only the refresh
execution time, excluding source appends, oracle comparison, and external
compaction. Record CPU/memory limits, cache size, compaction state, file format,
and fallback status with every comparison.
