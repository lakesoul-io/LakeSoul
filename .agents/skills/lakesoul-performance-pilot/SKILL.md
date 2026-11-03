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
export MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000

systemd-run --user --scope --collect --unit=<pilot-unit> \
  -p CPUQuota=1600% \
  -p MemoryMax=48G \
  -p MemorySwapMax=0 \
  -- <pilot-command>
```

- Keep the same cgroup limits for every side of a comparison.
- `taskset` alone is not a memory limit; use the cgroup.
- Use `dirty_decay_ms=5000` and `muzzy_decay_ms=5000`.  `0` disables decay
  and was measured slower on the 1M pilot; do not use it for comparisons.
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
- Run the compaction listener JVM with the same jemalloc settings as the
  pilot: `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so` and
  `MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000`.
  Without it the Spark driver uses glibc malloc, which shows up as a large
  `_int_malloc`/`malloc_consolidate`/`free` share in compaction profiles.
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

When reusing a table root, keep the source tables but clear stale IVM
state/MV metadata and data directories first.  Otherwise old commits,
partitions, or cache entries leak into the next run and waste disk/compaction
work.

- Delete only the `mv` table metadata and directory; keep the source tables.
- Drop the IVM view registry/cursors, then re-bootstrap the target from the
  existing sources (the pilot supports `PILOT_REUSE_SOURCE`).
- Remove old table roots, spill directories, and per-run cache directories.
- Use a separate cache path per experiment so one run cannot warm another.
- Re-bootstrap only when creating a new base; reuse a base only with a new
  change window so versions do not accumulate.

## 7. Report consistently

Report bootstrap and incremental refresh separately. Measure only the refresh
execution time, excluding source appends, oracle comparison, and external
compaction. Record CPU/memory limits, cache size, compaction state, file format,
and fallback status with every comparison.

## 8. Flamegraph profiling

Use `cargo flamegraph` with the pilot's `profiling` profile (`opt-level = 2`,
`debug = "line-tables-only"`).  Build and run it under the same jemalloc and
cgroup settings as the benchmark:

```bash
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
export MALLOC_CONF=background_thread:true,dirty_decay_ms:5000,muzzy_decay_ms:5000
export RUSTFLAGS='-Ctarget-feature=+avx2 -Clink-arg=-Wl,--no-rosegment'

systemd-run --user --scope --collect --unit=<pilot-flame> \
  -p CPUQuota=1600% -p MemoryMax=48G -p MemorySwapMax=0 \
  -- cargo flamegraph --profile profiling --no-inline --freq 99 \
       --bin <pilot-binary> -o flame-<case>.svg
```

- Set `RUSTFLAGS='-Ctarget-feature=+avx2 -Clink-arg=-Wl,--no-rosegment'`
  when building the profiling binary.  Without `--no-rosegment` the linker
  emits a separate read-only segment and perf cannot unwind the Rust stacks
  reliably, so most samples show up as `[unknown]`.  `RUSTFLAGS` from the
  environment overrides the repository's `.cargo/config.toml`, so keep the
  `-Ctarget-feature=+avx2` flag explicitly; otherwise the profiling build
  loses the AVX2 codegen used by the release build.
- Changing `RUSTFLAGS` changes Cargo's fingerprint, so the profiling profile
  is rebuilt once with the flags.
- `--no-inline` keeps inlined callees out of the stack, so the SVG attributes
  samples to real functions.  It can under-report tiny inlined helpers, but it
  is the consistent choice for comparing flamegraphs.
- `--freq 99` is the low-overhead sampling rate used for the 1M/10M pilots.
  Do not leave the default 997 Hz for long runs if you need the run's wall
  time to stay close to the unprofiled number.
- Use the same jemalloc `MALLOC_CONF` (including `dirty_decay_ms=5000` and
  `muzzy_decay_ms=5000`) and the same cgroup limits as the timed run.
- Stop external compaction and use a fresh `LAKESOUL_CACHE_PATH` before the
  profile, exactly as for a timed window.
- Check that perf is permitted before profiling:
  `cat /proc/sys/kernel/perf_event_paranoid`; a value above 1 needs `sudo` or
  a temporary sysctl change.
- If perf reports `lost ... chunks` or the SVG has large `[unknown]`
  sections, the stacks are incomplete: reduce the profiled window, raise the
  perf ring buffer / `perf_event_mlock_kb`, or try
  `--call-graph=fp` when the binary keeps frame pointers.  Do not attribute
  the missing samples to a function.
- The SVG is drawn with the call stack **bottom-up**: the root is at the
  bottom, callers sit below their callees, and the leaves are at the top.
  Read it bottom-up when following a hot path.  `cargo flamegraph
  --inverted` draws the opposite orientation, so do not mix orientations
  between runs.

## 9. Performance builds: `profiling`, not `release`

All pilot performance builds (Rust libraries, Spark JNI libraries and the
pilot binary) use the `profiling` profile with line tables and without the
linker's read-only segment.  Do not use `--release` for a performance run.

Build the two C FFI libraries from the repository root:

```bash
RUSTFLAGS='-Ctarget-feature=+avx2 -Clink-arg=-Wl,--no-rosegment' \
  cargo -q build --package lakesoul-io-c --package lakesoul-metadata-c \
                 --profile profiling

cp rust/target/profiling/lib*.so rust/target/release/
```

- The `.cargo/config.toml` sets `target-dir = "rust/target"`.  Without an
  explicit `--target`, the output is `rust/target/profiling`; with
  `--target x86_64-unknown-linux-gnu` it is
  `rust/target/x86_64-unknown-linux-gnu/profiling`.  Verify the path with
  `find rust/target -name 'liblakesoul_io_c.so'` if unsure.
- `debug = "line-tables-only"` is set in `[profile.profiling]`; the
  `-Clink-arg=-Wl,--no-rosegment` flag is required for native unwinding.
  Keep `-Ctarget-feature=+avx2`: environment `RUSTFLAGS` overrides the
  repository's target rustflags, so omitting it silently drops AVX2.
- Copying into `rust/target/release` is what the Spark cross-build and the
  compaction listener's `LD_LIBRARY_PATH` consume.

Build the Spark jar against those libraries:

```bash
mvn -q package -pl lakesoul-spark -am -Pcross-build -DskipTests
```

`-Pcross-build` uses `rust/target/release` and does not invoke Cargo.

Build the pilot binary with the same profile.  Do **not** use `--release`:
switching profiles forces a full recompilation and produces numbers on a
different binary.

```bash
cd /home/chenxu/ivm-pilot
RUSTFLAGS='-Ctarget-feature=+avx2 -Clink-arg=-Wl,--no-rosegment' \
  cargo build --profile profiling --bin verify_salorderentry_scale
```

### async-profiler for compaction

The local async-profiler is at
`/home/chenxu/soft/async-profiler-4.6-linux-x64`.  Run the compaction
listener JVM with jemalloc as well (the same `LD_PRELOAD` and `MALLOC_CONF`
as the pilot; put them in `start_tuned_compaction*.sh`).  Attach the profiler
to the Spark driver JVM (`NewCompactionTask`) while a compaction step is
running:

```bash
/home/chenxu/soft/async-profiler-4.6-linux-x64/bin/asprof \
  -e cpu -d 60 --cstack fp \
  -o collapsed -f /tmp/opencode/compaction.collapsed <jvm-pid>
```

- `-e cpu` samples CPU; `--cstack fp` (or `dwarf`) walks native frames.
  Do not add `-F mixed`: this JVM rejects it with
  `mixed feature is only allowed with VMStructs stack walking`.
- The native libraries must be the `--no-rosegment` profiling build above,
  otherwise the Rust frames show up as `[unknown]`.
- `-o collapsed` is useful for scripted aggregation; `-o flamegraph` writes
  an HTML/SVG flamegraph.  `-o flamegraph` and `-o collapsed` are separate
  runs.
- Attach only after the compaction notification has started (the log shows
  `begining`); do not profile the idle listener.
- On a cold first compaction the profile also contains C2 JIT compiler and
  G1 young-GC threads.  Do not attribute those samples to the Rust merge.
  For a steady-state profile, take a second 30s sample after the JVM has
  compiled the compaction path.


