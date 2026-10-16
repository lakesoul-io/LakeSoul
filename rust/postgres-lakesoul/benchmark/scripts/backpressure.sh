#!/usr/bin/env bash
# Slow-client backpressure matrix against an already deployed run directory.
#
#   scripts/backpressure.sh RUN_DIR BENCH_TAG [CASE...]
#
# CASE = NAME:DB:PROTOCOL:MAX_ROWS:READ_RATE[:STALL_AFTER:STALL_S]
#   READ_RATE is bytes/s or `max`; STALL_AFTER bytes before a STALL_S pause.
# Every case streams SQL (default: the full lineitem table) through one
# connection with a small receive buffer and is cancelled after
# QUERY_TIMEOUT seconds (CancelRequest), so slow cases measure what the
# server buffers while the client lags, and whether cancel still works.
# Env: SQL (default `SELECT * FROM lineitem`), QUERY_TIMEOUT (150), RCVBUF (65536).
set -euo pipefail
cd "$(dirname "$0")/.."
run=$1 bench=$2
shift 2
SQL=${SQL:-SELECT * FROM lineitem}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-150}
RCVBUF=${RCVBUF:-65536}
if [ $# -eq 0 ]; then
    set -- \
        pk-simple-max:tpch_sf1:simple:0:max \
        pk-simple-10m:tpch_sf1:simple:0:10485760 \
        pk-simple-1m:tpch_sf1:simple:0:1048576 \
        pk-simple-100k:tpch_sf1:simple:0:102400 \
        pk-simple-stall:tpch_sf1:simple:0:max:1048576:120 \
        pk-ext-all-1m:tpch_sf1:extended:0:1048576 \
        pk-ext-1000-1m:tpch_sf1:extended:1000:1048576 \
        nopk-simple-max:tpch_sf1_nopk:simple:0:max \
        nopk-simple-1m:tpch_sf1_nopk:simple:0:1048576 \
        nopk-simple-stall:tpch_sf1_nopk:simple:0:max:1048576:120
fi
for c in "$@"; do
    IFS=: read -r name db protocol max_rows rate stall_after stall_s <<<"$c"
    extra=()
    if [ -n "${stall_after:-}" ]; then
        extra+=(--stall-after-bytes "$stall_after" --stall-seconds "$stall_s")
    fi
    python3 scripts/bench.py drive --run-dir "$run" --bench-tag "$bench" --name "bp-$name" \
        --timeout $((QUERY_TIMEOUT + 600)) -- slowclient --host lakesoul-coordinator --database "$db" \
        --sql "$SQL" --protocol "$protocol" --max-rows "$max_rows" --read-rate "$rate" \
        --rcvbuf "$RCVBUF" --query-timeout "$QUERY_TIMEOUT" "${extra[@]}" || true
done
