#!/usr/bin/env bash
# Coordinator<-worker buffer budget vs slow client, against an already deployed
# run directory (distributed: workers >= 1, nopk table so the scan is shipped).
#
#   scripts/buffer-budget.sh RUN_DIR BENCH_TAG [CASE...]
#
# CASE = BUDGET_BYTES:READ_RATE[:STALL_AFTER:STALL_S]; BUDGET `default` skips the SET.
# Each case sets `distributed.worker_connection_buffer_budget_bytes` on the
# connection, streams SQL (default `SELECT * FROM lineitem` on tpch_sf1_nopk)
# and is cancelled after QUERY_TIMEOUT seconds. Compare coordinator/worker RSS
# across budgets with `scripts/report.py summary RUN_DIR --name bb-...`.
# `SHOW distributed.*` returns an empty value; a rejected SET is only visible
# as a coordinator `SET statement ... ignored` warning, so check the log.
# Env: SQL, DB (tpch_sf1_nopk), QUERY_TIMEOUT (150), RCVBUF (65536).
set -euo pipefail
cd "$(dirname "$0")/.."
run=$1 bench=$2
shift 2
SQL=${SQL:-SELECT * FROM lineitem}
DB=${DB:-tpch_sf1_nopk}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-150}
RCVBUF=${RCVBUF:-65536}
if [ $# -eq 0 ]; then
    set -- 1048576:max:1048576:120 default:max:1048576:120 536870912:max:1048576:120 \
        1048576:1048576 default:1048576
fi
for c in "$@"; do
    IFS=: read -r budget rate stall_after stall_s <<<"$c"
    extra=()
    name="bb-$budget-$rate"
    if [ -n "${stall_after:-}" ]; then
        extra+=(--stall-after-bytes "$stall_after" --stall-seconds "$stall_s")
        name="$name-stall"
    fi
    if [ "$budget" != default ]; then
        extra+=(--setup "SET distributed.worker_connection_buffer_budget_bytes = $budget")
    fi
    python3 scripts/bench.py drive --run-dir "$run" --bench-tag "$bench" --name "$name" \
        --timeout $((QUERY_TIMEOUT + 600)) -- slowclient --host lakesoul-coordinator --database "$DB" \
        --sql "$SQL" --read-rate "$rate" --rcvbuf "$RCVBUF" --query-timeout "$QUERY_TIMEOUT" \
        "${extra[@]}" || true
done
