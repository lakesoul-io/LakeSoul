#!/usr/bin/env bash
# Admission/overload and isolation scenarios against a deployed run directory.
#
#   scripts/concurrency.sh RUN_DIR BENCH_TAG closed|open|isolation
#
# closed:    closed-loop MIX at each of LEVELS clients for STEP_S seconds.
# open:      Poisson open-loop MIX at each of RATES qps for STEP_S seconds.
# isolation: short-query mix alone, then concurrently with a CPU-heavy and
#            with a memory-heavy background (both closed-loop, BG_CLIENTS).
# Env: DB (tpch_sf1), SF (1), MIX (/bench/workloads/mixes/mixed.json),
#      LEVELS ("1 2 4 8 16 32 64 128"), RATES ("0.25 0.5 1 2 4 8"),
#      STEP_S (60), QUERY_TIMEOUT (120), BG_CLIENTS (4), FG_CLIENTS (2).
set -euo pipefail
cd "$(dirname "$0")/.."
run=$1 bench=$2 mode=$3
DB=${DB:-tpch_sf1}
SF=${SF:-1}
MIX=${MIX:-/bench/workloads/mixes/mixed.json}
LEVELS=${LEVELS:-1 2 4 8 16 32 64 128}
RATES=${RATES:-0.25 0.5 1 2 4 8}
STEP_S=${STEP_S:-60}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-120}
BG_CLIENTS=${BG_CLIENTS:-4}
FG_CLIENTS=${FG_CLIENTS:-2}
mixes=/bench/workloads/mixes

load() { # NAME MIX ARGS...
    local name=$1 mix=$2
    shift 2
    python3 scripts/bench.py drive --run-dir "$run" --bench-tag "$bench" --name "$name" \
        --timeout $((STEP_S + QUERY_TIMEOUT + 600)) -- load --host lakesoul-coordinator \
        --database "$DB" --sf "$SF" --mix "$mix" --duration "$STEP_S" \
        --query-timeout "$QUERY_TIMEOUT" "$@" || true
}

case "$mode" in
closed)
    for c in $LEVELS; do
        load "closed-c$c" "$MIX" --mode closed --concurrency "$c" --max-inflight $((c > 256 ? c : 256))
    done
    ;;
open)
    for r in $RATES; do
        load "open-r$r" "$MIX" --mode open --rate "$r" --max-inflight 256
    done
    ;;
isolation)
    load iso-short-alone "$mixes/short.json" --mode closed --concurrency "$FG_CLIENTS"
    for bg in cpu mem; do
        # Background outlasts the foreground window (20 s head start + STEP_S);
        # the later --duration overrides load()'s.
        load "iso-bg-$bg" "$mixes/$bg.json" --mode closed --concurrency "$BG_CLIENTS" \
            --duration $((STEP_S + 40)) &
        pid=$!
        sleep 20 # let the background saturate before measuring the foreground
        load "iso-short-with-$bg" "$mixes/short.json" --mode closed --concurrency "$FG_CLIENTS"
        wait "$pid"
    done
    ;;
*)
    echo "mode must be closed, open or isolation" >&2
    exit 2
    ;;
esac
