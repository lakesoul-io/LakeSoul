#!/usr/bin/env bash
# Memory-governance matrix: execution-pool budget x container limit x pool kind.
#
#   scripts/memory-matrix.sh RESULTS_PREFIX SERVICE_TAG BENCH_TAG CELL...
#
# CELL = PROFILE:BUDGET:POOL, e.g. small:1Gi:fair. BUDGET=none leaves
# LAKESOUL_MEMORY_LIMIT unset (baseline images ignore it anyway).
# Each cell gets a fresh (cold) deployment and runs the memory-heavy SF10
# TPC-H queries once, then a closed-loop burst of the mix in MIX.
# Env overrides: QUERIES (default q18,q09,q21,q13), DB (tpch_sf10), SF (10),
# MIX (/bench/workloads/mixes/mem.json, path inside the bench image),
# BURST_CONCURRENCY (4), BURST_SECONDS (180), QUERY_TIMEOUT (900).
set -euo pipefail
cd "$(dirname "$0")/.."
prefix=$1 service=$2 bench=$3
shift 3
QUERIES=${QUERIES:-q18,q09,q21,q13}
DB=${DB:-tpch_sf10}
SF=${SF:-10}
MIX=${MIX:-/bench/workloads/mixes/mem.json}
BURST_CONCURRENCY=${BURST_CONCURRENCY:-4}
BURST_SECONDS=${BURST_SECONDS:-180}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-900}

for cell in "$@"; do
    IFS=: read -r profile budget pool <<<"$cell"
    run="results/${prefix}-${profile}-${budget}-${pool}"
    env_args=(--env "all:LAKESOUL_MEMORY_POOL=$pool")
    [ "$budget" != none ] && env_args+=(--env "all:LAKESOUL_MEMORY_LIMIT=$budget")
    python3 scripts/bench.py deploy --run-dir "$run" --profile "$profile" \
        --service-tag "$service" --bench-tag "$bench" --workers 2 "${env_args[@]}"
    python3 scripts/bench.py drive --run-dir "$run" --bench-tag "$bench" --name serial \
        --timeout 5400 -- tpch --host lakesoul-coordinator --database "$DB" --sf "$SF" \
        --queries "$QUERIES" --iterations 1 --warmup 0 --query-timeout "$QUERY_TIMEOUT" \
        --max-outages 1 || true
    if [ "$BURST_SECONDS" != 0 ]; then
        python3 scripts/bench.py drive --run-dir "$run" --bench-tag "$bench" --name burst \
            --timeout $((BURST_SECONDS + QUERY_TIMEOUT + 300)) -- load --host lakesoul-coordinator \
            --database "$DB" --sf "$SF" --mode closed --concurrency "$BURST_CONCURRENCY" \
            --duration "$BURST_SECONDS" --mix "$MIX" --query-timeout "$QUERY_TIMEOUT" || true
    fi
    python3 scripts/report.py summary "$run" >"$run/summary.md" || true
done
