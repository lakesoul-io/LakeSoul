#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
#
# Vector index benchmark orchestration (E1-E4).
#
# Datasets are read from local fvecs/ivecs files (no download); override the
# locations with DATA_DIR or the individual *_BASE/*_QUERY/*_GT variables.
#
# Usage:
#   script/benchmark/vector/run.sh [e1|e2|e3|e4|e5|all] [--quick] [--results DIR]
#
# Environment:
#   DATA_DIR      dataset root (default: ~/program/opensource/rabitq-rs/data)
#   RESULTS_DIR   output root (default: <repo>/benchmark-results/<timestamp>)
#   THREADS       worker threads (default: 16)
#   QUICK=1       reduced sizes for a smoke run
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

DATA_DIR="${DATA_DIR:-$HOME/program/opensource/rabitq-rs/data}"
GLOVE_DIR="$DATA_DIR/glove-200d/processed"
GIST_DIR="$DATA_DIR/gist"

GLOVE_BASE="$GLOVE_DIR/train.fvecs"
GLOVE_QUERY="$GLOVE_DIR/test.fvecs"
GLOVE_GT="$GLOVE_DIR/groundtruth.ivecs"

GIST_BASE="$GIST_DIR/gist_base.fvecs"
GIST_QUERY="$GIST_DIR/gist_query.fvecs"
GIST_GT="$GIST_DIR/gist_groundtruth.ivecs"
GIST_LEARN="$GIST_DIR/gist_learn.fvecs"

THREADS="${THREADS:-16}"
QUICK="${QUICK:-0}"

usage() {
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
}

SCENARIOS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        e1|e2|e3|e4|e5|all) SCENARIOS+=("$1"); shift ;;
        --quick) QUICK=1; shift ;;
        --results) RESULTS_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done
if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
    SCENARIOS=(all)
fi

RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/benchmark-results/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$RESULTS_DIR/logs" "$RESULTS_DIR/work"
RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"

BENCH=(cargo bench -p lakesoul-datafusion --bench vector_rebuild_bench --)

# Dataset-specific arguments; sets BASE/QUERY/GT/LEARN.
set_dataset() {
    case "$1" in
        glove)
            BASE="$GLOVE_BASE"; QUERY="$GLOVE_QUERY"; GT="$GLOVE_GT"; LEARN=""
            ;;
        gist)
            BASE="$GIST_BASE"; QUERY="$GIST_QUERY"; GT="$GIST_GT"
            LEARN="--learn $GIST_LEARN"
            ;;
        *) echo "unknown dataset: $1" >&2; exit 2 ;;
    esac
    DS_ARGS=(--base "$BASE" --query "$QUERY" --gt "$GT")
    if [[ -n "$LEARN" ]]; then
        # shellcheck disable=SC2206
        DS_ARGS+=($LEARN)
    fi
}

# run_one <name> [bench args...]
run_one() {
    local name="$1"; shift
    echo "── $name"
    RUST_LOG=warn "${BENCH[@]}" "$@" \
        --out "$RESULTS_DIR/$name.json" \
        >"$RESULTS_DIR/logs/$name.log" 2>&1 || {
        echo "   FAILED: $name (tail of $RESULTS_DIR/logs/$name.log)"
        tail -5 "$RESULTS_DIR/logs/$name.log" | sed 's/^/   /'
        return 1
    }
}

# ---------------------------------------------------------------------------
# E1: policy x drift quality/cost on the streaming workload
# ---------------------------------------------------------------------------
run_e1() {
    local ds="$1"
    set_dataset "$ds"
    local limit=100000 rounds=10 per=10000 ckpt=1 nq=100
    local ratios=(0.25 0.5 1.0 2.0)
    if [[ "$ds" == gist ]]; then
        ratios=(1.0)   # GIST stream runs are heavier: keep the sweep small
        ckpt=2
    fi
    if [[ "$QUICK" == 1 ]]; then
        limit=30000; rounds=4; per=3000; ckpt=1
    fi
    local drift
    for drift in uniform skew shift; do
        run_one "e1_${ds}_none_${drift}" "${DS_ARGS[@]}" \
            --scenario stream --limit "$limit" --nlist 256 --n-queries "$nq" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every "$ckpt" \
            --drift "$drift" --drift-strength 3.0 --policy none \
            --work-dir "$RESULTS_DIR/work/e1_${ds}_${drift}"
        local r
        for r in "${ratios[@]}"; do
            run_one "e1_${ds}_auto${r}_${drift}" "${DS_ARGS[@]}" \
                --scenario stream --limit "$limit" --nlist 256 --n-queries "$nq" \
                --rounds "$rounds" --per-round "$per" --checkpoint-every "$ckpt" \
                --drift "$drift" --drift-strength 3.0 --policy auto --max-delta-ratio "$r" \
                --work-dir "$RESULTS_DIR/work/e1_${ds}_${drift}"
        done
        run_one "e1_${ds}_periodic_${drift}" "${DS_ARGS[@]}" \
            --scenario stream --limit "$limit" --nlist 256 --n-queries "$nq" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every "$ckpt" \
            --drift "$drift" --drift-strength 3.0 --policy periodic --period 3 \
            --work-dir "$RESULTS_DIR/work/e1_${ds}_${drift}"
        run_one "e1_${ds}_always_${drift}" "${DS_ARGS[@]}" \
            --scenario stream --limit "$limit" --nlist 256 --n-queries "$nq" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every "$ckpt" \
            --drift "$drift" --drift-strength 3.0 --policy always \
            --work-dir "$RESULTS_DIR/work/e1_${ds}_${drift}"
    done
}

# ---------------------------------------------------------------------------
# E2: fresh build scaling (N x nlist)
# ---------------------------------------------------------------------------
run_e2() {
    local ds="$1"
    set_dataset "$ds"
    local sizes=(100000 300000 1000000)
    local nlists=(256 1024 4096)
    if [[ "$ds" == gist ]]; then
        nlists=(256 1024)
    fi
    if [[ "$QUICK" == 1 ]]; then
        sizes=(100000); nlists=(256)
    fi
    local n nl
    for n in "${sizes[@]}"; do
        for nl in "${nlists[@]}"; do
            run_one "e2_${ds}_n${n}_nlist${nl}" "${DS_ARGS[@]}" \
                --scenario build --limit "$n" --nlist "$nl" \
                --n-queries 100 --threads "$THREADS" \
                --work-dir "$RESULTS_DIR/work/e2_${ds}_n${n}_nlist${nl}"
        done
    done
}

# ---------------------------------------------------------------------------
# E3: per-cluster vs shard-level trigger analysis
# ---------------------------------------------------------------------------
run_e3() {
    local ds="$1"
    set_dataset "$ds"
    local limit=100000 rounds=10 per=10000 ckpt=1 nq=100
    if [[ "$QUICK" == 1 ]]; then
        limit=30000; rounds=4; per=3000
    fi
    local drift
    for drift in uniform skew shift; do
        run_one "e3_${ds}_${drift}" "${DS_ARGS[@]}" \
            --scenario trigger --limit "$limit" --nlist 256 --n-queries "$nq" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every "$ckpt" \
            --drift "$drift" --drift-strength 3.0 \
            --work-dir "$RESULTS_DIR/work/e3_${ds}_${drift}"
    done
}

# ---------------------------------------------------------------------------
# E4: search recall/QPS on fresh / delta / rebuilt index states
# ---------------------------------------------------------------------------
run_e4() {
    local ds="$1"
    set_dataset "$ds"
    local limit=100000 nlist=256 rounds=6 per=5000 nq=100
    if [[ "$QUICK" == 1 ]]; then
        limit=30000; rounds=3; per=3000
    fi
    local sweep="1,4,16,64,128,256"
    # Search GT is brute-forced over the vectors actually stored in the
    # index (base + deltas), so do not pass the dataset-wide --gt here.
    local search_args=(--base "$BASE" --query "$QUERY")

    # fresh state
    run_one "e4_${ds}_fresh_seed" "${DS_ARGS[@]}" \
        --scenario build --limit "$limit" --nlist "$nlist" --n-queries "$nq" \
        --threads "$THREADS" \
        --work-dir "$RESULTS_DIR/work/e4_${ds}_fresh"
    run_one "e4_${ds}_fresh" "${search_args[@]}" \
        --scenario search --reuse --limit "$limit" --nlist "$nlist" \
        --n-queries "$nq" --top-k 10 --nprobe-sweep "$sweep" \
        --threads "$THREADS" \
        --work-dir "$RESULTS_DIR/work/e4_${ds}_fresh"

    # accumulated-delta state (no rebuild)
    run_one "e4_${ds}_delta_seed" "${DS_ARGS[@]}" \
        --scenario stream --limit "$limit" --nlist "$nlist" --n-queries "$nq" \
        --rounds "$rounds" --per-round "$per" --checkpoint-every "$rounds" \
        --drift uniform --policy none --threads "$THREADS" \
        --work-dir "$RESULTS_DIR/work/e4_${ds}_delta"
    run_one "e4_${ds}_delta" "${search_args[@]}" \
        --scenario search --reuse --limit "$limit" --nlist "$nlist" \
        --n-queries "$nq" --top-k 10 --nprobe-sweep "$sweep" \
        --threads "$THREADS" \
        --work-dir "$RESULTS_DIR/work/e4_${ds}_delta"

    # rebuilt state (fresh base every round)
    run_one "e4_${ds}_rebuilt_seed" "${DS_ARGS[@]}" \
        --scenario stream --limit "$limit" --nlist "$nlist" --n-queries "$nq" \
        --rounds "$rounds" --per-round "$per" --checkpoint-every "$rounds" \
        --drift uniform --policy always --threads "$THREADS" \
        --work-dir "$RESULTS_DIR/work/e4_${ds}_rebuilt"
    run_one "e4_${ds}_rebuilt" "${search_args[@]}" \
        --scenario search --reuse --limit "$limit" --nlist "$nlist" \
        --n-queries "$nq" --top-k 10 --nprobe-sweep "$sweep" \
        --threads "$THREADS" \
        --work-dir "$RESULTS_DIR/work/e4_${ds}_rebuilt"
}

# ---------------------------------------------------------------------------
# E5: end-to-end DataFusion SQL (SQL INSERT + index-backed SQL search)
# ---------------------------------------------------------------------------
run_e5() {
    local ds="$1"
    set_dataset "$ds"
    local limit=100000 nlist=256 nq=100 rounds=10 per=10000
    if [[ "$ds" == gist ]]; then
        nq=50
    fi
    if [[ "$QUICK" == 1 ]]; then
        limit=20000; nlist=64; nq=20; rounds=2; per=1000
    fi
    rounds="${E5_ROUNDS:-$rounds}"
    per="${E5_PER_ROUND:-$per}"
    # Requires PostgreSQL metadata (same environment as the integration tests).
    # Base write + `rounds` incremental inserts through SQL; then SQL search.
    # No --gt/--learn: the ground truth is brute-forced over the written data
    # and the SQL scenario does not use an update pool.
    run_one "e5_${ds}" --base "$BASE" --query "$QUERY" \
        --scenario sql --limit "$limit" --nlist "$nlist" --n-queries "$nq" \
        --top-k 10 --nprobe 64 --threads "$THREADS" \
        --rounds "$rounds" --per-round "$per" --drift uniform \
        --table "vec_bench_sql_${ds}" \
        --work-dir "$RESULTS_DIR/work/e5_${ds}"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
echo "results: $RESULTS_DIR"
echo "threads: $THREADS  quick: $QUICK"

for scenario in "${SCENARIOS[@]}"; do
    case "$scenario" in
        all)
            run_e2 glove
            run_e1 glove
            run_e3 glove
            run_e4 glove
            run_e2 gist
            run_e1 gist
            run_e3 gist
            run_e4 gist
            run_e5 glove
            run_e5 gist
            ;;
        e1) run_e1 glove; run_e1 gist ;;
        e2) run_e2 glove; run_e2 gist ;;
        e3) run_e3 glove; run_e3 gist ;;
        e4) run_e4 glove; run_e4 gist ;;
        e5) run_e5 glove; run_e5 gist ;;
    esac
done

echo
echo "done. plot with:"
echo "  uv run --with matplotlib python script/benchmark/vector/plot.py --results $RESULTS_DIR"
