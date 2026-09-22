#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
#
# Text index benchmark orchestration (T1-T7).
#
# Datasets are streamed from the Hugging Face Hub on first use and cached
# under DATA_DIR; the Rust harness itself never touches the network.
#
# Usage:
#   script/benchmark/text/run.sh [quick|t1|t2|t3|t4|t5|t6|t7|all] [--results DIR]
#
# Environment:
#   DATA_DIR      dataset root (default: ~/data/lakesoul-text-bench)
#   RESULTS_DIR   output root (default: <repo>/benchmark-results/text-<ts>)
#   THREADS       worker threads reported in the summaries (default: 16)
#   LAKESOUL_PG_* PostgreSQL metadata connection (required)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

DATA_DIR="${DATA_DIR:-$HOME/data/lakesoul-text-bench}"
THREADS="${THREADS:-16}"
RESULTS_DIR="${RESULTS_DIR:-}"
QUICK=0

usage() {
    sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
}

SCENARIOS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        quick|t1|t2|t3|t4|t5|t6|t7|all) SCENARIOS+=("$1"); shift ;;
        --results) RESULTS_DIR="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done
if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
    SCENARIOS=(quick)
fi

if [[ -z "$RESULTS_DIR" ]]; then
    RESULTS_DIR="$REPO_ROOT/benchmark-results/text-$(date +%Y%m%d-%H%M%S)"
fi
mkdir -p "$RESULTS_DIR/logs" "$RESULTS_DIR/work"
RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
mkdir -p "$DATA_DIR"

BENCH=(cargo bench -p lakesoul-datafusion --bench text_index_bench --)

# ---------------------------------------------------------------------------
# Dataset preparation
# ---------------------------------------------------------------------------

# ensure_dataset <name> <limit|""> -> prints the dataset directory
ensure_dataset() {
    local name="$1" limit="${2:-}"
    local dir="$DATA_DIR/$name${limit:+-$limit}"
    if [[ ! -f "$dir/corpus.jsonl" || ! -f "$dir/queries.jsonl" ]]; then
        echo "  downloading $name${limit:+ (limit $limit)} -> $dir" >&2
        local args=(--out "$dir")
        if [[ -n "$limit" ]]; then
            args+=(--limit "$limit")
        fi
        uv run --with datasets python script/benchmark/text/download.py \
            "$name" "${args[@]}" >&2
    fi
    echo "$dir"
}

# run_one <name> -- <bench args...>
run_one() {
    local name="$1"; shift
    [[ "$1" == "--" ]] && shift
    echo "── $name"
    RUST_LOG=warn "${BENCH[@]}" "$@" \
        --out "$RESULTS_DIR/$name.json" \
        >"$RESULTS_DIR/logs/$name.log" 2>&1 || {
        echo "   FAILED: $name (tail of $RESULTS_DIR/logs/$name.log)"
        tail -5 "$RESULTS_DIR/logs/$name.log" | sed 's/^/   /'
        FAILURES=$((FAILURES + 1))
        return 0
    }
}

# ---------------------------------------------------------------------------
# T1: incremental update policies
# ---------------------------------------------------------------------------

run_t1() {
    local ds dir limit rounds per
    ds="${T1_DATASET:-msmarco}"
    if [[ "$QUICK" == 1 ]]; then
        ds=scifact; dir="$(ensure_dataset scifact)"
        limit=4000; rounds=3; per=500
    else
        limit=100000; rounds=10; per=10000
        dir="$(ensure_dataset "$ds" "$limit")"
    fi
    local q="$dir/queries.jsonl" qr="$dir/qrels.tsv" c="$dir/corpus.jsonl"

    # Append growth across the policies (the core write/read trade-off).
    for policy in none auto periodic always; do
        local ratio_arg=()
        [[ "$policy" == auto ]] && ratio_arg=(--max-delta-ratio 1.0)
        run_one "t1_${ds}_${policy}_append" -- \
            --scenario stream --corpus "$c" --queries "$q" --qrels "$qr" \
            --limit "$limit" --query-limit 300 \
            --work-dir "$RESULTS_DIR/work/t1_${ds}_${policy}" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every 1 \
            --policy "$policy" "${ratio_arg[@]}" --drift append \
            --candidates 100 --top-k 10
    done

    # Ratio sweep under append.
    local ratio
    for ratio in 0.25 0.5 2.0; do
        run_one "t1_${ds}_auto${ratio}_append" -- \
            --scenario stream --corpus "$c" --queries "$q" --qrels "$qr" \
            --limit "$limit" --query-limit 300 \
            --work-dir "$RESULTS_DIR/work/t1_${ds}_auto${ratio}" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every 1 \
            --policy auto --max-delta-ratio "$ratio" --drift append \
            --candidates 100 --top-k 10
    done

    # Rewrite / delete / topic-shift under the default auto policy.
    local drift
    for drift in rewrite delete topic-shift; do
        run_one "t1_${ds}_auto1.0_${drift}" -- \
            --scenario stream --corpus "$c" --queries "$q" --qrels "$qr" \
            --limit "$limit" --query-limit 300 \
            --work-dir "$RESULTS_DIR/work/t1_${ds}_${drift}" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every 1 \
            --policy auto --max-delta-ratio 1.0 --drift "$drift" \
            --candidates 100 --top-k 10
    done
}

# ---------------------------------------------------------------------------
# T2: fresh build scaling + tokenizer/parameter matrix
# ---------------------------------------------------------------------------

run_t2() {
    local ds="msmarco"
    local sizes tokenizers positions stored
    if [[ "$QUICK" == 1 ]]; then
        local dir; dir="$(ensure_dataset scifact)"
        for tokenizer in jieba default en_stem; do
            run_one "t2_scifact_${tokenizer}_pos" -- \
                --scenario build --corpus "$dir/corpus.jsonl" \
                --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
                --limit 5183 --query-limit 100 \
                --work-dir "$RESULTS_DIR/work/t2_scifact_$tokenizer" \
                --tokenizer "$tokenizer" --candidates 100 --top-k 10
        done
        return
    fi

    # Tokenizer x positions x stored matrix at 100K documents.
    local dir; dir="$(ensure_dataset "$ds" 100000)"
    for tokenizer in jieba default en_stem; do
        for positions in pos nopos; do
            for stored in plain stored; do
                local flags=()
                [[ "$positions" == nopos ]] && flags+=(--no-positions)
                [[ "$stored" == stored ]] && flags+=(--stored)
                run_one "t2_${ds}_${tokenizer}_${positions}_${stored}" -- \
                    --scenario build --corpus "$dir/corpus.jsonl" \
                    --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
                    --limit 100000 --query-limit 300 \
                    --work-dir "$RESULTS_DIR/work/t2_${ds}_${tokenizer}_${positions}_${stored}" \
                    --tokenizer "$tokenizer" "${flags[@]}" \
                    --candidates 100 --top-k 10
            done
        done
    done

    # Scaling at the default configuration.
    local limit_dir
    for limit in 300000 1000000 2000000; do
        limit_dir="$(ensure_dataset "$ds" "$limit")"
        run_one "t2_${ds}_scale${limit}" -- \
            --scenario build --corpus "$limit_dir/corpus.jsonl" \
            --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
            --limit "$limit" --query-limit 100 \
            --work-dir "$RESULTS_DIR/work/t2_${ds}_scale$limit" \
            --candidates 100 --top-k 10
    done

    # Chinese parameter matrix (jieba vs default) on T2Retrieval.
    local zh; zh="$(ensure_dataset t2retrieval)"
    for tokenizer in jieba default; do
        run_one "t2_t2retrieval_${tokenizer}" -- \
            --scenario build --corpus "$zh/corpus.jsonl" \
            --queries "$zh/queries.jsonl" --qrels "$zh/qrels.tsv" \
            --limit 100000 --query-limit 300 \
            --work-dir "$RESULTS_DIR/work/t2_t2retrieval_$tokenizer" \
            --tokenizer "$tokenizer" --candidates 100 --top-k 10
    done
}

# ---------------------------------------------------------------------------
# T3: drift trigger analysis (no rebuilds)
# ---------------------------------------------------------------------------

run_t3() {
    local ds dir limit rounds per
    ds="${T3_DATASET:-msmarco}"
    if [[ "$QUICK" == 1 ]]; then
        ds=scifact; dir="$(ensure_dataset scifact)"; limit=4000; rounds=4; per=500
    else
        limit=100000; rounds=10; per=10000
        dir="$(ensure_dataset "$ds" "$limit")"
    fi
    local drift
    for drift in append rewrite topic-shift; do
        run_one "t3_${ds}_${drift}" -- \
            --scenario trigger --corpus "$dir/corpus.jsonl" \
            --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
            --limit "$limit" --query-limit 200 \
            --work-dir "$RESULTS_DIR/work/t3_${ds}_${drift}" \
            --rounds "$rounds" --per-round "$per" --checkpoint-every 1 \
            --policy none --drift "$drift" --candidates 100 --top-k 10
    done
}

# ---------------------------------------------------------------------------
# T4: search across index states (fresh / delta / rebuilt)
# ---------------------------------------------------------------------------

run_t4() {
    local ds dir limit rounds per
    ds="${T4_DATASET:-msmarco}"
    if [[ "$QUICK" == 1 ]]; then
        ds=scifact; dir="$(ensure_dataset scifact)"; limit=4000; rounds=2; per=500
    else
        limit=100000; rounds=6; per=5000
        dir="$(ensure_dataset "$ds" "$limit")"
    fi
    local c="$dir/corpus.jsonl" q="$dir/queries.jsonl" qr="$dir/qrels.tsv"

    run_one "t4_${ds}_fresh_build" -- \
        --scenario build --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_fresh" \
        --candidates 100 --top-k 10
    run_one "t4_${ds}_fresh_search" -- \
        --scenario search --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 --reuse \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_fresh" \
        --candidate-sweep 10,20,40,100,200 --top-k 10

    run_one "t4_${ds}_delta_build" -- \
        --scenario stream --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_delta" \
        --rounds "$rounds" --per-round "$per" --checkpoint-every "$rounds" \
        --policy none --drift append --candidates 100 --top-k 10
    run_one "t4_${ds}_delta_search" -- \
        --scenario search --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 --reuse \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_delta" \
        --candidate-sweep 10,20,40,100,200 --top-k 10

    run_one "t4_${ds}_rebuilt_build" -- \
        --scenario stream --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_rebuilt" \
        --rounds "$rounds" --per-round "$per" --checkpoint-every "$rounds" \
        --policy always --drift append --candidates 100 --top-k 10
    run_one "t4_${ds}_rebuilt_search" -- \
        --scenario search --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 --reuse \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_rebuilt" \
        --candidate-sweep 10,20,40,100,200 --top-k 10

    # Verification cost: same workload without the exact pass.
    run_one "t4_${ds}_fresh_search_noverify" -- \
        --scenario search --corpus "$c" --queries "$q" --qrels "$qr" \
        --limit "$limit" --query-limit 300 --reuse --no-verify \
        --work-dir "$RESULTS_DIR/work/t4_${ds}_fresh" \
        --candidate-sweep 100 --top-k 10
}

# ---------------------------------------------------------------------------
# T5: shard-count sweep
# ---------------------------------------------------------------------------

run_t5() {
    local ds dir limit
    if [[ "$QUICK" == 1 ]]; then
        dir="$(ensure_dataset scifact)"; limit=4000; ds=scifact
    else
        ds="${T5_DATASET:-msmarco}"; limit=100000
        dir="$(ensure_dataset "$ds" "$limit")"
    fi
    local shards
    for shards in 1 4 16 64; do
        [[ "$shards" -gt "$limit" ]] && continue
        run_one "t5_${ds}_shards${shards}_build" -- \
            --scenario build --corpus "$dir/corpus.jsonl" \
            --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
            --limit "$limit" --query-limit 300 --shards "$shards" \
            --work-dir "$RESULTS_DIR/work/t5_${ds}_shards$shards" \
            --candidates 100 --top-k 10
        run_one "t5_${ds}_shards${shards}_search" -- \
            --scenario search --corpus "$dir/corpus.jsonl" \
            --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
            --limit "$limit" --query-limit 300 --shards "$shards" --reuse \
            --work-dir "$RESULTS_DIR/work/t5_${ds}_shards$shards" \
            --candidate-sweep 10,20,40,100,200 --top-k 10
    done
}

# ---------------------------------------------------------------------------
# T6: end-to-end SQL
# ---------------------------------------------------------------------------

run_t6() {
    local ds dir limit rounds per
    ds="${T6_DATASET:-msmarco}"
    if [[ "$QUICK" == 1 ]]; then
        ds=scifact; dir="$(ensure_dataset scifact)"; limit=4000; rounds=2; per=500
    else
        limit=100000; rounds=2; per=10000
        dir="$(ensure_dataset "$ds" "$limit")"
    fi
    run_one "t6_${ds}_sql" -- \
        --scenario sql --corpus "$dir/corpus.jsonl" \
        --queries "$dir/queries.jsonl" --qrels "$dir/qrels.tsv" \
        --limit "$limit" --query-limit "${T6_QUERIES:-100}" \
        --work-dir "$RESULTS_DIR/work/t6_${ds}" \
        --rounds "$rounds" --per-round "$per" \
        --table "text_bench_sql_${ds}" --sql-hash-buckets 1 \
        --out "$RESULTS_DIR/t6_${ds}_sql.json" \
        --top-k 10
}

# ---------------------------------------------------------------------------
# T7: ES-compatible gateway HTTP path
# ---------------------------------------------------------------------------

run_t7() {
    local dir url index="text_bench_gateway"
    if [[ "$QUICK" == 1 ]]; then
        dir="$(ensure_dataset scifact)"
    else
        dir="$(ensure_dataset msmarco 100000)"
    fi
    local table_work="$RESULTS_DIR/work/t7_gateway_data"
    local config="$RESULTS_DIR/t7_gateway.toml"
    mkdir -p "$table_work"
    cat >"$config" <<EOF
[server]
listen = "127.0.0.1:19200"
version = "8.19.6"

[lakesoul]
namespace = "default"
provision_on_start = true

[defaults]
hash_bucket_num = 4
tokenizer = "jieba"

[[indexes]]
name = "$index"
table = "text_bench_gateway"
path = "file://$table_work"
EOF
    echo "── t7 building the gateway"
    cargo -q build --release -p lakesoul-es-gateway
    LAKESOUL_PG_URL="${LAKESOUL_PG_URL:-}" \
        ./rust/target/release/lakesoul-es-gateway --config "$config" \
        >"$RESULTS_DIR/logs/t7_gateway.log" 2>&1 &
    local gateway_pid=$!
    trap 'kill "$gateway_pid" 2>/dev/null || true' RETURN
    url="http://127.0.0.1:19200"
    for _ in $(seq 1 60); do
        if curl -sf "$url/" >/dev/null 2>&1; then break; fi
        sleep 1
    done
    echo "── t7 gateway bench"
    uv run python script/benchmark/text/gateway_bench.py \
        --gateway-url "$url" --index "$index" \
        --corpus "$dir/corpus.jsonl" --queries "$dir/queries.jsonl" \
        --qrels "$dir/qrels.tsv" \
        --limit "${T7_LIMIT:-20000}" --query-limit 200 \
        --batch-size 500 --top-k 10 --rounds 2 --per-round 2000 \
        --out "$RESULTS_DIR/t7_gateway.json" \
        >"$RESULTS_DIR/logs/t7_gateway_bench.log" 2>&1 || {
        echo "   FAILED: t7 (tail of $RESULTS_DIR/logs/t7_gateway_bench.log)"
        tail -5 "$RESULTS_DIR/logs/t7_gateway_bench.log" | sed 's/^/   /'
    }
    kill "$gateway_pid" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

if [[ -z "${LAKESOUL_PG_URL:-}" ]]; then
    echo "warning: LAKESOUL_PG_URL is not set (the harness needs PostgreSQL)" >&2
fi
echo "results: $RESULTS_DIR"
echo "data:    $DATA_DIR"
echo "threads: $THREADS  quick: $QUICK"
FAILURES=0

for scenario in "${SCENARIOS[@]}"; do
    case "$scenario" in
        quick)
            QUICK=1
            echo "== quick smoke (scifact)"
            run_t2
            run_t1
            run_t3
            run_t4
            run_t5
            run_t6
            ;;
        all)
            run_t2; run_t1; run_t3; run_t4; run_t5; run_t6; run_t7
            ;;
        t1) run_t1 ;;
        t2) run_t2 ;;
        t3) run_t3 ;;
        t4) run_t4 ;;
        t5) run_t5 ;;
        t6) run_t6 ;;
        t7) run_t7 ;;
    esac
done

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "done with $FAILURES failed scenario(s); see $RESULTS_DIR/logs"
else
    echo "done."
fi
echo "plot with:"
echo "  uv run --with matplotlib python script/benchmark/text/plot.py --results $RESULTS_DIR"
exit $((FAILURES > 0 ? 1 : 0))
