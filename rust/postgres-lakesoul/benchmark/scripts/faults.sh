#!/usr/bin/env bash
# Pod-level fault scenarios against a deployed run directory (namespace lakesoul).
#
#   scripts/faults.sh RUN_DIR BENCH_TAG SCENARIO...
#
# cancel            CancelRequest 2 s into q01 (pk) and into q01 (nopk, distributed);
#                   records cancel->error/ready latency and session reuse.
# stmt-timeout      server-side `SET statement_timeout = 2000` before q01.
# disconnect        FIN and RST 2 s into q01 (pk and nopk), then 40 s of
#                   resource samples to see whether execution stops.
# connflood         ramp to 500 and 2000 idle connections (100/s), hold 30 s.
# maxrows           extended-protocol Execute.max_rows = -1 and 2147483647.
# kill-coordinator  delete the coordinator pod 30 s into a 150 s closed-loop load.
# kill-worker       delete one worker pod 30 s into a 150 s distributed (nopk) load.
# Env: Q01 (path of q01.sql, default workloads/tpch/queries/q01.sql).
set -euo pipefail
cd "$(dirname "$0")/.."
run=$1 bench=$2
shift 2
ctx=(--context "${KUBE_CONTEXT:-external}" -n lakesoul)
# Drop `--` comment lines first: joined into one line they would comment out
# the whole statement (an empty query that "completes" in 20 ms).
q01=$(grep -v '^[[:space:]]*--' "${Q01:-workloads/tpch/queries/q01.sql}" | tr '\n' ' ' | sed 's/;[[:space:]]*$//')
mixes=/bench/workloads/mixes

drive() { # NAME TIMEOUT DRIVER_ARGS...
    local name=$1 timeout=$2
    shift 2
    python3 scripts/bench.py drive --run-dir "$run" --bench-tag "$bench" --name "$name" \
        --timeout "$timeout" -- "$@" || true
}

settle() { # NAME SECONDS: resource samples for SECONDS after now
    local name=$1 s=$2 since
    since=$(date +%s)
    sleep "$s"
    python3 scripts/bench.py collect --run-dir "$run" --name "$name" --since "$since" --until "$(date +%s)" || true
}

pod_of() { kubectl "${ctx[@]}" get pods -l "app.kubernetes.io/component=$1" -o jsonpath='{.items[0].metadata.name}'; }

fault_under_load() { # NAME COMPONENT DB MIX
    local name=$1 component=$2 db=$3 mix=$4 victim
    drive "$name" 900 load --host lakesoul-coordinator --database "$db" --sf 1 --mix "$mix" \
        --mode closed --concurrency 4 --duration 150 --query-timeout 120 --no-stop-on-outage &
    local pid=$!
    sleep 45 # Job scheduling + 30 s of steady load
    victim=$(pod_of "$component")
    printf '{"fault":"delete pod","pod":"%s","unix":%s}\n' "$victim" "$(date +%s.%N)" >"$run/$name.fault.json"
    kubectl "${ctx[@]}" delete pod "$victim" --wait=false
    wait "$pid"
}

for s in "$@"; do
    case "$s" in
    cancel)
        for db in tpch_sf1 tpch_sf1_nopk; do
            drive "cancel-$db" 600 cancel --host lakesoul-coordinator --database "$db" --sql "$q01" --after 2
        done
        ;;
    stmt-timeout)
        drive stmt-timeout 600 cancel --host lakesoul-coordinator --database tpch_sf1 \
            --sql "SET statement_timeout = 2000; $q01" --after 120
        ;;
    disconnect)
        for db in tpch_sf1 tpch_sf1_nopk; do
            drive "disconnect-fin-$db" 600 disconnect --host lakesoul-coordinator --database "$db" --sql "$q01" --after 2
            settle "disconnect-fin-$db-after" 40
            drive "disconnect-rst-$db" 600 disconnect --host lakesoul-coordinator --database "$db" --sql "$q01" --after 2 --rst
            settle "disconnect-rst-$db-after" 40
        done
        ;;
    connflood)
        for n in 500 2000; do
            drive "connflood-$n" 900 connflood --host lakesoul-coordinator --database tpch_sf1 \
                --connections "$n" --rate 100 --hold 30
        done
        ;;
    maxrows)
        for m in -1 2147483647; do
            drive "maxrows-$m" 600 slowclient --host lakesoul-coordinator --database tpch_sf1 \
                --sql "SELECT 1" --protocol extended --max-rows "$m" --read-rate max --query-timeout 60
            settle "maxrows-$m-after" 30
        done
        ;;
    kill-coordinator)
        fault_under_load kill-coordinator coordinator tpch_sf1 "$mixes/mixed.json"
        ;;
    kill-worker)
        fault_under_load kill-worker worker tpch_sf1_nopk "$mixes/mixed.json"
        ;;
    *)
        echo "unknown scenario $s" >&2
        exit 2
        ;;
    esac
done
