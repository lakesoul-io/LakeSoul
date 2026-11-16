#!/bin/sh
# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Resource sampler sidecar. The pod sets `shareProcessNamespace: true`, so the
# LakeSoul process is visible here; its cgroup (v1 or v2) is read through
# /proc/<pid>/root, i.e. from the target container's own mount namespace.
# One JSON object per line on stdout (collected with `kubectl logs`).
#
#   SAMPLER_TARGET    process name to watch (postgres-lakesoul | lakesoul-worker)
#   SAMPLER_INTERVAL  seconds between samples (default 1)
set -u
TARGET="${SAMPLER_TARGET:?}"
INTERVAL="${SAMPLER_INTERVAL:-1}"
# The kernel truncates comm to 15 bytes (`postgres-lakeso`).
COMM=$(printf '%.15s' "$TARGET")

find_pid() {
    for d in /proc/[0-9]*; do
        [ "$(cat "$d/comm" 2>/dev/null)" = "$COMM" ] && {
            echo "${d#/proc/}"
            return 0
        }
    done
    return 1
}

# Prints `key value` pairs of a flat stat file, prefixed.
kv() { [ -r "$2" ] && awk -v p="$1" 'NF == 2 { printf "\"%s%s\":%s,", p, $1, $2 }' "$2"; }
one() { [ -r "$2" ] && printf '"%s":%s,' "$1" "$(cat "$2")"; }

pid=""
while :; do
    if [ -z "$pid" ] || [ ! -d "/proc/$pid" ]; then
        pid=$(find_pid) || pid=""
        if [ -z "$pid" ]; then
            printf '{"ts":%s,"target":"%s","pid":null}\n' "$(date +%s)" "$TARGET"
            sleep "$INTERVAL"
            continue
        fi
    fi
    cg="/proc/$pid/root/sys/fs/cgroup"
    {
        printf '{"ts":%s,"target":"%s","pid":%s,' "$(date +%s)" "$TARGET" "$pid"
        awk '/^(VmRSS|VmHWM|VmSize|RssAnon|RssFile|Threads):/ { v = $2; printf "\"%s\":%s,", $1 == "Threads:" ? "threads" : tolower(substr($1, 1, length($1) - 1)) "_kb", v }' "/proc/$pid/status" 2>/dev/null
        awk '{ printf "\"minflt\":%s,\"majflt\":%s,\"utime_ticks\":%s,\"stime_ticks\":%s,", $10, $12, $14, $15 }' "/proc/$pid/stat" 2>/dev/null
        awk -F': ' '/^(read_bytes|write_bytes|rchar|wchar):/ { printf "\"io_%s\":%s,", $1, $2 }' "/proc/$pid/io" 2>/dev/null
        printf '"fds":%s,' "$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)"
        if [ -r "$cg/memory.current" ]; then
            printf '"cgroup":2,'
            one mem_current "$cg/memory.current"
            one mem_peak "$cg/memory.peak"
            [ -r "$cg/memory.max" ] && printf '"mem_max":"%s",' "$(cat "$cg/memory.max")"
            kv memstat_ "$cg/memory.stat"
            kv memevents_ "$cg/memory.events"
            kv cpustat_ "$cg/cpu.stat"
        else
            m="$cg/memory"
            printf '"cgroup":1,'
            one mem_current "$m/memory.usage_in_bytes"
            one mem_peak "$m/memory.max_usage_in_bytes"
            one mem_max "$m/memory.limit_in_bytes"
            one mem_failcnt "$m/memory.failcnt"
            kv memstat_ "$m/memory.stat"
            kv oom_ "$m/memory.oom_control"
            c="$cg/cpu,cpuacct"
            [ -d "$c" ] || c="$cg/cpu"
            kv cpustat_ "$c/cpu.stat"
            one cpuacct_usage_ns "$c/cpuacct.usage"
            one cfs_quota_us "$c/cpu.cfs_quota_us"
            one cfs_period_us "$c/cpu.cfs_period_us"
        fi
        printf '"ok":true}\n'
    } 2>/dev/null
    sleep "$INTERVAL"
done
