#!/bin/bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# One-container LakeSoul: PostgreSQL metadata, one distributed worker and the
# PostgreSQL wire coordinator. The image is built from flake.nix
# (packages.<system>.all-in-one-image).
set -Eeuo pipefail

APP_DIR=${APP_DIR:-/app}
PGDATA=${PGDATA:-/var/lib/postgresql/data}
PGDATA_OWNER=${PGDATA_OWNER:-65534:65534}

POSTGRES_USER=${POSTGRES_USER:-lakesoul}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-lakesoul}
POSTGRES_DB=${POSTGRES_DB:-lakesoul}

LAKESOUL_WAREHOUSE=${LAKESOUL_WAREHOUSE:-file:///warehouse}
LAKESOUL_PG_BIND=${LAKESOUL_PG_BIND:-0.0.0.0}
LAKESOUL_PG_PORT=${LAKESOUL_PG_PORT:-5433}
LAKESOUL_PG_METRICS_ADDR=${LAKESOUL_PG_METRICS_ADDR:-0.0.0.0:19090}
LAKESOUL_WORKER_BIND=${LAKESOUL_WORKER_BIND:-127.0.0.1}
LAKESOUL_WORKER_PORT=${LAKESOUL_WORKER_PORT:-50051}
LAKESOUL_WORKER_METRICS_ADDR=${LAKESOUL_WORKER_METRICS_ADDR:-127.0.0.1:19091}

WORKER_THREADS=${WORKER_THREADS:-4}
WORKER_MEMORY_LIMIT=${WORKER_MEMORY_LIMIT:-4Gi}
WORKER_VORTEX_DECODE_BUDGET=${WORKER_VORTEX_DECODE_BUDGET:-1Gi}
COORDINATOR_THREADS=${COORDINATOR_THREADS:-4}
COORDINATOR_MEMORY_LIMIT=${COORDINATOR_MEMORY_LIMIT:-2Gi}
COORDINATOR_VORTEX_DECODE_BUDGET=${COORDINATOR_VORTEX_DECODE_BUDGET:-512Mi}

export LAKESOUL_WAREHOUSE
export LAKESOUL_PG_BIND LAKESOUL_PG_PORT LAKESOUL_PG_METRICS_ADDR
export LAKESOUL_WORKER_BIND LAKESOUL_WORKER_PORT LAKESOUL_WORKER_METRICS_ADDR
export LAKESOUL_PG_USERNAME=${LAKESOUL_PG_USERNAME:-$POSTGRES_USER}
export LAKESOUL_PG_PASSWORD=${LAKESOUL_PG_PASSWORD:-$POSTGRES_PASSWORD}
export LAKESOUL_PG_URL=${LAKESOUL_PG_URL:-jdbc:postgresql://127.0.0.1:5432/${POSTGRES_DB}?stringtype=unspecified}
# A one-container local deployment is a single trusted node: do not require
# LakeSoul tokens. RBAC requires JWT and stays off as well.
export LAKESOUL_PG_JWT_AUTH=${LAKESOUL_PG_JWT_AUTH:-false}
export LAKESOUL_PG_RBAC=${LAKESOUL_PG_RBAC:-false}
# IVM is always installed; this makes new connections start with it on.
export LAKESOUL_PG_IVM_DEFAULT=${LAKESOUL_PG_IVM_DEFAULT:-true}
# Deferred-index builds run in a child process supervised by the coordinator.
export LAKESOUL_PG_INDEX_MAINTENANCE_EXECUTOR=${LAKESOUL_PG_INDEX_MAINTENANCE_EXECUTOR:-local}
export LAKESOUL_DISTRIBUTED_DISCOVERY=${LAKESOUL_DISTRIBUTED_DISCOVERY:-static}
export LAKESOUL_DISTRIBUTED_WORKERS=${LAKESOUL_DISTRIBUTED_WORKERS:-http://127.0.0.1:${LAKESOUL_WORKER_PORT}}
export LAKESOUL_DISTRIBUTED_TARGET_PARTITIONS=${LAKESOUL_DISTRIBUTED_TARGET_PARTITIONS:-4}
export LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL=${LAKESOUL_DISTRIBUTED_FALLBACK_LOCAL:-true}
export MALLOC_CONF=${MALLOC_CONF:-background_thread:true}
export TMPDIR=${TMPDIR:-/var/scratch}
export LAKESOUL_CACHE_PATH=${LAKESOUL_CACHE_PATH:-/var/scratch/lakesoul-cache}
export TZ=${TZ:-UTC}

run_as() {
    setpriv --reuid="${PGDATA_OWNER%:*}" --regid="${PGDATA_OWNER#*:}" \
        --clear-groups -- "$@"
}

log() {
    printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

PG_PID=
WORKER_PID=
COORDINATOR_PID=

stop_children() {
    local status=$?
    trap - TERM INT EXIT
    if [[ -n "${COORDINATOR_PID}" ]]; then
        kill -TERM "${COORDINATOR_PID}" 2>/dev/null || true
    fi
    if [[ -n "${WORKER_PID}" ]]; then
        kill -TERM "${WORKER_PID}" 2>/dev/null || true
    fi
    if [[ -n "${PG_PID}" ]]; then
        # SIGINT is PostgreSQL's fast shutdown: disconnect clients and exit.
        kill -INT "${PG_PID}" 2>/dev/null || true
    fi
    wait 2>/dev/null || true
    exit "${status}"
}

trap 'exit 130' INT
trap 'exit 143' TERM
trap stop_children EXIT

mkdir -p "${PGDATA}" /var/lib/postgresql /warehouse /var/scratch /var/log/lakesoul
chown -R "${PGDATA_OWNER}" "${PGDATA}" /var/lib/postgresql /warehouse \
    /var/scratch /var/log/lakesoul

if [[ ! -s "${PGDATA}/PG_VERSION" ]]; then
    log "initializing PostgreSQL metadata database in ${PGDATA}"
    run_as initdb -D "${PGDATA}" -U "${POSTGRES_USER}" \
        --auth-local=trust --auth-host=scram-sha-256 \
        --encoding=UTF8 --locale=C >/dev/null
fi

log "starting PostgreSQL on 127.0.0.1:5432"
run_as postgres -D "${PGDATA}" \
    -c listen_addresses=127.0.0.1 \
    -c port=5432 \
    -c unix_socket_directories=/tmp \
    -c max_connections=200 &
PG_PID=$!

for _ in $(seq 1 300); do
    if pg_isready -h /tmp -p 5432 -U "${POSTGRES_USER}" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "${PG_PID}" 2>/dev/null; then
        log "PostgreSQL exited during startup" >&2
        exit 1
    fi
    sleep 0.2
done
pg_isready -h /tmp -p 5432 -U "${POSTGRES_USER}" >/dev/null

# Keep the password in sync with the environment even when the data directory
# predates a changed POSTGRES_PASSWORD.
PGPASSWORD="${POSTGRES_PASSWORD}" psql -h /tmp -p 5432 -U "${POSTGRES_USER}" \
    -d postgres -v ON_ERROR_STOP=1 \
    -c "ALTER USER \"${POSTGRES_USER}\" PASSWORD '${POSTGRES_PASSWORD}'" \
    >/dev/null

if ! PGPASSWORD="${POSTGRES_PASSWORD}" psql -h /tmp -p 5432 -U "${POSTGRES_USER}" \
    -d postgres -tAc "select 1 from pg_database where datname = '${POSTGRES_DB}'" \
    | grep -q 1; then
    log "creating metadata database ${POSTGRES_DB}"
    PGPASSWORD="${POSTGRES_PASSWORD}" createdb -h /tmp -p 5432 \
        -U "${POSTGRES_USER}" "${POSTGRES_DB}"
fi

if ! PGPASSWORD="${POSTGRES_PASSWORD}" psql -h /tmp -p 5432 -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" -tAc "select to_regclass('public.namespace')" \
    | grep -q namespace; then
    log "applying the LakeSoul metadata schema"
    PGPASSWORD="${POSTGRES_PASSWORD}" psql -h /tmp -p 5432 -U "${POSTGRES_USER}" \
        -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1 -f "${APP_DIR}/meta_init.sql" \
        >/dev/null
fi

log "starting LakeSoul worker on ${LAKESOUL_WORKER_BIND}:${LAKESOUL_WORKER_PORT}"
run_as "${APP_DIR}/lakesoul-worker" \
    --worker-threads "${WORKER_THREADS}" \
    --memory-limit "${WORKER_MEMORY_LIMIT}" \
    --vortex-decode-budget "${WORKER_VORTEX_DECODE_BUDGET}" &
WORKER_PID=$!

for _ in $(seq 1 300); do
    if (echo >"/dev/tcp/${LAKESOUL_WORKER_BIND}/${LAKESOUL_WORKER_PORT}") \
        >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
        log "LakeSoul worker exited during startup" >&2
        exit 1
    fi
    sleep 0.2
done
if ! (echo >"/dev/tcp/${LAKESOUL_WORKER_BIND}/${LAKESOUL_WORKER_PORT}") \
    >/dev/null 2>&1; then
    log "LakeSoul worker did not become ready" >&2
    exit 1
fi

log "starting postgres-lakesoul on ${LAKESOUL_PG_BIND}:${LAKESOUL_PG_PORT}"
run_as "${APP_DIR}/postgres-lakesoul" \
    --worker-threads "${COORDINATOR_THREADS}" \
    --memory-limit "${COORDINATOR_MEMORY_LIMIT}" \
    --vortex-decode-budget "${COORDINATOR_VORTEX_DECODE_BUDGET}" &
COORDINATOR_PID=$!

log "LakeSoul is ready: psql 'postgresql://${POSTGRES_USER}@127.0.0.1:${LAKESOUL_PG_PORT}/default'"

# Exit as soon as any process exits; Docker's restart policy then starts a
# clean supervisor instead of leaving a half-alive endpoint.
set +e
wait -n "${PG_PID}" "${WORKER_PID}" "${COORDINATOR_PID}"
STATUS=$?
set -e
log "a LakeSoul process exited (status ${STATUS}); stopping the container"
exit "${STATUS}"
