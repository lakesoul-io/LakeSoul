#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Start a real postgres-lakesoul process and run the psycopg and pgjdbc
# baseline against it. The metadata database and warehouse are disposable;
# set LAKESOUL_PG_* for a local metadata PostgreSQL.
set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$DIR/../../.." && pwd)
BIN=${POSTGRES_LAKESOUL_BIN:-"$ROOT/rust/target/debug/postgres-lakesoul"}

if [[ ! -x "$BIN" ]]; then
  echo "postgres-lakesoul binary not found at $BIN; building it" >&2
  (cd "$ROOT" && cargo -q build -p postgres-lakesoul)
fi

if [[ -z "${CLIENT_COMPAT_PORT:-}" ]]; then
  CLIENT_COMPAT_PORT=$(python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
  )
fi
HOST=127.0.0.1
PORT=$CLIENT_COMPAT_PORT
DATABASE=default
USER=${PGUSER:-lakesoul_test}
PASSWORD=${PGPASSWORD:-lakesoul_test}
WAREHOUSE=$(mktemp -d "${TMPDIR:-/tmp}/lakesoul-client-compat.XXXXXX")
LOG="$WAREHOUSE/postgres-lakesoul.log"
SERVER_PID=""

cleanup() {
  status=$?
  trap - EXIT
  if [[ $status -ne 0 && -f "$LOG" ]]; then
    echo "---- postgres-lakesoul log (last 200 lines) ----" >&2
    tail -200 "$LOG" >&2 || true
    echo "-----------------------------------------------" >&2
  fi
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WAREHOUSE"
  exit "$status"
}
trap cleanup EXIT INT TERM

export TZ=UTC
export LAKESOUL_PG_URL=${LAKESOUL_PG_URL:-"jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified"}
export LAKESOUL_PG_USERNAME=${LAKESOUL_PG_USERNAME:-lakesoul_test}
export LAKESOUL_PG_PASSWORD=${LAKESOUL_PG_PASSWORD:-lakesoul_test}

"$BIN" \
  --bind "$HOST" \
  --port "$PORT" \
  --warehouse-prefix "file://$WAREHOUSE" \
  --discovery none \
  --no-metrics \
  --worker-threads 2 \
  --max-connections 32 \
  --max-queries-per-user 32 \
  --max-result-rows 0 \
  --max-result-bytes 0 \
  --memory-limit 0 \
  --vortex-decode-budget 0 \
  >"$LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 120); do
  if pg_isready -h "$HOST" -p "$PORT" -d "$DATABASE" -U "$USER" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "postgres-lakesoul exited before becoming ready" >&2
    exit 1
  fi
  sleep 1
done
pg_isready -h "$HOST" -p "$PORT" -d "$DATABASE" -U "$USER" >/dev/null

export PGHOST=$HOST
export PGPORT=$PORT
export PGDATABASE=$DATABASE
export PGUSER=$USER
export PGPASSWORD=$PASSWORD
export PGSSLMODE=disable

echo "== psycopg baseline =="
if command -v uv >/dev/null 2>&1; then
  (cd "$DIR" && uv run --with-requirements "$DIR/requirements.txt" -- \
    python -m pytest "$DIR" -q -rxX)
else
  python3 -c 'import pytest, psycopg2, psycopg, sqlalchemy' || {
    echo "install $DIR/requirements.txt or put uv on PATH" >&2
    exit 1
  }
  (cd "$DIR" && python3 -m pytest "$DIR" -q -rxX)
fi

echo "== pgjdbc baseline =="
if ! command -v mvn >/dev/null 2>&1; then
  echo "mvn is required for the pgjdbc baseline" >&2
  exit 1
fi
JDBC_TABLE="client_jdbc_$(python3 - <<'PY'
import uuid
print(uuid.uuid4().hex[:16])
PY
)"
mvn -q -f "$DIR/jdbc/pom.xml" compile exec:java \
  -Dexec.args="$HOST $PORT $DATABASE $USER $PASSWORD $JDBC_TABLE"

echo "client compatibility baseline passed"
