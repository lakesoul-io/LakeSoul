#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Start a real postgres-lakesoul process and run the psycopg, pgjdbc and
# pgvector baselines against it. lakesoul-console creates the vector-index
# fixture table. The metadata database and warehouse are disposable; set
# LAKESOUL_PG_* for a local metadata PostgreSQL.
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

# A self-signed certificate is enough for sslmode=require, which encrypts
# without validating the chain. The key must be PKCS#8: the server rejects
# anything else at start-up instead of silently serving plaintext.
TLS_DIR="$WAREHOUSE/tls"
mkdir -p "$TLS_DIR"
TLS_CERT="$TLS_DIR/server.crt"
TLS_KEY="$TLS_DIR/server.key"
if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate the TLS test certificate" >&2
  exit 1
fi
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
  -out "$TLS_KEY" >/dev/null 2>&1
openssl req -new -x509 -key "$TLS_KEY" -out "$TLS_CERT" -days 2 \
  -subj "/CN=localhost" >/dev/null 2>&1

# The baseline is the deployment shape this feature is for: TLS is required,
# so every driver must negotiate it. pg_isready and psql read the same env.
export PGSSLMODE=require

# pgvector columns are only reported as the `vector` wire type when the table
# declares a vector index column. PostgreSQL CREATE TABLE cannot do that here,
# so create the fixture the same way the Python SDK and DataFusion do: through
# lakesoul-console, against the same metadata database and warehouse.
CONSOLE_BIN=${POSTGRES_LAKESOUL_CONSOLE_BIN:-"$ROOT/rust/target/test-fast/lakesoul-console"}
if [[ ! -x "$CONSOLE_BIN" ]]; then
  CONSOLE_BIN="$ROOT/rust/target/debug/lakesoul-console"
fi
if [[ ! -x "$CONSOLE_BIN" ]]; then
  echo "lakesoul-console binary not found; build it with" >&2
  echo "  cargo build --profile test-fast -p lakesoul-console" >&2
  exit 1
fi
export CLIENT_COMPAT_VECTOR_TABLE=client_compat_vector
cat > "$WAREHOUSE/client_vector.sql" <<SQL
CREATE EXTERNAL TABLE $CLIENT_COMPAT_VECTOR_TABLE (
    id BIGINT NOT NULL PRIMARY KEY,
    vec FLOAT[] NOT NULL
) STORED AS LAKESOUL
LOCATION 'file://$WAREHOUSE/$CLIENT_COMPAT_VECTOR_TABLE'
OPTIONS (
    'vector_index_columns' '[{"column":"vec","dim":3,"nlist":4,"total_bits":7,"metric":"L2"}]',
    'hashBucketNum' '4'
);
INSERT INTO $CLIENT_COMPAT_VECTOR_TABLE VALUES (1, ARRAY[1.0, 2.0, 3.0]), (2, ARRAY[4.0, 5.0, 6.0]);
SQL
echo "== create pgvector fixture =="
if ! "$CONSOLE_BIN" \
  --warehouse-prefix "file://$WAREHOUSE" \
  --log-dir "$WAREHOUSE" \
  -f "$WAREHOUSE/client_vector.sql" >"$WAREHOUSE/console.log" 2>&1; then
  echo "failed to create the pgvector fixture table" >&2
  tail -100 "$WAREHOUSE/console.log" >&2 || true
  exit 1
fi

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
  --tls-cert "$TLS_CERT" \
  --tls-key "$TLS_KEY" \
  --require-tls \
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
# PGSSLMODE=require was exported before readiness and applies to psql too.

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
JDBC_ARGS="$HOST $PORT $DATABASE $USER $PASSWORD $JDBC_TABLE"
mvn -q -f "$DIR/jdbc/pom.xml" compile exec:java \
  -Dexec.mainClass=io.lakesoul.compat.JdbcSmoke \
  -Dexec.args="$JDBC_ARGS require"

echo "== pgjdbc semantic types and pgvector baseline =="
mvn -q -f "$DIR/jdbc/pom.xml" exec:java \
  -Dexec.mainClass=io.lakesoul.compat.JdbcTypeSmoke \
  -Dexec.args="$JDBC_ARGS require"
mvn -q -f "$DIR/jdbc/pom.xml" exec:java \
  -Dexec.mainClass=io.lakesoul.compat.JdbcVectorSmoke \
  -Dexec.args="$JDBC_ARGS require"

echo "client compatibility baseline passed"
