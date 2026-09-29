#!/usr/bin/env bash
# Demo launcher: postgres-lakesoul with OTLP traces exported to Jaeger.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../.."   # adjust based on how deep the script is nested
cd "$REPO_ROOT"
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
export RUST_LOG=info
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_SERVICE_NAME=lakesoul-pg-demo
exec rust/target/debug/postgres-lakesoul -p 6543 --metrics-addr 0.0.0.0:19090 \
  --worker-threads 8 \
  --warehouse-prefix 's3://lakesoul-test-bucket' \
  --endpoint 'http://localhost:9000' \
  --s3-bucket 'lakesoul-test-bucket' \
  --s3-access-key 'rustfsadmin' \
  --s3-secret-key 'rustfsadmin' \
  >/tmp/lakesoul-pg.log 2>&1
