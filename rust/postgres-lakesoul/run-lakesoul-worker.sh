#!/usr/bin/env bash
# Demo launcher for one LakeSoul distributed worker with explicit identity.
#   usage: run-lakesoul-worker.sh <1|2>
#   worker-1 -> grpc :50051, metrics :19091, service.instance.id=worker-1
#   worker-2 -> grpc :50052, metrics :19092, service.instance.id=worker-2
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/../.."   # adjust based on how deep the script is nested
cd "$REPO_ROOT"


N="$1"
PORT=$((50050 + N))
METRICS_PORT=$((19090 + N))

export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_SERVICE_NAME=lakesoul-worker
export OTEL_RESOURCE_ATTRIBUTES="service.instance.id=worker-${N}"
export RUST_LOG=info

exec rust/target/debug/lakesoul-worker \
  --bind 127.0.0.1 \
  --port "$PORT" \
  --metrics-addr "127.0.0.1:${METRICS_PORT}" \
  --worker-threads 4 \
  --warehouse-prefix 's3://lakesoul-test-bucket' \
  --endpoint 'http://localhost:9000' \
  --s3-bucket 'lakesoul-test-bucket' \
  --s3-access-key 'rustfsadmin' \
  --s3-secret-key 'rustfsadmin' \
  >"/tmp/lakesoul-worker-${N}.log" 2>&1
