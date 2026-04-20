#!/usr/bin/env bash
set -euo pipefail

PRESTO_CONTAINER="${PRESTO_CONTAINER:-presto}"
WORKER_CONTAINER="${WORKER_CONTAINER:-presto-worker}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))

echo "Waiting for Presto worker to become active..."
echo "PRESTO_CONTAINER=${PRESTO_CONTAINER}"
echo "WORKER_CONTAINER=${WORKER_CONTAINER}"
echo "TIMEOUT_SECONDS=${TIMEOUT_SECONDS}"

while [ "$(date +%s)" -lt "${deadline}" ]; do
  echo "==== /v1/node ===="
  out="$(docker exec "${PRESTO_CONTAINER}" bash -lc 'curl -s http://localhost:8080/v1/node' || true)"
  echo "${out}"

  if python3 - <<'PY' "${out}"
import json, sys

raw = sys.argv[1].strip()
if not raw:
    sys.exit(1)

try:
    data = json.loads(raw)
except Exception:
    sys.exit(1)

active = 0
for node in data:
    if node.get("recentSuccesses", 0) > 0 and node.get("recentFailureRatio", 1) < 1:
        active += 1

print(f"active_workers={active}")
sys.exit(0 if active >= 1 else 1)
PY
  then
    echo "Presto worker is active"
    exit 0
  fi

  sleep "${SLEEP_SECONDS}"
done

echo "No active workers found within timeout"

echo "==== coordinator /v1/node ===="
docker exec "${PRESTO_CONTAINER}" bash -lc 'curl -s http://localhost:8080/v1/node || true' || true

echo "==== coordinator config ===="
docker exec "${PRESTO_CONTAINER}" bash -lc 'cat /opt/presto-server/etc/config.properties || true; echo; cat /opt/presto-server/etc/node.properties || true' || true

echo "==== worker config ===="
docker exec "${WORKER_CONTAINER}" bash -lc 'cat /opt/presto-server/etc/config.properties || true; echo; cat /opt/presto-server/etc/node.properties || true' || true

echo "==== worker -> coordinator ===="
docker exec "${WORKER_CONTAINER}" bash -lc 'curl -sv http://presto-discovery:8080/v1/info || true' || true

echo "==== worker logs ===="
docker logs "${WORKER_CONTAINER}" --tail=300 || true

echo "==== coordinator logs ===="
docker logs "${PRESTO_CONTAINER}" --tail=300 || true

exit 1
