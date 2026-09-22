#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# End-to-end compaction + clean test.
#
# Starts the docker compose environment (PostgreSQL with logical replication,
# RustFS, Flink cluster), submits the Flink clean job and the Spark
# NewCompactionTask in the background, then drives several data rounds and
# checks the normal, tagged and blob scenarios via script/ci/compaction_clean_e2e.py.
#
# Usage:
#   script/ci/compaction_clean_e2e.sh          # leave the environment running
#   script/ci/compaction_clean_e2e.sh --down   # stop the environment afterwards
#
# Expects the module jars either in the module target directories or already
# copied into script/benchmark/work-dir (see .github/workflows/compaction-clean-e2e.yml).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$ROOT/script/benchmark/work-dir"
COMPOSE_DIR="$ROOT/docker/lakesoul-docker-compose-env"
JOBMANAGER="lakesoul-docker-compose-env-jobmanager-1"
PG_CONTAINER="lakesoul-test-pg"
PG_SERVICE="lakesoul-meta-db"
CLEAN_JOB_SLOT="clean_job_slot"
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
  if [[ -x "$ROOT/python/.venv/bin/python" ]]; then
    PYTHON="$ROOT/python/.venv/bin/python"
  else
    PYTHON="python3"
  fi
fi

DOWN=0
[[ "${1:-}" == "--down" ]] && DOWN=1

log() { printf '\n=== %s ===\n' "$*"; }

flink_jar_name="${FLINK_JAR_NAME:-}"
spark_jar_name="${SPARK_JAR_NAME:-}"
if [[ -z "$flink_jar_name" ]]; then
  flink_jar_name="$(basename "$(ls "$ROOT"/lakesoul-flink/target/lakesoul-flink-*.jar 2>/dev/null | grep -v tests | head -1)")"
fi
if [[ -z "$spark_jar_name" ]]; then
  spark_jar_name="$(basename "$(ls "$ROOT"/lakesoul-spark/target/lakesoul-spark-*.jar 2>/dev/null | grep -v tests | head -1)")"
fi
[[ -n "$flink_jar_name" && -f "$WORK_DIR/$flink_jar_name" ]] ||
  { echo "flink jar missing in $WORK_DIR (set FLINK_JAR_NAME)"; exit 1; }
[[ -n "$spark_jar_name" && -f "$WORK_DIR/$spark_jar_name" ]] ||
  { echo "spark jar missing in $WORK_DIR (set SPARK_JAR_NAME)"; exit 1; }

META_INIT_BACKUP="$(mktemp /tmp/lakesoul-meta-init-XXXXXX.sql)"
cp "$ROOT/script/meta_init.sql" "$META_INIT_BACKUP"
cleanup() {
  docker rm -f lakesoul-e2e-compaction >/dev/null 2>&1 || true
  cp "$META_INIT_BACKUP" "$ROOT/script/meta_init.sql"
}
trap cleanup EXIT

log "deploy cluster"
# compaction must trigger after one extra version so the test does not have to write ten rounds
sed -i 's/if NEW.version - rs_version >= 10 then/if NEW.version - rs_version >= 1 then/' "$ROOT/script/meta_init.sql"
sed -i 's/if NEW.version >= 10 then/if NEW.version >= 1 then/' "$ROOT/script/meta_init.sql"
PROXY_OVERRIDE="$(mktemp /tmp/lakesoul-e2e-compose-XXXXXX.yml)"
cat > "$PROXY_OVERRIDE" <<'YAML'
services:
  jobmanager:
    environment:
      HTTP_PROXY: ""
      HTTPS_PROXY: ""
      http_proxy: ""
      https_proxy: ""
      NO_PROXY: "rustfs,localhost,127.0.0.1"
      no_proxy: "rustfs,localhost,127.0.0.1"
  taskmanager:
    environment:
      HTTP_PROXY: ""
      HTTPS_PROXY: ""
      http_proxy: ""
      https_proxy: ""
      NO_PROXY: "rustfs,localhost,127.0.0.1"
      no_proxy: "rustfs,localhost,127.0.0.1"
YAML
(cd "$COMPOSE_DIR" && docker compose -f docker-compose.yml -f "$PROXY_OVERRIDE" --profile s3 up -d)

log "ensure object store bucket"
# the bundled rc client fails against rustfs 1.0.0-beta.3, create the bucket directly
uv run --quiet --no-project --with boto3 python - "${LAKESOUL_E2E_BUCKET:-lakesoul-test-bucket}" <<'PYBUCKET' || true
import sys
import boto3

bucket = sys.argv[1]
client = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="rustfsadmin",
    aws_secret_access_key="rustfsadmin",
    region_name="us-east-1",
)
try:
    client.create_bucket(Bucket=bucket)
    print(f"created bucket {bucket}")
except Exception as exc:
    print(f"create bucket {bucket}: {exc}")
print("buckets:", [item["Name"] for item in client.list_buckets()["Buckets"]])
PYBUCKET

log "enable logical replication"
for _ in $(seq 1 60); do
  docker exec "$PG_CONTAINER" psql -U lakesoul_test -d lakesoul_test -c "select 1" >/dev/null 2>&1 && break
  sleep 2
done
docker exec "$PG_CONTAINER" psql -U lakesoul_test -d postgres -c "alter system set wal_level=logical" >/dev/null
docker restart "$PG_CONTAINER" >/dev/null
for _ in $(seq 1 60); do
  docker exec "$PG_CONTAINER" psql -U lakesoul_test -d lakesoul_test -c "select 1" >/dev/null 2>&1 && break
  sleep 2
done
wal_level="$(docker exec "$PG_CONTAINER" psql -U lakesoul_test -d lakesoul_test -tAc "show wal_level")"
[[ "$wal_level" == "logical" ]] || { echo "wal_level is $wal_level"; exit 1; }

for _ in $(seq 1 60); do
  docker exec "$JOBMANAGER" flink list >/dev/null 2>&1 && break
  sleep 2
done

log "submit clean job"
docker exec -t "$JOBMANAGER" flink run -d \
  -c org.apache.flink.lakesoul.entry.clean.NewCleanJob \
  "/opt/flink/work-dir/$flink_jar_name" \
  --source_db.host "$PG_SERVICE" --source_db.port 5432 --source_db.dbName lakesoul_test \
  --source_db.user lakesoul_test --source_db.password lakesoul_test \
  --source.parallelism 1 --slotName "$CLEAN_JOB_SLOT" --plugName pgoutput \
  --schemaList public --splitSize 1 \
  --url "jdbc:postgresql://$PG_SERVICE:5432/lakesoul_test" \
  --dataExpiredTime 0 --ontimer_interval 1

log "start Spark compaction task"
(
  cd "$WORK_DIR"
  docker rm -f lakesoul-e2e-compaction >/dev/null 2>&1 || true
  nohup docker run --name lakesoul-e2e-compaction --cpus 2 -m 5000m --net lakesoul-docker-compose-env_default --rm -t \
    --env HTTP_PROXY= --env HTTPS_PROXY= --env http_proxy= --env https_proxy= \
    --env NO_PROXY="rustfs,localhost,127.0.0.1" --env no_proxy="rustfs,localhost,127.0.0.1" \
    -v "${PWD}:/opt/spark/work-dir" \
    --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties \
    --env LAKESOUL_IO_USE_V2_MERGE=true \
    swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/spark:3.5.8-py310-hadoop334 \
    spark-submit --driver-memory 2G --executor-memory 2G \
    --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m \
    --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.endpoint=http://rustfs:9000 \
    --conf spark.hadoop.fs.s3a.access.key=rustfsadmin \
    --conf spark.hadoop.fs.s3a.secret.key=rustfsadmin \
    --conf spark.hadoop.fs.s3a.proxy.host= --conf spark.hadoop.fs.s3a.proxy.port=-1 \
    --conf spark.sql.warehouse.dir=s3://lakesoul-test-bucket/ \
    --conf spark.dmetasoul.lakesoul.native.io.enable=true \
    --conf spark.dmetasoul.lakesoul.compaction.level.file.number.limit=2 \
    --conf spark.dmetasoul.lakesoul.compaction.level.file.merge.num.limit=2 \
    --class com.dmetasoul.lakesoul.spark.compaction.NewCompactionTask \
    --master local[4] "/opt/spark/work-dir/$spark_jar_name" \
    --threadpool.size 10 --database "" --file_num_limit 2 --file_size_limit 10KB --new_compact_percentage 100 \
    > compaction.log 2>&1 &
)

log "run data rounds and assertions"
LAKESOUL_PG_URL="${LAKESOUL_PG_URL:-jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified}" \
LAKESOUL_PG_USERNAME="${LAKESOUL_PG_USERNAME:-lakesoul_test}" \
LAKESOUL_PG_PASSWORD="${LAKESOUL_PG_PASSWORD:-lakesoul_test}" \
RUSTFS_ENDPOINT="${RUSTFS_ENDPOINT:-http://127.0.0.1:9000}" \
"$PYTHON" "$ROOT/script/ci/compaction_clean_e2e.py"

log "done"
if [[ "$DOWN" == "1" ]]; then
  (cd "$COMPOSE_DIR" && docker compose --profile s3 down -v)
else
  echo "environment left running (docker compose --profile s3 down -v to stop)"
fi
