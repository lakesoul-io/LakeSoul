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

log "deploy cluster"
# compaction must trigger after one extra version so the test does not have to write ten rounds
sed -i 's/if NEW.version - rs_version >= 10 then/if NEW.version - rs_version >= 1 then/' "$ROOT/script/meta_init.sql"
sed -i 's/if NEW.version >= 10 then/if NEW.version >= 1 then/' "$ROOT/script/meta_init.sql"
(cd "$COMPOSE_DIR" && docker compose --profile s3 up -d)

log "enable logical replication"
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
  --dataExpiredTime 10000 --ontimer_interval 0

log "start Spark compaction task"
(
  cd "$WORK_DIR"
  nohup docker run --cpus 2 -m 5000m --net lakesoul-docker-compose-env_default --rm -t \
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
    --conf spark.sql.warehouse.dir=s3://lakesoul-test-bucket/ \
    --conf spark.dmetasoul.lakesoul.native.io.enable=true \
    --conf spark.dmetasoul.lakesoul.compaction.level.file.number.limit=5 \
    --conf spark.dmetasoul.lakesoul.compaction.level.file.merge.num.limit=2 \
    --class com.dmetasoul.lakesoul.spark.compaction.NewCompactionTask \
    --master local[4] "/opt/spark/work-dir/$spark_jar_name" \
    --threadpool.size=10 --database="" --file_num_limit=5 --file_size_limit=10KB \
    > compaction.log 2>&1 &
)

log "run data rounds and assertions"
LAKESOUL_PG_URL="${LAKESOUL_PG_URL:-jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified}" \
LAKESOUL_PG_USERNAME="${LAKESOUL_PG_USERNAME:-lakesoul_test}" \
LAKESOUL_PG_PASSWORD="${LAKESOUL_PG_PASSWORD:-lakesoul_test}" \
RUSTFS_ENDPOINT="${RUSTFS_ENDPOINT:-http://127.0.0.1:9000}" \
python3 "$ROOT/script/ci/compaction_clean_e2e.py"

log "done"
if [[ "$DOWN" == "1" ]]; then
  (cd "$COMPOSE_DIR" && docker compose --profile s3 down -v)
else
  echo "environment left running (docker compose --profile s3 down -v to stop)"
fi
