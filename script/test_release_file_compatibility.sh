#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${LEGACY_SPARK_PYTHON:?Set LEGACY_SPARK_PYTHON to a Python 3.10 environment with pyspark 3.3.1}"
: "${LEGACY_SPARK_JAR:?Set LEGACY_SPARK_JAR to lakesoul-spark-3.3-3.0.0.jar}"
: "${CURRENT_PYTHON:?Set CURRENT_PYTHON to the current LakeSoul Python environment}"

legacy_spark_sha256="429a1856d47c5c912a5f793072aa7ecfa5631cfcb1d6bea24bbe698fcc267c60"
baseline_tag="${LAKESOUL_MIGRATION_BASELINE:-v3.0.0}"
host="${PGHOST:-127.0.0.1}"
port="${PGPORT:-5432}"
user="${PGUSER:-lakesoul_test}"
database="${LAKESOUL_LEGACY_DATABASE:-lakesoul_release_legacy}"
output_dir="${LAKESOUL_COMPAT_OUTPUT_DIR:-compatibility-artifacts/legacy-recovery}"
storage_dir="${LAKESOUL_COMPAT_STORAGE_DIR:-/tmp/lakesoul-release-legacy/storage}"
repo_root="$(pwd)"
output_dir="$(mkdir -p "$output_dir" && cd "$output_dir" && pwd)"
mkdir -p "$storage_dir"
mkdir -p "$output_dir/java-legacy" "$output_dir/java-current"
storage_uri="file://$storage_dir"
connection=(-h "$host" -p "$port" -U "$user")
database_url="jdbc:postgresql://$host:$port/$database?stringtype=unspecified"
psql_url="postgresql://$user@$host:$port/$database"

cleanup() {
  dropdb "${connection[@]}" --if-exists "$database" >/dev/null 2>&1 || true
}
trap cleanup EXIT

export LAKESOUL_PG_URL="$database_url"
export LAKESOUL_PG_USERNAME="$user"

rm -rf "$storage_dir"
mkdir -p "$storage_dir"
dropdb "${connection[@]}" --if-exists "$database"
createdb "${connection[@]}" "$database"
git show "$baseline_tag:script/meta_init.sql" > "$output_dir/v3-meta-init.sql"
psql "${connection[@]}" -v ON_ERROR_STOP=1 -d "$database" \
  -f "$output_dir/v3-meta-init.sql" >/dev/null

legacy_java_options="-Djava.io.tmpdir=$output_dir/java-legacy"

JAVA_TOOL_OPTIONS="$legacy_java_options" \
  PYSPARK_PYTHON="$LEGACY_SPARK_PYTHON" \
  PYSPARK_DRIVER_PYTHON="$LEGACY_SPARK_PYTHON" \
  "$LEGACY_SPARK_PYTHON" "$repo_root/python/tests/compat/legacy_fixture.py" spark-3.0 \
  --storage "$storage_uri" \
  --spark-jar "$LEGACY_SPARK_JAR" \
  --spark-jar-sha256 "$legacy_spark_sha256" \
  --output "$output_dir/legacy-manifest.json"

# The legacy writer has exited. Capture PostgreSQL and table data from this
# single quiesced point and bind both artifacts to one checksummed backup set.
pg_dump "${connection[@]}" --format=custom \
  --file="$output_dir/pre-upgrade-metadata.dump" "$database"
tar -C "$(dirname "$storage_dir")" -cf "$output_dir/pre-upgrade-table-data.tar" \
  "$(basename "$storage_dir")"
"$CURRENT_PYTHON" python/tests/compat/legacy_fixture.py backup-manifest \
  --metadata "$output_dir/pre-upgrade-metadata.dump" \
  --table-data "$output_dir/pre-upgrade-table-data.tar" \
  --output "$output_dir/backup-set.json"

export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$output_dir/java-current"
export PYSPARK_PYTHON="$CURRENT_PYTHON"
export PYSPARK_DRIVER_PYTHON="$CURRENT_PYTHON"

python script/metadata_migrate.py migrate --database-url "$psql_url"
PYTHONPATH=python/tests:python/src "$CURRENT_PYTHON" -m compat.run_matrix \
  --read-manifest "$output_dir/legacy-manifest.json" \
  --readers pyarrow,datafusion,spark \
  --output-dir "$output_dir/current-reads"

PYTHONPATH=python/tests:python/src "$CURRENT_PYTHON" \
  python/tests/compat/legacy_fixture.py upgrade-parquet \
  --manifest "$output_dir/legacy-manifest.json" \
  --output "$output_dir/upgrade-manifest.json"
PYTHONPATH=python/tests:python/src "$CURRENT_PYTHON" -m compat.run_matrix \
  --read-manifest "$output_dir/upgrade-manifest.json" \
  --readers pyarrow,datafusion,spark \
  --output-dir "$output_dir/upgrade-reads"

PYTHONPATH=python/tests:python/src "$CURRENT_PYTHON" -m compat.run_matrix \
  --mode full --writers spark --readers pyarrow,datafusion,spark \
  --cases range_overwrite --force-physical-format parquet \
  --storage "$storage_uri" --run-id release_parquet_window \
  --output-dir "$output_dir/parquet-window-overwrite"

# Destroy both working copies, verify the immutable backup pair, then restore it.
dropdb "${connection[@]}" "$database"
rm -rf "$storage_dir"
"$CURRENT_PYTHON" python/tests/compat/legacy_fixture.py verify-backup \
  --manifest "$output_dir/backup-set.json"
createdb "${connection[@]}" "$database"
pg_restore "${connection[@]}" --exit-on-error --dbname="$database" \
  "$output_dir/pre-upgrade-metadata.dump"
tar -C "$(dirname "$storage_dir")" -xf "$output_dir/pre-upgrade-table-data.tar"

JAVA_TOOL_OPTIONS="$legacy_java_options" \
  PYSPARK_PYTHON="$LEGACY_SPARK_PYTHON" \
  PYSPARK_DRIVER_PYTHON="$LEGACY_SPARK_PYTHON" \
  "$LEGACY_SPARK_PYTHON" "$repo_root/python/tests/compat/legacy_fixture.py" verify-spark-3.0 \
  --state "$output_dir/legacy-manifest.json" \
  --spark-jar "$LEGACY_SPARK_JAR" \
  --spark-jar-sha256 "$legacy_spark_sha256"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$output_dir/java-current"
export PYSPARK_PYTHON="$CURRENT_PYTHON"
export PYSPARK_DRIVER_PYTHON="$CURRENT_PYTHON"
python script/metadata_migrate.py migrate --database-url "$psql_url"
PYTHONPATH=python/tests:python/src "$CURRENT_PYTHON" -m compat.run_matrix \
  --read-manifest "$output_dir/legacy-manifest.json" \
  --readers pyarrow,datafusion,spark \
  --output-dir "$output_dir/restored-reads"

printf 'Legacy Parquet compatibility and backup-set recovery passed: %s\n' \
