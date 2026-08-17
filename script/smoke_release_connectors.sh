#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
    echo "Usage: $0 <spark|flink|presto> <artifact-directory> <core-version>" >&2
    exit 2
}

[[ $# -eq 3 ]] || usage
engine=$1
artifact_dir=$(realpath "$2")
version=$3
work_dir=$(mktemp -d "/tmp/lakesoul-${engine}-install-smoke.XXXXXX")
presto_pid=
cleanup() {
    if [[ -n "$presto_pid" ]]; then
        kill "$presto_pid" 2>/dev/null || true
        wait "$presto_pid" 2>/dev/null || true
    fi
    rm -rf "$work_dir"
}
trap cleanup EXIT

case "$engine" in
    spark)
        jar="$artifact_dir/lakesoul-spark-3.5_2.12-${version}.jar"
        test -f "$jar"
        curl --fail --location --retry 3 \
            --output "$work_dir/spark.tgz" \
            "https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz"
        tar -C "$work_dir" -xzf "$work_dir/spark.tgz"
        cat > "$work_dir/smoke.scala" <<'SCALA'
Class.forName("com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
println("LakeSoul Spark connector loaded")
System.exit(0)
SCALA
        SPARK_LOCAL_IP=127.0.0.1 \
            "$work_dir/spark-3.5.8-bin-hadoop3/bin/spark-shell" \
            --master 'local[1]' --jars "$jar" -i "$work_dir/smoke.scala"
        ;;
    flink)
        jar="$artifact_dir/lakesoul-flink-1.20_2.12-${version}.jar"
        test -f "$jar"
        curl --fail --location --retry 3 \
            --output "$work_dir/flink.tgz" \
            "https://dlcdn.apache.org/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz"
        tar -C "$work_dir" -xzf "$work_dir/flink.tgz"
        cat > "$work_dir/smoke.sql" <<SQL
SET 'execution.runtime-mode' = 'batch';
CREATE TEMPORARY TABLE lakesoul_install_smoke (
  id BIGINT
) WITH (
  'connector' = 'lakesoul',
  'path' = 'file://${work_dir}/table',
  'format' = 'parquet'
);
EXPLAIN SELECT * FROM lakesoul_install_smoke;
SQL
        "$work_dir/flink-1.20.0/bin/sql-client.sh" \
            --jar "$jar" --file "$work_dir/smoke.sql"
        ;;
    presto)
        jar="$artifact_dir/lakesoul-presto-0.296-${version}.jar"
        test -f "$jar"
        curl --fail --location --retry 3 \
            --output "$work_dir/presto.tgz" \
            "https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.296/presto-server-0.296.tar.gz"
        curl --fail --location --retry 3 \
            --output "$work_dir/presto-cli" \
            "https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.296/presto-cli-0.296-executable.jar"
        chmod +x "$work_dir/presto-cli"
        tar -C "$work_dir" -xzf "$work_dir/presto.tgz"
        presto_home="$work_dir/presto-server-0.296"
        mkdir -p "$presto_home/etc/catalog" "$presto_home/plugin/lakesoul" "$work_dir/presto-data"
        cp "$jar" "$presto_home/plugin/lakesoul/"
        cat > "$presto_home/etc/config.properties" <<'PROPERTIES'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=18080
discovery-server.enabled=true
discovery.uri=http://127.0.0.1:18080
PROPERTIES
        cat > "$presto_home/etc/node.properties" <<PROPERTIES
node.environment=lakesoul-release-smoke
node.id=lakesoul-release-smoke
node.data-dir=${work_dir}/presto-data
PROPERTIES
        cat > "$presto_home/etc/jvm.config" <<'JVM'
-server
-Xmx1G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dlakesoul.pg.url=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
-Dlakesoul.pg.username=lakesoul_test
-Dlakesoul.pg.password=lakesoul_test
JVM
        cat > "$presto_home/etc/catalog/lakesoul.properties" <<'PROPERTIES'
connector.name=lakesoul
case-sensitive-name-matching=false
PROPERTIES
        "$presto_home/bin/launcher" run > "$work_dir/presto.log" 2>&1 &
        presto_pid=$!
        for _ in $(seq 1 60); do
            if curl --fail --silent http://127.0.0.1:18080/v1/info >/dev/null; then
                break
            fi
            if ! kill -0 "$presto_pid" 2>/dev/null; then
                cat "$work_dir/presto.log" >&2
                exit 1
            fi
            sleep 2
        done
        curl --fail --silent http://127.0.0.1:18080/v1/info >/dev/null || {
            cat "$work_dir/presto.log" >&2
            exit 1
        }
        catalogs=$(
            "$work_dir/presto-cli" --server http://127.0.0.1:18080 \
                --execute 'SHOW CATALOGS' | tr -d '"'
        )
        case $'\n'"$catalogs"$'\n' in
            *$'\nlakesoul\n'*) ;;
            *)
                printf 'LakeSoul catalog was not loaded; catalogs:\n%s\n' "$catalogs" >&2
                exit 1
                ;;
        esac
        ;;
    *)
        usage
        ;;
esac
