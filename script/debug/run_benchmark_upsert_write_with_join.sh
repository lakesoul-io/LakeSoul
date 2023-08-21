# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

./script/debug/clean_docker_compose.sh
docker run --rm -ti --cpus 4 --net lakesoul-docker-compose-env-debug_default -v /opt/spark/work-dir/data:/opt/spark/work-dir/data -v $PWD/lakesoul-spark/target:/opt/spark/work-dir/jars -v $PWD/script/benchmark/work-dir/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 16g --jars /opt/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT.jar --class org.apache.spark.sql.lakesoul.benchmark.io.deltaJoin.UpsertWriteWithJoin /opt/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar --localtest
