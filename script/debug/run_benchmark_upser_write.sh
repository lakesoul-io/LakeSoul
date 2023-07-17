# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

docker run --rm -ti --net lakesoul-docker-compose-env_default -v /opt/spark/work-dir/data:/opt/spark/work-dir/data -v $PWD/lakesoul-spark/target:/opt/spark/work-dir/jars -v $PWD/script/benchmark/work-dir/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 4g --jars /opt/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT.jar --class org.apache.spark.sql.lakesoul.benchmark.io.UpsertWriteBenchmark /opt/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar --localtest