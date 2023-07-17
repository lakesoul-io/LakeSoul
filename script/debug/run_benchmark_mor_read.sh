# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

docker run --rm -ti --net lakesoul-docker-compose-env-zenghua_default -v /home/huazeng/opt/spark/work-dir/result:/opt/spark/work-dir/result -v $PWD/lakesoul-spark/target:/opt/spark/work-dir/jars -v $PWD/script/benchmark/work-dir/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties --env RUST_BACKTRACE=1 --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 16g --jars /opt/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT.jar --class org.apache.spark.sql.lakesoul.benchmark.io.MorReadBenchmark /opt/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar --localtest