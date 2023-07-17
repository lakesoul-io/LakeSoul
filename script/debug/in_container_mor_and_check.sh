# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

spark-submit --driver-memory 16g --jars /opt/bitnami/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT.jar --class org.apache.spark.sql.lakesoul.benchmark.io.MorReadBenchmark /opt/bitnami/spark/work-dir/jars/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar --localtest $1

spark-submit --driver-memory 16G --executor-memory 16G --master "local[4]" --conf spark.pyspark.python=./venv/bin/python3 /opt/bitnami/spark/CCFCheck.py merge /tmp/result/ccf/result /tmp/result/ccf/expected