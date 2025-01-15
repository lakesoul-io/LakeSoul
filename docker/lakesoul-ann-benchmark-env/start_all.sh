#!/usr/bin/env bash

set -e

LAKESOUL_VERSION=2.6.1
SPARK_LAKESOUL_JAR=lakesoul-spark-3.3-${LAKESOUL_VERSION}.jar

cd lakesoul-spark
mvn clean package -DskipTests

cp target/lakesoul-spark-2.6.1.jar ../docker/lakesoul-ann-benchmark-env/packages/


echo "Start docker-compose..."
sudo docker compose -f docker-compose.yml --env-file docker-compose.env up -d

echo "Start prepare data for LSH ANN benchmark..."
sudo docker exec -it lakesoul-ann-spark spark-sql \
  --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  -f /opt/sql/prepare_ann_benchmark.sql

echo "============================================================================="
echo "Success to launch LSH ANN benchmark environment!"
echo "You can:"
echo "    'bash start_spark_sql.sh' to login into spark"
echo "============================================================================="

