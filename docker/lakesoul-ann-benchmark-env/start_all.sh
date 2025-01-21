#!/usr/bin/env bash

set -e

LAKESOUL_VERSION=2.6.2-SNAPSHOT
SPARK_LAKESOUL_JAR=lakesoul-spark-3.3-${LAKESOUL_VERSION}.jar
SPARK_LAKESOUL_TEST_JAR=lakesoul-spark-3.3-${LAKESOUL_VERSION}-tests.jar

download_hdf5_file() {
    local FILE_NAME=$1
    local EXPECTED_MD5=$2
    local DOWNLOAD_URL=$3

    if [[ -f ${FILE_NAME} ]]; then
        local FILE_MD5
        echo "compare md5 of ${FILE_NAME} with ${EXPECTED_MD5}"
        FILE_MD5=$(md5sum ${FILE_NAME} | awk '{print $1}')
        if [[ "${FILE_MD5}" != "${EXPECTED_MD5}" ]]; then
            echo "md5 not match, expected: ${EXPECTED_MD5}, actual: ${FILE_MD5}"
            rm "${FILE_NAME}"
            wget "${DOWNLOAD_URL}/${FILE_NAME}"
        fi
    else
        echo "download ${FILE_NAME} "
        wget "${DOWNLOAD_URL}/${FILE_NAME}"
    fi
}

mkdir -p data/embeddings

download_hdf5_file "fashion-mnist-784-euclidean.hdf5" "23249362dbd58a321c05b1a85275adba" "http://ann-benchmarks.com"
cp fashion-mnist-784-euclidean.hdf5 data/embeddings/

mkdir -p packages/jars

cp ../../lakesoul-spark/target/${SPARK_LAKESOUL_JAR} packages/jars/
cp ../../lakesoul-spark/target/${SPARK_LAKESOUL_TEST_JAR} packages/jars/


echo "Start docker-compose..."
docker compose -f docker-compose.yml --env-file docker-compose.env up -d

echo "Start prepare data for LSH ANN benchmark..."

# docker exec -it lakesoul-ann-spark spark-submit \
#   --class org.apache.spark.sql.lakesoul.benchmark.ann.LocalSensitiveHashANN \
#   --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
#   --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
#   --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
#   --conf spark.hadoop.fs.s3a.path.style.access=true \
#   --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
#   --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
#   --conf spark.hadoop.fs.s3a.access.key=admin \
#   --conf spark.hadoop.fs.s3a.secret.key=password \
#   /opt/packages/${SPARK_LAKESOUL_TEST_JAR} \
#   --HDF5_FILE=/data/embeddings/fashion-mnist-784-euclidean.hdf5

echo "============================================================================="
echo "Success to launch LSH ANN benchmark environment!"
echo "You can:"
echo "    'bash start_spark_sql.sh' to login into spark"
echo "============================================================================="

