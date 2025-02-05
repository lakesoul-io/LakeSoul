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

# echo "Prepare python environment..."
# if [ ! -d ".venv" ]; then
#     python -m venv .venv
#     source .venv/bin/activate
#     # 在本地安装必要的Python包
#     pip install -r requirements.txt
# else
#     source .venv/bin/activate
# fi

mkdir -p packages/jars

cp ../../lakesoul-spark/target/${SPARK_LAKESOUL_JAR} packages/jars/
cp ../../lakesoul-spark/target/${SPARK_LAKESOUL_TEST_JAR} packages/jars/


echo "Start docker-compose..."
docker compose -f docker-compose.yml --env-file docker-compose.env up -d

echo "Waiting for MinIO to be ready..."
sleep 10  # 给 MinIO 一些启动时间

# 创建 bucket
docker exec lakesoul-ann-minio mc alias set local http://localhost:9000 admin password
docker exec lakesoul-ann-minio mc mb local/lakesoul-test-bucket
docker exec lakesoul-ann-minio mc policy set public local/lakesoul-test-bucket


# 首先将 Python 脚本和虚拟环境复制到容器中
# docker cp .venv/lib/python3.11/site-packages lakesoul-ann-spark:/opt/bitnami/python/lib/python3.8

# bash prepare_ann_table.sh

echo "============================================================================="
echo "Success to launch LSH ANN benchmark environment!"
echo "You can:"
echo "    'bash start_spark_sql.sh' to login into spark"
echo "============================================================================="

