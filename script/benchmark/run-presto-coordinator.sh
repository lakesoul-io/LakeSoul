#!/usr/bin/env bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
PROJECT_DIR=$(realpath $SCRIPT_DIR/../..)
PRESTO_JAR_NAME=$(python $SCRIPT_DIR/../get_jar_name.py $PROJECT_DIR/lakesoul-presto)
PRESTO_JAR_FILE_PATH=$PROJECT_DIR/lakesoul-presto/target/$PRESTO_JAR_NAME

COORDINATOR_ETC_DIR=$(realpath $1)

if [ "$2" = "start" ]; then
  docker run -d --name=presto --net host \
  -v $PRESTO_JAR_FILE_PATH:/opt/presto-server/plugin/lakesoul/${PRESTO_JAR_NAME} \
  -v $COORDINATOR_ETC_DIR/catalog:/opt/presto-server/etc/catalog \
  -v $COORDINATOR_ETC_DIR/config.properties:/opt/presto-server/etc/config.properties \
  -v $COORDINATOR_ETC_DIR/node.properties:/opt/presto-server/etc/node.properties \
  -v $COORDINATOR_ETC_DIR/log.properties:/opt/presto-server/etc/log.properties \
  -v $COORDINATOR_ETC_DIR/jvm.config:/opt/presto-server/etc/jvm.config \
  -v ${PWD}/script/benchmark/work-dir:/opt/spark/work-dir \
  -v ${COORDINATOR_ETC_DIR}/lakesoul.properties:/root/lakesoul.properties \
  --env lakesoul_home=/root/lakesoul.properties \
  -p 8080:8080 \
  prestodb/presto:0.292
elif [ "$2" = "stop" ]; then
  docker rm -f presto
fi
