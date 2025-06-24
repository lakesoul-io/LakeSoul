#!/usr/bin/env bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
PROJECT_DIR=$(realpath $SCRIPT_DIR/../..)

WORKER_ETC_DIR=$PROJECT_DIR/script/benchmark/presto-native-worker
PRESTO_SERVER_BINARY=$(realpath $1)

if [ "$2" = "start" ]; then
  export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
  export RUST_LOG=info
  nohup $PRESTO_SERVER_BINARY \
  -etc_dir $WORKER_ETC_DIR \
  -logtostderr true \
  > $SCRIPT_DIR/velox.log 2>&1 &
elif [ "$2" = "stop" ]; then
  pkill presto_server
fi
