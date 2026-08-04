#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CURRENT_USER=$(id -un)

: "${HADOOP_HOME:?HADOOP_HOME is not set. Enter the development shell with: nix develop}"
: "${JAVA_HOME:?JAVA_HOME is not set. Enter the development shell with: nix develop}"

export LAKESOUL_TEST_HDFS_URL=${LAKESOUL_TEST_HDFS_URL:-hdfs://localhost:9000}
export LAKESOUL_HDFS_TEST_HOME=${LAKESOUL_HDFS_TEST_HOME:-$HOME/.local/share/lakesoul-hdfs-test}
export HADOOP_CONF_DIR=$LAKESOUL_HDFS_TEST_HOME/etc
export HADOOP_LOG_DIR=$LAKESOUL_HDFS_TEST_HOME/logs
export HADOOP_PID_DIR=$LAKESOUL_HDFS_TEST_HOME/pids
export CLASSPATH="$HADOOP_CONF_DIR:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*"
export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

NAME_DIR=$LAKESOUL_HDFS_TEST_HOME/name
DATA_DIR=$LAKESOUL_HDFS_TEST_HOME/data
KEEP_HDFS_RUNNING=${KEEP_HDFS_RUNNING:-0}
RESET_HDFS=${RESET_HDFS:-0}
STARTED_NAMENODE=0
STARTED_DATANODE=0

log() {
  printf '[hdfs-test] %s\n' "$*"
}

is_daemon_running() {
  hdfs --daemon status "$1" >/dev/null 2>&1
}

print_logs() {
  if compgen -G "$HADOOP_LOG_DIR/*" >/dev/null; then
    log "Last 100 lines of Hadoop logs:"
    tail -n 100 "$HADOOP_LOG_DIR"/* || true
  fi
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM

  if ((status != 0)); then
    print_logs
  fi

  if [[ "$KEEP_HDFS_RUNNING" != "1" ]]; then
    if [[ "$STARTED_DATANODE" == "1" ]]; then
      log "Stopping DataNode"
      hdfs --daemon stop datanode >/dev/null 2>&1 || true
    fi
    if [[ "$STARTED_NAMENODE" == "1" ]]; then
      log "Stopping NameNode"
      hdfs --daemon stop namenode >/dev/null 2>&1 || true
    fi
  else
    log "Keeping HDFS running because KEEP_HDFS_RUNNING=1"
  fi

  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$HADOOP_CONF_DIR" "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$NAME_DIR" "$DATA_DIR"

if [[ ! -f "$HADOOP_CONF_DIR/hadoop-env.sh" ]]; then
  log "Copying Hadoop configuration templates"
  cp -a "$HADOOP_HOME/etc/hadoop/." "$HADOOP_CONF_DIR/"
  chmod -R u+w "$HADOOP_CONF_DIR"
fi

cat >"$HADOOP_CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${LAKESOUL_TEST_HDFS_URL}</value>
  </property>
</configuration>
EOF

cat >"$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${NAME_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${DATA_DIR}</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>localhost:9870</value>
  </property>
</configuration>
EOF

if [[ "$RESET_HDFS" == "1" ]]; then
  if is_daemon_running namenode || is_daemon_running datanode; then
    log "Cannot reset HDFS data while a daemon is running"
    exit 1
  fi
  log "Resetting HDFS data under $LAKESOUL_HDFS_TEST_HOME"
  rm -rf "$NAME_DIR" "$DATA_DIR"
  mkdir -p "$NAME_DIR" "$DATA_DIR"
fi

if [[ ! -d "$NAME_DIR/current" ]]; then
  log "Formatting NameNode"
  hdfs namenode -format -nonInteractive
fi

if ! is_daemon_running namenode; then
  log "Starting NameNode at $LAKESOUL_TEST_HDFS_URL"
  hdfs --daemon start namenode
  STARTED_NAMENODE=1
else
  log "NameNode is already running"
fi

if ! is_daemon_running datanode; then
  log "Starting DataNode"
  hdfs --daemon start datanode
  STARTED_DATANODE=1
else
  log "DataNode is already running"
fi

log "Waiting for HDFS to become ready"
ready=0
for _ in $(seq 1 60); do
  if hdfs dfsadmin -report 2>/dev/null | grep -Eq 'Live datanodes \([1-9][0-9]*\)'; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" != "1" ]]; then
  log "HDFS did not become ready within 60 seconds"
  exit 1
fi

hdfs dfsadmin -safemode wait >/dev/null
hdfs dfs -mkdir -p "/user/$CURRENT_USER"
hdfs dfs -chmod -R 777 "/user/$CURRENT_USER"

log "HDFS is ready; running LakeSoul HDFS integration test"
cd "$ROOT_DIR"
cargo -q test \
  --package lakesoul-io \
  --features hdfs \
  hdfs::tests::test_hdfs \
  -- --ignored --nocapture

log "LakeSoul HDFS integration test passed"
