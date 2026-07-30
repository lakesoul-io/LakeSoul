#!/usr/bin/env bash
# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Build LakeSoul Rust workspace inside the build-env container.
# Reuses host cargo caches and writes output files as the host user.

set -euo pipefail

# -------- Configuration --------
IMAGE="swr.cn-north-4.myhuaweicloud.com/lakeinsight-repo/lakesoul-build-env:latest"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
USER_ID="$(id -u)"
GROUP_ID="$(id -g)"

# -------- Ensure host cache dirs --------
mkdir -p "${HOME}/.m2"

# -------- Info --------
echo "============================================"
echo " LakeSoul Java Build (containerized)"
echo "============================================"
echo " Project : ${PROJECT_DIR}"
echo " User    : ${USER} (uid=${USER_ID}, gid=${GROUP_ID})"
echo " Image   : ${IMAGE}"
echo " Caches  :"
echo "   maven cache : ${HOME}/.m2"
echo "============================================"
echo

# -------- Run build --------
docker run --rm -ti --net host \
  --user "${USER_ID}:${GROUP_ID}" \
  --env USER="${USER}" \
  --env HOME="${HOME}" \
  --volume "${PROJECT_DIR}:${PROJECT_DIR}:rw" \
  --volume "${HOME}/.m2:/opt/maven/.m2:rw" \
  --workdir "${PROJECT_DIR}" \
  "${IMAGE}" \
  bash -c '
    set -euo pipefail

    echo
    echo "=== Toolchain ==="
    java -version
    mvn -version
    echo

    echo "=== Building Java workspace ==="
    mvn package -pl lakesoul-spark -pl lakesoul-flink -pl lakesoul-presto -am -Pcross-build -DskipTests -Dmaven.repo.local=/opt/maven/.m2/repository --settings /opt/maven/.m2/settings.xml
    echo
    echo "=== Build complete ==="
  '
