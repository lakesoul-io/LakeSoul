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
mkdir -p "${HOME}/.cargo/registry"
mkdir -p "${HOME}/.cargo/git"

# -------- Info --------
echo "============================================"
echo " LakeSoul Rust Build (containerized)"
echo "============================================"
echo " Project : ${PROJECT_DIR}"
echo " User    : ${USER} (uid=${USER_ID}, gid=${GROUP_ID})"
echo " Image   : ${IMAGE}"
echo " Caches  :"
echo "   cargo registry : ${HOME}/.cargo/registry"
echo "   cargo git      : ${HOME}/.cargo/git"
echo "============================================"
echo

# -------- Run build --------
docker run --rm --net host \
  --user "${USER_ID}:${GROUP_ID}" \
  --env USER="${USER}" \
  --volume "${PROJECT_DIR}:${PROJECT_DIR}:rw" \
  --volume "${HOME}/.cargo/registry:/opt/cargo/registry:rw" \
  --volume "${HOME}/.cargo/git:/opt/cargo/git:rw" \
  --volume "${HOME}/.cargo/config.toml:/opt/cargo/config.toml" \
  --workdir "${PROJECT_DIR}" \
  "${IMAGE}" \
  bash -c '
    set -euo pipefail

    echo
    echo "=== Toolchain ==="
    rustc --version
    cargo --version
    echo

    echo "=== Building Rust workspace ==="
    cargo build --package lakesoul-io-c --package lakesoul-metadata-c --release --all-features
    echo
    echo "=== Build complete ==="
  '
