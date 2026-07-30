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
mkdir -p "${HOME}/.cache/uv"
mkdir -p "${HOME}/.config/uv"
mkdir -p "${HOME}/.local/share/uv"
mkdir -p "${HOME}/.cache/cargo-zigbuild"
mkdir -p "${HOME}/.cache/zig"

# -------- Info --------
echo "============================================"
echo " LakeSoul Python Build (containerized)"
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
docker run --rm -ti --net host \
  --user "${USER_ID}:${GROUP_ID}" \
  --env USER="${USER}" \
  --env UV_DEFAULT_INDEX="https://mirrors.huaweicloud.com/repository/pypi/simple" \
  --env PIP_INDEX_URL="https://mirrors.huaweicloud.com/repository/pypi/simple" \
  --env HOME=$HOME \
  --volume "${PROJECT_DIR}:${PROJECT_DIR}:rw" \
  --volume "${HOME}/.cargo/registry:/opt/cargo/registry:rw" \
  --volume "${HOME}/.cargo/git:/opt/cargo/git:rw" \
  --volume "${HOME}/.cargo/config.toml:/opt/cargo/config.toml" \
  --volume "${HOME}/.cache/uv:/opt/uv-cache:rw" \
  --volume "${HOME}/.cache/zig:${HOME}/.cache/zig:rw" \
  --volume "${HOME}/.cache/cargo-zigbuild:${HOME}/.cache/cargo-zigbuild:rw" \
  --volume "${HOME}/.config/uv:${HOME}/.config/uv:rw" \
  --volume "${HOME}/.local/share/uv:${HOME}/.local/share/uv:rw" \
  --workdir "${PROJECT_DIR}" \
  "${IMAGE}" \
  bash -c '
    set -euox pipefail

    echo
    echo "=== Toolchain ==="
    rustc --version
    cargo --version
    uv --version
    echo

    echo "=== Building python workspace ==="
    cd python
    uvx --from maturin[zig] maturin build --release --zig --target x86_64-unknown-linux-gnu --auditwheel repair --compatibility manylinux2014 --out ../dist
    echo
    echo "=== Build complete ==="
  '