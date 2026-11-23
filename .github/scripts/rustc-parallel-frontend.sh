#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Cargo wrapper that enables rustc's parallel frontend for test harnesses.
#
# Test harnesses (`--test`) are executable artifacts, so sccache cannot cache
# them; they always run a full rustc frontend + codegen. Their borrow checking
# and type checking are single-threaded, which dominates a large crate's
# harness build. `--jobs-frontend` parallelizes those queries per item.
#
# The flag is unstable: this only works because the workflow sets
# RUSTC_BOOTSTRAP=1. It is applied to harnesses only, so cacheable library
# builds still go through sccache with their original command line (and thus
# keep hitting the existing cache entries).
#
# Cargo calls a RUSTC_WRAPPER as: <wrapper> <real-rustc> <args...>.

set -euo pipefail

rustc_bin="$1"
shift

if [ "$(basename "$rustc_bin")" = "rustc" ]; then
  for arg in "$@"; do
    if [ "$arg" = "--test" ]; then
      jobs="${LAKESOUL_FRONTEND_JOBS:-$(nproc)}"
      exec "$rustc_bin" -Zunstable-options --jobs-frontend="$jobs" "$@"
    fi
  done
fi

exec "${SCCACHE_PATH:-sccache}" "$rustc_bin" "$@"
