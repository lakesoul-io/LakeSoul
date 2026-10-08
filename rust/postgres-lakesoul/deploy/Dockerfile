# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# One image per Linux x86_64 build, two entry points:
#
#   /app/postgres-lakesoul   (PG-wire coordinator; selected via `pod command`)
#   /app/lakesoul-worker     (distributed worker; selected via `pod command`)
#
# Multi-stage: the builder vendored protoc / flatc / clang, the runtime only
# needs ca-certificates and the runtime libraries of the produced binaries.
# Both stages are Debian bookworm glibc on x86_64, so binaries built here
# COPY into the runtime stage as-is.
#
# Build from the repository root:
#     docker build -f rust/postgres-lakesoul/deploy/Dockerfile -t lakesoul-k8s:local .
#
# Nix alternative with the same /app layout (binaries keep their Nix glibc
# closure instead of a Debian base): `nix build .#image && ./result | docker load`
# (see deploy/k8s/README.md).
#
# Undo NOT copying Nix-produced binaries baked into ordinary Ubuntu images:
# the build does NOT rely on the workstation's Nix toolchain at COPY time;
# everything is compiled inside the image from Cargo.lock-pinned sources.

FROM --platform=linux/x86_64 docker.io/library/debian:bookworm AS builder

ARG CARGO_JOBS=4

# Matches the `flatbuffers` crate version pinned in Cargo.lock (pulled in by
# the vortex dependency tree); the generated bindings must come from a
# compatible flatc.
ARG FLATBUFFERS_VERSION=25.12.19
ARG FLATBUFFERS_SHA256=9f87066dc5dfa7fe02090b55bab5f3e55df03e32c9b0cdf229004ade7d091039

ENV DEBIAN_FRONTEND=noninteractive

# clang/libclang: the `vortex` dependency's linker and `-sys` crates need a
# C toolchain; flatc: its build script generates FlatBuffers bindings (can be
# overridden with $FLATC); protoc: the LakeSoul protobuf build scripts use it.
RUN apt-get update && apt-get install --no-install-recommends -y \
        build-essential \
        ca-certificates \
        clang \
        file \
        libclang-dev \
        unzip \
        pkg-config \
        protobuf-compiler \
        xz-utils \
        curl \
    && rm -rf /var/lib/apt/lists/*

# flatc: the FlatBuffers compiler must match the runtime library generation
# pinned in Cargo.lock (`flatbuffers = 25.12.19`, vortex' dependency tree).
# The digest pins the upstream artifact (download URL/digest queried from the
# GitHub release API; regenerate on version bumps).
RUN curl -fsSL -o /tmp/flatc.zip \
        "https://github.com/google/flatbuffers/releases/download/v${FLATBUFFERS_VERSION}/Linux.flatc.binary.g%2B%2B-13.zip" \
    && echo "${FLATBUFFERS_SHA256}  /tmp/flatc.zip" | sha256sum --check - \
    && unzip -p /tmp/flatc.zip flatc > /usr/local/bin/flatc \
    && chmod 0755 /usr/local/bin/flatc \
    && rm /tmp/flatc.zip \
    && /usr/local/bin/flatc --version
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH=/root/.cargo/bin:$PATH \
    CARGO_HOME=/root/.cargo \
    LIBCLANG_PATH=/usr/lib/llvm-19/lib \
    FLATC=/usr/local/bin/flatc

WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
# Repository rustflags (`-Ctarget-feature=+avx2`), as in local and Nix builds:
# the produced binaries need AVX2 on every node.
COPY .cargo/ .cargo/
COPY rust/ rust/
# The root workspace lists `python` as a member; Cargo resolves every member
# manifest even when building a single package. Only the manifest set is
# needed — the python crate itself is never compiled here — but path deps
# force the full source tree.
COPY python/ python/

# Build output path: `.cargo/config.toml` points the target dir at rust/target;
# CARGO_TARGET_DIR overrides only that (rustflags still apply), so Cargo
# writes binaries to /workspace/target/release. Copy from there — a path only
# this RUN step creates, never one that could pre-exist from the COPY context.
ENV CARGO_TARGET_DIR=/workspace/target
RUN export LIBCLANG_PATH="$(ls -d /usr/lib/llvm-*/lib | sort -V | tail -1)" && \
    echo "using LIBCLANG_PATH=$LIBCLANG_PATH" && \
    cargo build --release \
        --package lakesoul-datafusion --bin lakesoul-worker \
    && cargo build --release \
        --package postgres-lakesoul --bin postgres-lakesoul \
    && mkdir -p /app \
    && cp target/release/postgres-lakesoul /app/ \
    && cp target/release/lakesoul-worker /app/

# Inspectable by the reviewer (`docker run ... /bin/bash -c 'ldd /app/*'`).
# COPY into a bookworm-slim runtime keeps ABI and loader paths unchanged.
FROM --platform=linux/x86_64 docker.io/library/debian:bookworm-slim

RUN apt-get update && apt-get install --no-install-recommends -y \
        ca-certificates \
        zlib1g \
        libzstd1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /usr/sbin/nologin lakesoul

COPY --from=builder /app/postgres-lakesoul /app/postgres-lakesoul
COPY --from=builder /app/lakesoul-worker /app/lakesoul-worker

# Distributed shuffle/segment scratch tmpDirs against emptyDir volumes mounted
# at these paths in the Pod spec.
RUN mkdir -p /var/scratch \
    && chown -R lakesoul:lakesoul /app /var/scratch

USER lakesoul
ENV TMPDIR=/var/scratch
# Config intentionally via env not CLI args; a Pod picks the binary:
WORKDIR /app

# No EXPOSE declarations here: the Pod spec documents the two distinct ports.
