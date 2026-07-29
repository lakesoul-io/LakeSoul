# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:20.04

LABEL authors="LakeSoul Contributors"
LABEL description="LakeSoul build environment base image (Rust + Java + Python)"
LABEL version="1.0"

RUN sed -i "s@http://.*archive.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list
RUN sed -i "s@http://.*security.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive
# -------- System packages & build essentials --------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    pkg-config \
    autoconf \
    automake \
    libtool \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libreadline-dev \
    libncurses5-dev \
    libffi-dev \
    curl \
    wget \
    ca-certificates \
    gnupg \
    git \
    unzip \
    tzdata \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# -------- Environment variables --------
ENV TZ=Asia/Shanghai
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# -------- uv (Python package manager) --------
ENV UV_CACHE_DIR=/opt/uv-cache
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# -------- Java (JDK 17 + Maven) --------
# JDK 17 Temurin (matches the project's flake.nix development default)
# Java 8 target compatibility is configured via Maven pom.xml
RUN curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium-archive-keyring.gpg] https://packages.adoptium.net/artifactory/deb focal main" > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends temurin-17-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Maven
ARG MAVEN_VERSION=3.9.16
RUN curl -fsSL https://mirrors.tuna.tsinghua.edu.cn/apache/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    | tar -xzC /opt && \
    ln -s /opt/apache-maven-${MAVEN_VERSION} /opt/maven

ENV MAVEN_HOME=/opt/maven
ENV PATH=${MAVEN_HOME}/bin:${PATH}
ENV MAVEN_OPTS="-Xmx4g \
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    -Dio.netty.tryReflectionSetAccessible=true \
    -Duser.timezone=UTC"

# -------- Rust --------
ARG RUST_VERSION=stable
ENV RUSTUP_HOME=/opt/rustup
ENV CARGO_HOME=/opt/cargo
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
RUN mkdir -p ${RUSTUP_HOME} ${CARGO_HOME} && \
    curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | \
    RUSTUP_HOME=${RUSTUP_HOME} CARGO_HOME=${CARGO_HOME} sh -s -- -y \
    --profile minimal \
    --default-toolchain ${RUST_VERSION} \
    --component clippy \
    --component rustfmt \
    --component rust-analyzer

ENV PATH=${CARGO_HOME}/bin:${PATH}
RUN chmod -R 777 /opt/rustup/tmp

ENV PROTOC_VERSION=25.3
ENV PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip
ENV PROTOC_URL=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}
RUN curl -LO ${PROTOC_URL} && \
    unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc && \
    unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*' && \
    rm ${PROTOC_ZIP}
