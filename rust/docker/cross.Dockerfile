# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:20.04

RUN sed -i "s@http://.*archive.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list && \
    sed -i "s@http://.*security.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install build-essential clang lld openjdk-11-jdk-headless zip unzip git curl wget -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV LD_LIBRARY_PATH=$JAVA_HOME/lib/server

ENV PROTOC_VERSION=25.3
ENV PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip
ENV PROTOC_URL=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}
RUN curl -LO ${PROTOC_URL} && \
    unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc && \
    unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*' && \
    rm ${PROTOC_ZIP}
