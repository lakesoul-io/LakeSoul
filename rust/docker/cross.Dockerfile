# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

FROM dockcross/manylinux2014-x64:latest
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && \
    sed -i 's|#baseurl=http://mirror.centos.org/centos/$releasever|baseurl=http://vault.centos.org/7|g' /etc/yum.repos.d/CentOS-*

RUN yum update -y && yum -y install java-11-openjdk java-11-openjdk-devel && yum clean all

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$LD_LIBRARY_PATH

ENV PROTOC_VERSION=25.3
ENV PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip
ENV PROTOC_URL=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}
RUN yum -y install unzip && yum clean all
RUN curl -LO ${PROTOC_URL} && \
    unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc && \
    unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*' && \
    rm ${PROTOC_ZIP}
