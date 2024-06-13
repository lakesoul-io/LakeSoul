# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

ARG BASE_IMG=dockcross/manylinux2014-x64:latest

FROM $BASE_IMG

RUN yum update -y && yum -y install java-11-openjdk java-11-openjdk-devel xz-devel && yum clean all

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$LD_LIBRARY_PATH