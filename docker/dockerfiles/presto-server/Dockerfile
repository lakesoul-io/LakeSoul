# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

FROM openjdk:11
LABEL author=yuanf
LABEL version="1.0"
LABEL description="presto server simplified"
LABEL build_cmd = "docker build -t foo:1.0 ."

RUN apt update && apt install -y curl

RUN apt install -y python-is-python3

RUN cd /root \
    && curl https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/yuanf/presto-server-pure-0.283.tar > presto.zip \
    && tar -xvf presto.zip  \
    && rm -rf presto.zip

ENV PRESTO_HOME /root/presto-server-pure
WORKDIR $PRESTO_HOME
EXPOSE 8080
VOLUME $PRESTO_HOME/plugin

CMD bin/launcher run



