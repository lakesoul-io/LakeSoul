# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

FROM docker.1ms.run/library/flink:2.1.0-scala_2.12-java11


# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
RUN pip3 install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple apache-flink==2.1.0

RUN mkdir ./plugins/s3 && cp ./opt/flink-s3-fs-hadoop-2.1.0.jar ./plugins/s3
