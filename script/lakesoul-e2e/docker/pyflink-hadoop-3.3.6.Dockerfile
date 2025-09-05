# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
FROM swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/flink:1.20.1-scala_2.12-java11
# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python
# install PyFlink
RUN pip3 install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple apache-flink==1.20.1
RUN mkdir -p /opt/flink/plugins/s3 && mkdir ./plugins/s3-fs-presto && cp ./opt/flink-s3-fs-presto-2.1.0.jar ./plugins/s3-fs-presto/
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-1.20.1.jar /opt/flink/plugins/s3/
COPY opt/hadoop-3.3.6.tar.gz /opt/hadoop-3.3.6.tar.gz
RUN tar -xzf /opt/hadoop-3.3.6.tar.gz -C /opt && mv /opt/hadoop-3.3.6 /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/flink/lib/hadoop-aws-3.3.6.jar
RUN cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/flink/lib/aws-java-sdk-bundle-1.12.367.jar
COPY opt/parquet-hadoop-bundle-1.13.1.jar /opt/flink/lib/
RUN chown -R flink:flink /opt/flink
ENV HADOOP_CLASSPATH=/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*


