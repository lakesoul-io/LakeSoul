FROM swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/flink:1.20.1-scala_2.12-java11
RUN mkdir -p /opt/flink/plugins/s3
RUN cp /opt/flink/opt/flink-s3-fs-hadoop-1.20.1.jar /opt/flink/plugins/s3/
COPY opt/hadoop-3.3.6.tar.gz /opt/hadoop-3.3.6.tar.gz
RUN tar -xzf /opt/hadoop-3.3.6.tar.gz -C /opt && mv /opt/hadoop-3.3.6 /opt/hadoop
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/flink/lib/hadoop-aws-3.3.6.jar
RUN cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/flink/lib/aws-java-sdk-bundle-1.12.367.jar
COPY opt/parquet-hadoop-bundle-1.13.1.jar /opt/flink/lib/
RUN chown -R flink:flink /opt/flink
ENV HADOOP_CLASSPATH=/opt/hadoop-3.3.6/etc/hadoop:/opt/hadoop-3.3.6/share/hadoop/common/lib/*:/opt/hadoop-3.3.6/share/hadoop/common/*:/opt/hadoop-3.3.6/share/hadoop/hdfs:/opt/hadoop-3.3.6/share/hadoop/hdfs/lib/*:/opt/hadoop-3.3.6/share/hadoop/hdfs/*:/opt/hadoop-3.3.6/share/hadoop/mapreduce/*:/opt/hadoop-3.3.6/share/hadoop/yarn:/opt/hadoop-3.3.6/share/hadoop/yarn/lib/*:/opt/hadoop-3.3.6/share/hadoop/yarn/*