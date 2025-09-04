FROM swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/spark:3.3.3-scala2.12-java11-ubuntu
USER root
COPY opt/hadoop-3.3.2.tar.gz /opt/hadoop-3.3.2.tar.gz
RUN tar -xzf /opt/hadoop-3.3.2.tar.gz -C /opt/
RUN mv /opt/hadoop-3.3.2 /opt/hadoop
RUN cp /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.2.jar /opt/spark/jars/
RUN cp /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/
RUN chown -R spark:spark /opt/spark
USER spark
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CLASSPATH=/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*
ENV SPARK_DIST_CLASSPATH=/opt/hadoop/etc/hadoop:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/hdfs:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/yarn:/opt/hadoop/share/hadoop/yarn/lib/*:/opt/hadoop/share/hadoop/yarn/*
ENTRYPOINT [ "/opt/entrypoint.sh" ]