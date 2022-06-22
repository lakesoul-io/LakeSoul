FROM ubuntu:20.04 as build

ENV DEBIAN_FRONTEND=noninteractive
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal main restricted" >/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal-updates main restricted" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal universe" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal-updates universe" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal multiverse" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal-updates multiverse" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu/ focal-backports main restricted universe multiverse" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu focal-security main restricted" >>/etc/apt/sources.list
RUN echo "deb http://repo.huaweicloud.com/ubuntu focal-security multiverse" >>/etc/apt/sources.list
RUN apt-get update && apt-get install -y openjdk-11-jdk maven && apt-get clean

COPY . /opt/LakeSoul
COPY docker/settings.xml /root/.m2/settings.xml

RUN --mount=type=cache,target=/root/.m2/repository mvn package -f /opt/LakeSoul/pom.xml -Dmaven.test.skip=true -DskipTests
RUN --mount=type=cache,target=/opt/LakeSoul/jars mvn -f /opt/LakeSoul/pom.xml dependency:copy-dependencies -DoutputDirectory=/opt/LakeSoul/jars -DincludeScope=runtime -DexcludeGroupIds=org.apache.parquet,org.apache.flink,org.scalatest,org.scala-lang,org.apache.livy,junit,org.slf4j,org.apache.logging.log4j,io.netty,org.apache.commons,org.xerial.snappy,com.codahale.metrics

FROM apache/spark:v3.1.3

USER root
RUN rm -f /opt/spark/jars/HikariCP*
COPY --from=build /opt/LakeSoul/jars/*.jar /opt/spark/jars
COPY --from=build /opt/LakeSoul/target/lakesoul-1.1.0.jar /opt/spark/jars

USER 185