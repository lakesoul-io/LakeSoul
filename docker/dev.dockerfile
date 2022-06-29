FROM ubuntu:20.04 as env

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
RUN apt-get update && apt-get upgrade -y && apt-get clean
RUN apt-get install -y locales && apt-get clean
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_CTYPE=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV TZ=Asia/Shanghai
RUN ln -svf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && apt-get install -y tzdata && apt-get clean

FROM env as build
RUN apt-get install -y openjdk-11-jdk maven && apt-get clean

COPY . /opt/LakeSoul
COPY docker/settings.xml /root/.m2/settings.xml

RUN --mount=type=cache,target=/root/.m2/repository mvn package -f /opt/LakeSoul/pom.xml -Dmaven.test.skip=true -DskipTests
RUN mvn -f /opt/LakeSoul/pom.xml dependency:copy-dependencies -DoutputDirectory=/opt/LakeSoul/jars -DincludeScope=runtime -DexcludeGroupIds=org.apache.parquet,org.apache.flink,org.scalatest,org.scala-lang,org.apache.livy,junit,org.slf4j,org.apache.logging.log4j,io.netty,org.apache.commons,org.xerial.snappy,com.codahale.metrics

FROM env as release
RUN apt-get install -y tini python3.8 python3-setuptools ca-certificates openjdk-11-jre && apt-get clean
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 30
RUN python -m easy_install install pip
RUN python -m pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
RUN python -m pip install https://ks3-cn-beijing.ksyuncs.com/dmetasoul-bucket/releases/spark/pyspark-3.1.2.f8301b97d4-py2.py3-none-any.whl --no-cache-dir
RUN python -m pip install pyarrow pandas --no-cache-dir
ENV SPARK_HOME /usr/local/lib/python3.8/dist-packages/pyspark
RUN mkdir -p /opt/spark/conf
ENV SPARK_CONF_DIR /opt/spark/conf
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr

RUN rm -f $SPARK_HOME/jars/HikariCP*
COPY --from=build /opt/LakeSoul/jars/*.jar $SPARK_HOME/jars
COPY --from=build /opt/LakeSoul/target/lakesoul-1.1.0.jar $SPARK_HOME/jars
RUN mkdir -p /opt/spark/work-dir
RUN apt-get install -y wget && apt-get clean
RUN wget https://raw.githubusercontent.com/apache/spark/v3.1.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh -O /opt/entrypoint.sh
RUN wget https://raw.githubusercontent.com/apache/spark/v3.1.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/decom.sh -O /opt/decom.sh
WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh
RUN chmod a+x /opt/entrypoint.sh
RUN chgrp root /etc/passwd && chmod ug+rw /etc/passwd
ENTRYPOINT [ "/opt/entrypoint.sh" ]