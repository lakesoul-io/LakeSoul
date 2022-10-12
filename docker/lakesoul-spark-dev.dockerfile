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
RUN echo "deb http://repo.huaweicloud.com/ubuntu focal-security universe" >>/etc/apt/sources.list
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

RUN --mount=type=cache,target=/root/.m2/repository mvn package -f /opt/LakeSoul/pom.xml -pl lakesoul-spark -am -Dmaven.test.skip=true -DskipTests

FROM env as release
RUN apt-get install -y tini python3.8 python3-setuptools ca-certificates openjdk-11-jre wget && apt-get clean
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 30
RUN python -m easy_install install pip
RUN python -m pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
ARG SPARK_FILE="https://ks3-cn-beijing.ksyuncs.com/dmetasoul-bucket/releases/spark/spark-3.1.2-bin-free-265f9ad4ee.tgz"
RUN mkdir -p /opt/spark && wget ${SPARK_FILE} && tar xf `basename ${SPARK_FILE}` -C /opt/spark --strip-components 1 && rm -f `basename ${SPARK_FILE}`
ENV SPARK_HOME /opt/spark
COPY --from=build /opt/LakeSoul/lakesoul-spark/target/lakesoul-spark-2.1.0-spark-3.1.2-SNAPSHOT.jar /opt/spark/jars
ENV SPARK_CONF_DIR /opt/spark/conf
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
RUN mkdir -p /opt/spark/conf
ENV SPARK_CONF_DIR /opt/spark/conf
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr

RUN mkdir -p /opt/spark/work-dir
RUN wget https://raw.githubusercontent.com/apache/spark/v3.1.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh -O /opt/entrypoint.sh
RUN wget https://raw.githubusercontent.com/apache/spark/v3.1.2/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/decom.sh -O /opt/decom.sh
WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir
RUN chmod a+x /opt/decom.sh
RUN chmod a+x /opt/entrypoint.sh
RUN chgrp root /etc/passwd && chmod ug+rw /etc/passwd
ENTRYPOINT [ "/opt/entrypoint.sh" ]