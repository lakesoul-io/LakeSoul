FROM ubuntu:22.04

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop

RUN sed -i "s@http://.*archive.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list && \
    sed -i "s@http://.*security.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install net-tools procps telnet vim openjdk-11-jre-headless curl libjemalloc2 -y

RUN curl -L -o /opt/hadoop-${HADOOP_VERSION}.tar.gz https://mirrors.huaweicloud.com/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    HADOOP_TAR_NAME=hadoop-${HADOOP_VERSION} && \
    tar -xzf /opt/${HADOOP_TAR_NAME}.tar.gz -C /opt && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /opt/${HADOOP_TAR_NAME}.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}

ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

ENV LAKESOUL_USER=stackable
ENV LAKESOUL_UID=1000
ENV HOME=/home/$LAKESOUL_USER
ENV SHELL=/bin/bash
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV TZ=Asia/Shanghai

RUN groupadd -r -g ${LAKESOUL_UID} ${LAKESOUL_USER} \
 && useradd -M -s /bin/bash -N -u ${LAKESOUL_UID} -g ${LAKESOUL_UID} ${LAKESOUL_USER} \
 && mkdir -p ${HOME} \
 && chown -R ${LAKESOUL_USER}:users ${HOME} \
 && passwd -d ${LAKESOUL_USER} \
 && usermod -aG sudo ${LAKESOUL_USER} \
 && echo ${LAKESOUL_USER}' ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
 && mkdir -p /app

COPY rust/target/release/flight_sql_server /app/flight_sql_server
RUN chown -R $LAKESOUL_USER:$LAKESOUL_USER /app \
    && chmod +x /app/flight_sql_server 

USER ${LAKESOUL_USER}
WORKDIR ${HOME}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
ENV RUST_LOG_FORMAT="%Y-%m-%dT%H:%M:%S%:z %l [%f:%L] %m"