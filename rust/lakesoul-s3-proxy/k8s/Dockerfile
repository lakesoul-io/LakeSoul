FROM ubuntu:22.04

RUN sed -i "s@http://.*archive.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list && \
    sed -i "s@http://.*security.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install net-tools procps telnet vim curl libjemalloc2 -y && \
    apt-get clean

ENV LAKESOUL_USER=lakesoul
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

COPY target/release/lakesoul-s3-proxy /app/lakesoul-s3-proxy
RUN chown -R $LAKESOUL_USER:$LAKESOUL_USER /app \
    && chmod +x /app/lakesoul-s3-proxy
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

SHELL ["/bin/bash", "-c"]

RUN echo $'---\n\
version: 1\n\
threads: 4' > /opt/proxy_conf.yaml

USER ${LAKESOUL_USER}
WORKDIR ${HOME}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
ENV RUST_LOG_FORMAT="%Y-%m-%dT%H:%M:%S%:z %l [%f:%L] %m"
