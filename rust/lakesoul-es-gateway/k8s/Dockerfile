# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
#
# Build from the repository root:
#   cargo build --release -p lakesoul-es-gateway
#   docker build -f rust/lakesoul-es-gateway/k8s/Dockerfile -t lakesoul-es-gateway .
FROM ubuntu:22.04

RUN sed -i "s@http://.*archive.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list && \
    sed -i "s@http://.*security.ubuntu.com@http://mirrors.huaweicloud.com@g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
    apt-get install net-tools procps telnet vim curl libjemalloc2 ca-certificates -y && \
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

COPY rust/target/release/lakesoul-es-gateway /app/lakesoul-es-gateway
COPY rust/lakesoul-es-gateway/lakesoul-es-gateway.example.toml /opt/lakesoul-es-gateway.toml
RUN chown -R $LAKESOUL_USER:$LAKESOUL_USER /app /opt/lakesoul-es-gateway.toml \
    && chmod +x /app/lakesoul-es-gateway
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

USER ${LAKESOUL_USER}
WORKDIR ${HOME}
ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
EXPOSE 9200
ENTRYPOINT ["/app/lakesoul-es-gateway"]
CMD ["--config", "/opt/lakesoul-es-gateway.toml"]
