# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai
RUN apt update && apt upgrade -y ca-certificates
RUN echo "deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal main restricted universe multiverse" > /etc/apt/sources.list
RUN echo "deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-updates main restricted universe multiverse" >> /etc/apt/sources.list
RUN echo "deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ focal-backports main restricted universe multiverse" >> /etc/apt/sources.list
RUN echo "deb http://security.ubuntu.com/ubuntu/ focal-security main restricted universe multiverse" >> /etc/apt/sources.list
RUN apt update && apt install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    dpkg-reconfigure -f noninteractive tzdata
RUN apt install -y curl wget git openjdk-11-jdk cmake make maven zip unzip python3 python3-pip 
# protoc
COPY opt/protoc-25.7-linux-x86_64.zip /opt/protoc25.zip
RUN unzip /opt/protoc25.zip -d /opt/protoc25
ENV PATH=/opt/protoc25/bin:$PATH
COPY sysconf/settings.xml /etc/maven/settings.xml
# RUST
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV CARGO_HOME=/etc/cargo
ENV RUSTUP_HOME=/etc/rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh -s -- -y --profile minimal --default-toolchain none
COPY sysconf/config.toml $CARGO_HOME/config.toml
RUN ln -s $CARGO_HOME/bin/cargo /usr/bin/cargo
RUN chmod -R 777 $CARGO_HOME
RUN chmod -R 777 $RUSTUP_HOME
RUN chmod -R 777 /workspace