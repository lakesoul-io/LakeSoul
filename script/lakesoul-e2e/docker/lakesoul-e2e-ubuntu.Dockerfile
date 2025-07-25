# SPDX-FileCopyrightText: LakeSoul Contributors
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
# K8S
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.33/deb/Release.key | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
RUN chmod 644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg 
RUN echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.33/deb/ /' | tee /etc/apt/sources.list.d/kubernetes.list
RUN chmod 644 /etc/apt/sources.list.d/kubernetes.list
RUN apt update && apt install -y kubectl
ENV LAKESOUL_HOME=/opt/lakesoul
RUN mkdir -p $LAKESOUL_HOME/bin
ENV PATH=$LAKESOUL_HOME/bin:$PATH
RUN groupadd --system --gid=9999 lakesoul && useradd --system --home-dir $LAKESOUL_HOME --uid 9999 --gid=lakesoul lakesoul
# UV
RUN pip install -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple uv  
ENV UV_PYTHON_INSTALL_MIRROR="https://ghfast.top/https://github.com/indygreg/python-build-standalone/releases/download"
# protoc
COPY  opt/protoc-25.7-linux-x86_64.zip /opt/protoc25.zip
RUN unzip /opt/protoc25.zip -d /opt/protoc25
ENV PATH=/opt/protoc25/bin:$PATH
COPY --chown=lakesoul:lakesoul settings.xml $LAKESOUL_HOME/.m2/settings.xml
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
# flink
COPY opt/flink-1.20.1-bin-scala_2.12.tgz /opt/flink-1.20.1-bin-scala_2.12.tgz
RUN tar -xzvf /opt/flink-1.20.1-bin-scala_2.12.tgz -C /opt && mv /opt/flink-1.20.1 /opt/flink
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
RUN chown -R lakesoul:lakesoul $FLINK_HOME
# spark
COPY opt/spark-3.3.2-bin-hadoop3.tgz /opt/spark-3.3.2-bin-hadoop3.tgz
RUN tar -xzvf /opt/spark-3.3.2-bin-hadoop3.tgz -C /opt && mv /opt/spark-3.3.2-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
RUN chown -R lakesoul:lakesoul $SPARK_HOME 
# e2e
COPY --chown=lakesoul:lakesoul e2e/dist/e2etest-0.1.1.tar.gz $LAKESOUL_HOME/
RUN chown -R lakesoul:lakesoul $LAKESOUL_HOME 
USER lakesoul
WORKDIR $LAKESOUL_HOME
RUN tar xvzf $LAKESOUL_HOME/e2etest-0.1.1.tar.gz -C $LAKESOUL_HOME && cd $LAKESOUL_HOME/e2etest-0.1.1 && uv sync --index https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple && uv build && ln -s $LAKESOUL_HOME/e2etest-0.1.1/.venv/bin/e2etest $LAKESOUL_HOME/bin/e2etest
# RUST
RUN curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh -s -- -y --profile minimal --default-toolchain none
ENV PATH=$LAKESOUL_HOME/.cargo/bin:$PATH
RUN echo '[source.crates-io]' > $LAKESOUL_HOME/.cargo/config.toml && \
    echo 'replace-with = "rsproxy-sparse"' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo '[source.rsproxy]' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo 'registry = "https://rsproxy.cn/crates.io-index"' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo '[source.rsproxy-sparse]' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo 'registry = "sparse+https://rsproxy.cn/index/"' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo '[registries.rsproxy]' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo 'index = "https://rsproxy.cn/crates.io-index"' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo '[net]' >> $LAKESOUL_HOME/.cargo/config.toml && \
    echo 'git-fetch-with-cli = true' >> $LAKESOUL_HOME/.cargo/config.toml
# build

