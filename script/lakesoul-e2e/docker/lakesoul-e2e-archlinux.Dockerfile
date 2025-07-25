# SPDX-FileCopyrightText: LakeSoul Contributors
# 
# SPDX-License-Identifier: Apache-2.0
FROM docker.1ms.run/archlinux/archlinux:base-devel-20250630.0.373922
RUN echo "Server = https://mirrors.tuna.tsinghua.edu.cn/archlinux/\$repo/os/\$arch" | cat - /etc/pacman.d/mirrorlist > tmpf && mv tmpf /etc/pacman.d/mirrorlist
RUN pacman -Sy && pacman -S --noconfirm cargo rustup git jdk11-openjdk cmake make maven zip unzip wget python3 python-pip python-pipx kubectl uv
ENV LAKESOUL_HOME=/opt/lakesoul
RUN mkdir -p $LAKESOUL_HOME/bin
ENV PATH=$LAKESOUL_HOME/bin:$PATH
RUN groupadd --system --gid=9999 lakesoul && useradd --system --home-dir $LAKESOUL_HOME --uid 9999 --gid=lakesoul lakesoul
# protoc
COPY --chown=lakesoul:lakesoul opt/protoc-25.7-linux-x86_64.zip /opt/protoc25.zip
RUN unzip /opt/protoc25.zip -d /opt/protoc25
ENV PATH=/opt/protoc25/bin:$PATH
COPY --chown=lakesoul:lakesoul settings.xml $LAKESOUL_HOME/.m2/settings.xml
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
# flink
RUN wget https://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz -O /opt/flink-1.20.1-bin-scala_2.12.tgz && tar -xzvf /opt/flink-1.20.1-bin-scala_2.12.tgz  -C /opt  && mv /opt/flink-1.20.1 /opt/flink
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
# spark
RUN wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop3.tgz -O /opt/spark-3.3.2-bin-hadoop3.tgz && tar -xzvf /opt/spark-3.3.2-bin-hadoop3.tgz -C /opt && mv /opt/spark-3.3.2-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY --chown=lakesoul:lakesoul e2e/dist/e2etest-0.1.1.tar.gz $LAKESOUL_HOME/
RUN tar xvzf $LAKESOUL_HOME/e2etest-0.1.1.tar.gz -C $LAKESOUL_HOME && cd $LAKESOUL_HOME/e2etest-0.1.1 && uv sync && uv build && ln -s $LAKESOUL_HOME/e2etest-0.1.1/.venv/bin/e2etest $LAKESOUL_HOME/bin/e2etest
RUN chown -R lakesoul:lakesoul $LAKESOUL_HOME 
USER lakesoul
WORKDIR $LAKESOUL_HOME