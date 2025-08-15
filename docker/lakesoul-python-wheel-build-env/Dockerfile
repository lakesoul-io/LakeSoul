FROM quay.io/pypa/manylinux_2_28_x86_64:latest

RUN yum update -y &&                        \
    yum clean packages

ENV PATH=/opt/rh/devtoolset-12/root/usr/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/rh/devtoolset-12/root/usr/lib64:/opt/rh/devtoolset-12/root/usr/lib:/opt/rh/devtoolset-12/root/usr/lib64/dyninst:/opt/rh/devtoolset-12/root/usr/lib/dyninst:${LD_LIBRARY_PATH}
ENV DEVTOOLSET_ROOTPATH=/opt/rh/devtoolset-12/root

ARG PIP_MIRROR="https://pypi.tuna.tsinghua.edu.cn/simple/"
RUN python3.8 -m pip config set global.index-url "${PIP_MIRROR}"                       

ARG NINJA_VERSION=1.11.1
RUN rm -f ninja-linux.zip &&                                                                               \
    rm -f /usr/bin/ninja &&                                                                                \
    curl -L -O https://github.com/ninja-build/ninja/releases/download/v${NINJA_VERSION}/ninja-linux.zip && \
    unzip ninja-linux.zip -d /usr/bin &&                                                                   \
    rm -f ninja-linux.zip &&                                                                               \
    echo OK: Ninja

ARG RUST_VERSION=1.87.0
ENV RUSTUP_HOME=/opt/rustup
ENV CARGO_HOME=/opt/cargo
RUN mkdir -p $RUSTUP_HOME $CARGO_HOME
RUN echo "RUST version $RUST_VERSION"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
RUN curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | RUSTUP_HOME=$RUSTUP_HOME CARGO_HOME=$CARGO_HOME sh -s -- -y --profile minimal --default-toolchain $RUST_VERSION
RUN echo '[source.crates-io]' > $CARGO_HOME/config.toml && \
    echo 'replace-with = "rsproxy-sparse"' >> $CARGO_HOME/config.toml && \
    echo '[source.rsproxy]' >> $CARGO_HOME/config.toml && \
    echo 'registry = "https://rsproxy.cn/crates.io-index"' >> $CARGO_HOME/config.toml && \
    echo '[source.rsproxy-sparse]' >> $CARGO_HOME/config.toml && \
    echo 'registry = "sparse+https://rsproxy.cn/index/"' >> $CARGO_HOME/config.toml && \
    echo '[registries.rsproxy]' >> $CARGO_HOME/config.toml && \
    echo 'index = "https://rsproxy.cn/crates.io-index"' >> $CARGO_HOME/config.toml && \
    echo '[net]' >> $CARGO_HOME/config.toml && \
    echo 'git-fetch-with-cli = true' >> $CARGO_HOME/config.toml
ENV PATH=$CARGO_HOME/bin:$PATH