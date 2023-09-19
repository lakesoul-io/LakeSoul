FROM quay.io/pypa/manylinux2014_x86_64

RUN yum update -y &&                        \
    yum install -y devtoolset-11-gcc-c++ && \
    yum clean packages

ENV PATH=/opt/rh/devtoolset-11/root/usr/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/rh/devtoolset-11/root/usr/lib64:/opt/rh/devtoolset-11/root/usr/lib:/opt/rh/devtoolset-11/root/usr/lib64/dyninst:/opt/rh/devtoolset-11/root/usr/lib/dyninst:${LD_LIBRARY_PATH}
ENV DEVTOOLSET_ROOTPATH=/opt/rh/devtoolset-11/root

ARG PIP_MIRROR="https://pypi.tuna.tsinghua.edu.cn/simple/"
ARG PIP_EXTRA_MIRRORS="https://pypi.org/simple/"
RUN python3.8 -m pip config set global.index-url "${PIP_MIRROR}" &&                         \
    python3.8 -m pip config set global.extra-index-url "${PIP_MIRROR} ${PIP_EXTRA_MIRRORS}"

ARG NINJA_VERSION=1.11.1
RUN rm -f ninja-linux.zip &&                                                                               \
    rm -f /usr/bin/ninja &&                                                                                \
    curl -L -O https://github.com/ninja-build/ninja/releases/download/v${NINJA_VERSION}/ninja-linux.zip && \
    unzip ninja-linux.zip -d /usr/bin &&                                                                   \
    rm -f ninja-linux.zip &&                                                                               \
    echo OK: Ninja

ARG RUST_VERSION=nightly
RUN rm -f rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz &&                                          \
    rm -rf rust-${RUST_VERSION}-x86_64-unknown-linux-gnu &&                                                \
    rm -rf /usr/local/rust-${RUST_VERSION} &&                                                              \
    curl -L -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz &&   \
    tar -xf rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz &&                                        \
    ./rust-${RUST_VERSION}-x86_64-unknown-linux-gnu/install.sh --prefix=/usr/local/rust-${RUST_VERSION} && \
    rm -f rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz &&                                          \
    rm -rf rust-${RUST_VERSION}-x86_64-unknown-linux-gnu &&                                                \
    echo OK: Rust

ENV PATH=/usr/local/rust-${RUST_VERSION}/bin:${PATH}
