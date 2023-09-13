FROM quay.io/pypa/manylinux2014_x86_64

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
