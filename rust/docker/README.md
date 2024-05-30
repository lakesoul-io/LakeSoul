# Docker image builder for rust cross build

The images are based on manylinux2014 with Cargo and JDK installed.

## X86_64
```shell
DOCKER_BUILDKIT=1 docker build --network host -t dmetasoul/lakesoul-cross:v1.0.0 -f docker/cross.Dockerfile docker
```

## AArch64
```shell
DOCKER_BUILDKIT=1 docker build --network host --build-arg BASE_IMG=quay.io/pypa/manylinux2014_aarch64:latest -t dmetasoul/lakesoul-cross:v1.0.0-aarch64 -f docker/cross.Dockerfile docker
```

And build LakeSoul packages for aarch64:

```shell
# Add rust aarch64 toolchain on host
rustup target add aarch64-unknown-linux-gnu

# build native libs
cd rust
# If host is x86_64, this build will run under qemu virtualization, and could be very slow
cross build --target aarch64-unknown-linux-gnu --release --features hdfs
cp target/aarch64-unknown-linux-gnu/release/liblakesoul_*.so target/release

# build lakesoul spark/flink packages
mvn package -pl lakesoul-flink -am -Pcross-build -DskipTests
```