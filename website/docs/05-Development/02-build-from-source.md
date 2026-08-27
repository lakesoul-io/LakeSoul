# Build from Source

Run build commands from the repository root unless a section says otherwise. LakeSoul has one Rust workspace and one Maven multi-module build; the Python package compiles a PyO3 module from the same Rust sources.

## Build dependency flow

```text
Protobuf definitions
        |
        v
Rust metadata + NativeIO libraries
        |                  |
        |                  `----> Python PyO3 extension
        v
C ABI libraries (.so)
        |
        v
Java JNI bridge
        |
        +----> Spark connector
        +----> Flink connector
        `----> Presto connector
```

The JVM connectors load the native C ABI libraries. Build order matters: native libraries must exist before Maven packages or tests that load JNI.

## Rust workspace

The root `Cargo.toml` is the Cargo workspace entry point. The stable toolchain and required components are declared in `rust-toolchain.toml`.

Build active workspace members:

```bash
cargo -q build
```

Build the optimized C ABI libraries required by Java JNI:

```bash
cargo -q build --release \
  -p lakesoul-io-c \
  -p lakesoul-metadata-c
```

On Linux, the outputs are:

```text
rust/target/release/liblakesoul_io_c.so
rust/target/release/liblakesoul_metadata_c.so
```

A missing `protoc` normally fails during a crate build script before Rust compilation completes.

## JVM modules

Use JDK 11 for the Maven build. Confirm `JAVA_HOME` and the active JVM before starting:

```bash
java -version
mvn --version
```

After building the native libraries, build all Maven modules without tests:

```bash
mvn -q -B package -DskipTests --file pom.xml
```

Build one connector and its dependencies:

```bash
mvn -q -B package \
  -pl :lakesoul-spark-3.5_2.12 -am \
  -DskipTests --file pom.xml

mvn -q -B package \
  -pl :lakesoul-flink-1.20_2.12 -am \
  -DskipTests --file pom.xml

mvn -q -B package \
  -pl :lakesoul-presto-0.296 -am \
  -DskipTests --file pom.xml
```

`-pl` selects the module and `-am` also builds required reactor dependencies. Do not mix connector JARs and native libraries from different LakeSoul revisions.

## Python SDK

The Python project lives under `python/` and requires Python 3.10 or later.

Install the development dependency group and build the Rust extension into the virtual environment:

```bash
cd python
uv sync --group dev
uvx --from 'maturin>=1,<2' maturin develop
```

After `maturin develop`, imports resolve to the locally built extension:

```bash
uv run python -c 'import lakesoul; print(lakesoul.__file__)'
```

Re-run `uvx --from 'maturin>=1,<2' maturin develop` after changing Rust code exposed through PyO3. Pure Python changes under `python/src/` do not require a native rebuild in an editable development environment.

Build a release wheel:

```bash
uvx --from 'maturin>=1,<2' maturin build --release
```

The Python release and LakeSoul Core release have independent versions. See the [compatibility matrix](../01-Getting%20Started/04-compatibility.md) before combining a local Python build with connector artifacts.

## Website

The Docusaurus site lives under `website/` and requires Node.js 18 or later.

```bash
cd website
npm ci --omit-lockfile-registry-resolved true
npm run build
```

Start the development server for interactive documentation work:

```bash
npm run start
```

The production build renders both English and Chinese locales and fails on unresolved documentation links.

## Clean rebuilds

Prefer incremental builds during development. Use a clean build only when generated output or dependency resolution is suspected:

```bash
cargo -q clean
mvn -q -B clean package -DskipTests --file pom.xml
```

These commands remove build caches and can make the next build substantially slower. They do not reset PostgreSQL, RustFS, `.devenv/state`, or Python virtual environments.
