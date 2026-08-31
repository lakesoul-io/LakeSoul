# Development Environment

This guide prepares a workstation for changing LakeSoul itself. It is different from the [local runtime quick start](../01-Getting%20Started/01-setup-local-env.md), which installs released connector artifacts for application users.

LakeSoul combines Rust, Java/Scala, Python, native JNI libraries, PostgreSQL, and optional object storage. Linux x86_64 GNU is the officially supported native development and release platform; see the [compatibility matrix](../01-Getting%20Started/04-compatibility.md).

## Choose an environment

| Approach | Provides | Use it when |
|---|---|---|
| Nix Flake | Pinned compiler support libraries, Java, Hadoop, PostgreSQL client, language servers, and all repository formatters | Developing on x86_64 Linux with Nix. This is the recommended reproducible shell. |
| Devenv services | Repository-configured PostgreSQL 14 and RustFS processes with persistent local state | Tests need metadata or S3-compatible object storage. It can be used with either the Nix shell or a manually prepared toolchain. |
| Manual toolchain | Tools installed by the operating system or a version manager | Nix is unavailable, or the developer needs an environment matching a specific deployment. |

The Flake shell and Devenv services are complementary: `nix develop` provides development tools; `devenv up` runs local services.

## Nix Flake

The repository Flake currently supports `x86_64-linux` and exposes three shells:

```bash
# Default native shell: Java 17, Hadoop, Clang/LLVM, PostgreSQL client,
# Rust tooling, language servers, and formatters
nix develop

# FHS-style shell with Java 11, useful for Maven/Spark/Flink work
nix develop .#fhs

# Minimal shell containing only repository formatters
nix develop .#formatter
```

The default and FHS shells configure `JAVA_HOME`, `HADOOP_HOME`, `HADOOP_CONF_DIR`, `CLASSPATH`, `LIBCLANG_PATH`, `LD_LIBRARY_PATH`, `MAVEN_OPTS`, and UTC timezone data.

The Flake is the source of truth for packages it provides. Component package managers are still used inside the shell: Cargo for Rust, Maven for JVM modules, `uv` for Python, and npm for the website. If a component command is not present in the selected shell, install that tool separately or add it to `flake.nix`; do not silently rely on a different system version in CI-sensitive work.

Run one command in the formatter shell without entering an interactive shell:

```bash
nix develop .#formatter --command treefmt --ci -- path/to/changed-file
```

`flake.lock` pins the Nix inputs. Do not update it as a side effect of entering the shell.

## Devenv: PostgreSQL and RustFS

[`devenv.nix`](https://github.com/lakesoul-io/LakeSoul/blob/main/devenv.nix) defines the local services used by metadata and object-store tests:

- PostgreSQL 14 on `127.0.0.1:5432`;
- database, user, and password: `lakesoul_test`;
- LakeSoul schema initialized from `script/meta_init.sql`;
- RustFS API on `127.0.0.1:9000` and console on `127.0.0.1:9001`;
- RustFS access key and secret key: `rustfsadmin`.

Inspect the evaluated configuration:

```bash
devenv info
```

Start both services in one foreground process:

```bash
devenv up
```

Keep that terminal open. Stop the services with `Ctrl-C`. Service data is persisted under `.devenv/state`, so initialization scripts apply when a fresh service state is created; use the repository metadata migration tools for an existing state.

In another terminal, configure LakeSoul clients:

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

Verify PostgreSQL and the initialized schema:

```bash
PGPASSWORD=lakesoul_test psql \
  -h 127.0.0.1 -p 5432 -U lakesoul_test -d lakesoul_test \
  -c '\dt'
```

Verify RustFS:

```bash
curl -fsS http://127.0.0.1:9000/health
```

Devenv starts the storage service but application tests may still need to create their expected bucket.

## Manual prerequisites

For a non-Nix setup, install:

| Tool | Required baseline or purpose |
|---|---|
| Linux x86_64 GNU | Official native build platform |
| Rust stable | Pinned by `rust-toolchain.toml`; includes rustfmt, Clippy, and rust-analyzer |
| `protoc` 23.x | Rust and Python protobuf generation; matches CI |
| JDK 11 | Maven, Spark, and Flink build baseline |
| Maven | JVM multi-module build |
| PostgreSQL 14+ and `psql` | Metadata service and integration tests |
| Python 3.10+ | LakeSoul Python SDK |
| `uv` and Maturin | Python dependency management and PyO3 extension builds |
| Node.js 18+ and npm | Docusaurus website |
| Clang/LLVM and `pkg-config` | Native compilation and bindings |
| `treefmt` plus configured formatters | Repository-wide formatting |
| Lefthook | Local pre-commit and pre-push hooks |

Check the core commands before building:

```bash
rustc --version
cargo --version
protoc --version
java -version
mvn --version
python --version
uv --version
node --version
npm --version
psql --version
```

Continue with [Build from source](02-build-from-source.md), then [Testing and quality checks](03-testing-and-quality.md).
