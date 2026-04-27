# CLAUDE.md — LakeSoul Codebase Guide

## Project Overview

LakeSoul is a cloud-native Lakehouse framework (LF AI & Data sandbox project, v3.0.0).
It provides ACID transactions, LSM-Tree style upserts, schema evolution, CDC ingestion,
time travel, and unified batch/streaming processing.

The codebase is split across two build systems:
- **Rust** (Cargo workspace) — native metadata client and IO/merge layer
- **Java/Scala** (Maven multi-module) — Spark, Flink, and Presto integrations, plus a Java JNI bridge to the Rust layer

---

## Repository Layout

```
LakeSoul/
├── rust/                        # Cargo workspace root (active members below)
│   ├── proto/                   # Protobuf definitions (entity.proto) + generated Rust types
│   ├── lakesoul-metadata/       # PostgreSQL metadata client (tokio-postgres, bb8 pool)
│   ├── lakesoul-metadata-c/     # C FFI shim over lakesoul-metadata (for Java JNI)
│   ├── lakesoul-io/             # Native IO: Arrow/DataFusion/Parquet read-merge-write
│   ├── lakesoul-io-c/           # C FFI shim over lakesoul-io (for Java JNI)
│   ├── lakesoul-datafusion/     # DataFusion SQL integration 
│   ├── lakesoul-flight/         # Arrow Flight server (workspace-disabled, WIP)
│   ├── lakesoul-s3-proxy/       # S3 proxy (workspace-disabled, WIP)
│   └── lakesoul-console/        # CLI console (workspace-disabled, WIP)
│
├── native-io/
│   └── lakesoul-io-java/        # Java JNI bridge (loads liblakesoul_io_c.so / .dylib)
│
├── lakesoul-common/             # Shared Java utilities (protobuf, config, etc.)
├── lakesoul-spark/              # Apache Spark 3.3 integration (Scala 2.12)
├── lakesoul-flink/              # Apache Flink integration (Java + Scala)
├── lakesoul-presto/             # Presto/Trino integration
├── lakesoul-spark-gluten/       # Spark + Gluten/Velox integration (optional Maven profile)
│
├── python/                      # Python bindings (maturin/PyO3, pyproject.toml)
│
├── script/
│   ├── meta_init.sql            # PostgreSQL schema DDL
│   ├── meta_init_for_local_test.sh
│   ├── meta_rbac_init.sql       # RBAC row-level security policies
│   └── meta_cleanup.sql
│
└── docker/                      # Docker-compose setups for local dev
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Metadata store | PostgreSQL 14+ (ACID, MVCC, RBAC row-level security) |
| Native IO/merge | Rust — DataFusion 51, Arrow 57, Parquet, object_store 0.12 |
| Async runtime | Tokio (full features) |
| gRPC | Tonic 0.14 |
| Java bridge | JNI via `lakesoul-io-c` / `lakesoul-metadata-c` (C FFI) |
| Spark | Apache Spark 3.3.1, Scala 2.12 |
| Flink | Apache Flink (Java + Scala) |
| Python | PyO3 + maturin, pyarrow ≥ 16 |
| Build (Rust) | Cargo stable toolchain + rustfmt + clippy |
| Build (JVM) | Maven, Java 8 target |

---

## Build Commands

### Prerequisites

1. **PostgreSQL 14+** running locally with a test database:
   ```sh
   ./script/meta_init_for_local_test.sh -j 1
   ```
   Default test credentials: user=`lakesoul_test`, password=`lakesoul_test`, db=`lakesoul_test`

2. **protoc** (Protocol Buffers compiler) — required for Rust `build.rs` code generation.

3. **JDK 11** (for Maven builds, despite Java 8 source/target compatibility).

---

### Rust

```sh
# Build all active workspace members
cargo build

# Build release
cargo build --release

# Run all tests (requires PostgreSQL)
RUST_BACKTRACE=full cargo test

# Run tests for a specific package
cargo test --package lakesoul-io
cargo test --package lakesoul-metadata

# Lint
cargo clippy
cargo fmt --check

# Build the C FFI libraries (used by Java JNI)
cargo build --release -p lakesoul-io-c
cargo build --release -p lakesoul-metadata-c
# Output: rust/target/release/liblakesoul_io_c.so (Linux) or .dylib (macOS)
```

The Rust toolchain is pinned to `stable` in `rust-toolchain.toml`.

---

### Java / Maven (Spark & Flink)

The Java build **requires** the Rust C FFI `.so`/`.dylib` files to be built first
and placed at `rust/target/release/`.

```sh
# Build everything (skip tests)
mvn -B clean package -DskipTests --file pom.xml

# Run Spark tests (subset 1)
mvn -B clean test -pl lakesoul-spark -am -Pcross-build -Pparallel-test \
    -Dtest='UpdateScalaSuite,ReadSuite,...' -Dsurefire.failIfNoSpecifiedTests=false

# Run Flink tests
mvn -B clean test -pl lakesoul-flink -am -Pcross-build

# Enable Gluten/Velox profile
mvn -B clean package -Pgluten -DskipTests

# Generate test report
mvn surefire-report:report-only -pl lakesoul-spark -am
```

---

### Python

The Python package uses [maturin](https://github.com/PyO3/maturin) to compile the
Rust extension and [uv](https://github.com/astral-sh/uv) for dependency management.

```sh
cd python

# Install dev dependencies
uv sync --group dev

# Build and install the Rust extension in-place (development mode)
uv run maturin develop

# Run tests
uv run pytest tests/

# Build a release wheel
uv run maturin build --release
```

---

## Key Architecture Concepts

### Metadata Layer (`rust/lakesoul-metadata`)
- All table/partition/data-commit metadata is stored in **PostgreSQL**.
- The `DaoType` enum in `src/lib.rs` enumerates every SQL operation (select, insert, update, delete).
- `execute_query`, `execute_insert`, `execute_update` are the three async entry points used by both Rust and the JNI bridge.
- Connection pooling via `bb8-postgres`; results cached with `cached` crate.
- RBAC uses PostgreSQL row-level security (`meta_rbac_init.sql`).

### IO Layer (`rust/lakesoul-io`)
- Implements vectorised **merge-on-read** for LSM-Tree style hash-partitioned tables.
- Built on **DataFusion** physical plan execution; custom `PhysicalPlan` nodes live in `src/physical_plan/`.
- Object store abstraction via `object_store` crate (S3, local, HDFS optional via `hdfs` feature).
- Local disk **data cache** implemented in `src/cache/`.
- `LakeSoulReader` (`src/reader.rs`) and async writer (`src/writer/`) are the primary public APIs.
- Parquet is the underlying file format.

### Java JNI Bridge (`native-io/lakesoul-io-java`)
- Loads native shared libraries at runtime from the classpath.
- Provides Java-callable wrappers for `LakeSoulReader`, `LakeSoulWriter`, and metadata operations.

### Spark Integration (`lakesoul-spark`)
- `LakeSoulSparkSessionExtension` registers custom rules and strategies.
- `LakeSoulTable` is the DataFrame/SQL API entry point (similar to Delta's `DeltaTable`).
- `SparkMetaVersion` bridges Spark to the metadata client via JNI.
- Custom ANTLR4 grammar extends Spark SQL with LakeSoul-specific syntax.
- Compaction service lives under `spark/compaction/`.

### Flink Integration (`lakesoul-flink`)
- Provides `TableSource` and `TableSink` for both batch and streaming.
- Supports Flink CDC with auto DDL sync and exactly-once semantics.

---

## Environment Variables / Configuration

| Variable | Purpose |
|---|---|
| `lakesoul.pg.url` | PostgreSQL JDBC URL for metadata |
| `RUST_BACKTRACE` | Set to `full` for detailed Rust panics in tests |
| `RUSTFLAGS` | Set to `-Awarnings` in CI to suppress warnings as errors |

Local config files:
- `lakesoul.properties` — default runtime properties
- `pg.property` — PostgreSQL connection details for local dev

---

## CI / GitHub Actions Workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `rust-ci.yml` | Push/PR to `rust/**` | Cargo test with PostgreSQL + MinIO services |
| `rust-clippy.yml` | Push/PR to `rust/**` | `cargo clippy` lint check |
| `maven-test.yml` | Push/PR (non-rust paths) | Builds Rust `.so`, then runs Spark + Flink Maven tests |
| `flink-cdc-test.yml` | Scheduled/PR | End-to-end Flink CDC tests |
| `python-ci.yml` | Push/PR to `python/**` | Python build + pytest |
| `native-build.yml` | Push/PR | Cross-platform native library builds |

---

## Contribution Guidelines

- Branch naming: `feature/<short-description>` or `bug/<short-description>`
- PR title format: `[Component] Description` (e.g., `[Flink] add table source implementation`)
- All changes should include tests.
- Keep diffs small and self-contained.
- Open an issue before large changes to discuss the approach.
- Tag issues with the relevant component (`spark`, `flink`, `rust`, `python`, etc.).

---

## Common Pitfalls

1. **Protoc not installed** — `cargo build` will fail with a codegen error. Install `protoc` first.
2. **Native libraries missing for Maven tests** — `lakesoul-io-java` needs the `.so`/`.dylib` on the classpath or at `rust/target/release/`. Build Rust first.
3. **PostgreSQL not running** — both Rust and JVM tests need a live PostgreSQL instance. Run `meta_init_for_local_test.sh` to initialize the schema.
4. **Workspace-disabled crates** — `lakesoul-datafusion`, `lakesoul-flight`, etc. are commented out in `Cargo.toml`. Add them back to the `members` list to build them.
5. **Python maturin build** — must run from the `python/` directory, not the repo root; the root `Cargo.toml` does not include the `python` crate in active members.

## Code Reading Rules

Output (focus on building a data-flow mental model, not file-by-file summaries):

1. Entry points
   - External APIs, public functions, or interfaces where data enters the system

2. Core data structures
   - Key structs/enums/objects and their roles in the data flow

3. Data flow path
   - End-to-end path: input → transformations → output
   - Highlight intermediate representations

4. State mutation points
   - Where and how state changes occur

5. Concurrency paths
   - Async tasks, threads, channels, or event-driven flows

6. Error flow
   - Where errors originate, how they propagate, and how they are handled

7. Call chains
   - Key function call sequences along the main data flow

8. Potential risks
   - Race conditions, stale state, resource leaks, or inconsistent state

Finally:
- Provide an ASCII data flow diagram
- Explicitly point out areas of uncertainty or assumptions