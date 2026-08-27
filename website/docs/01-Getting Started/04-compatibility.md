# LakeSoul 4.0.0 Compatibility Matrix

This page defines the supported runtime and platform boundaries for LakeSoul Core `4.0.0`. “Release CI” identifies the exact baseline exercised by the `4.0.0` release gates; it does not imply support for unlisted versions.

## Core runtimes

| Component | Supported baseline | Release CI baseline | Status | Notes |
|---|---|---|---|---|
| Spark | Spark `3.5.8`, Scala `2.12.15`, Java 11 | Spark `3.5.8`, Scala `2.12.15`, Temurin 11 | GA | Use `lakesoul-spark-3.5_2.12:4.0.0`. |
| Flink | Flink `1.20.0`, Scala `2.12`, Java 11 or later | Flink `1.20.0`, Temurin 11 | GA | Use `lakesoul-flink-1.20_2.12:4.0.0`. |
| Flink CDC | `3.5.0` | `3.5.0` | GA | Reusing a Flink CDC `3.0` savepoint is unsupported. |
| Presto | Presto `0.296`, Java 17 | Presto `0.296`, Temurin 17 | GA | Use `lakesoul-presto-0.296:4.0.0`. Replace the complete deployment; mixed connector versions are unsupported. |
| PostgreSQL | PostgreSQL 14 or later | PostgreSQL `14.5` | GA | Apply the versioned metadata migration before starting `4.0.0`. |

## Python compatibility

LakeSoul Python is released independently from LakeSoul Core. The Python release aligned with the Core `4.0.0` compatibility gates is `2.0.0`.

| Component | Supported range | Release CI baseline | Status |
|---|---|---|---|
| LakeSoul Python | `2.0.0` | `2.0.0` | Independent release |
| Python | `>=3.10` | `3.10` | GA |
| Official wheel platform | Linux x86_64, manylinux2014 | `x86_64-unknown-linux-gnu` | GA |
| PyArrow | `>=16,<21` | Resolved from the locked Python release environment | GA |
| PySpark extra | `3.5.8` | `3.5.8` | GA |
| Ray extra | `>=2.55,<2.56` | Resolved from the locked Python release environment | Optional |
| Daft extra | `>=0.7.15` | Resolved from the locked Python release environment | Optional |

A Core tag does not publish Python, and a `py-vX.Y.Z` tag does not publish Core artifacts or change the website’s stable Core version.

## Native platform support

| Operating system | Architecture | ABI/target | Build | Native smoke test | Connector E2E | Support level |
|---|---|---|---:|---:|---:|---|
| Linux GNU | x86_64 | `x86_64-unknown-linux-gnu` | Required | Required | Required | GA Production |
| Linux GNU | aarch64 | — | No | No | No | Unsupported |
| Linux musl | Any | — | No | No | No | Unsupported |
| macOS | Any | — | No | No | No | Unsupported |
| Windows | Any | — | No | No | No | Unsupported |
| Any 32-bit OS | Any | — | No | No | No | Unsupported |

Official connector JARs embed Linux x86_64 GNU native libraries. LakeSoul `4.0.0` does not publish official native artifacts or provide an official source-build support contract for the unsupported platforms above.

## File and upgrade compatibility

| Capability | `4.0.0` support | Compatibility boundary |
|---|---|---|
| Read legacy Parquet | Supported | Covered by release compatibility gates. |
| Read Vortex | Supported | Uses Vortex's standard writer strategy; files use the `.vortex` extension. |
| Read Vortex Compact | Supported | Uses the compact strategy; files also use the `.vortex` extension. |
| Mixed-format snapshot | Supported | One snapshot may contain Parquet and either Vortex write strategy. |
| Default writes | Vortex Compact | LakeSoul `3.x` cannot read the resulting Vortex files. |
| Upgrade from `3.0.0` | Supported cold upgrade | Stop all processes, restore-test a consistent metadata/data backup pair, migrate metadata, and replace all runtimes together. |
| Direct upgrade from `2.x` | Unsupported | Upgrade through the verified `3.0.0` baseline. |
| Mixed `3.x`/`4.0.0` operation | Unsupported | Do not mix writers, readers, connector JARs, or native libraries. |
| In-place rollback after a Vortex commit | Unsupported | Restore PostgreSQL metadata and table data from the same quiesced pre-upgrade backup. |
| Flink CDC `3.0` savepoint reuse | Unsupported | Use source retention or an independently verified replay/backfill procedure. |

See [Physical File Formats](05-physical-file-formats.md) for the differences between `parquet`, `vortex`, and `vortex-compact`, format selection in each writer, and the Vortex rollback boundary.

## Gluten Preview

| LakeSoul | Spark | Scala | Gluten | Platform | Distribution | Support level |
|---|---|---|---|---|---|---|
| `4.0.0` | `3.5.8` | `2.12` | `1.6.0` | Linux x86_64 GNU | GitHub Release JAR only | Preview |

The Gluten artifact is `lakesoul-spark-gluten-3.5_2.12-4.0.0.jar`. It is not published to Maven Central and does not block Core GA. Preview status means its dependency chain and production support contract are not yet at GA level.

See the [4.0.0 release notes](https://github.com/lakesoul-io/LakeSoul/blob/v4.0.0/docs/release/release-4.0.0.md) and [upgrade and recovery guide](https://github.com/lakesoul-io/LakeSoul/blob/v4.0.0/docs/release/upgrade-4.0.0.md) before upgrading.
