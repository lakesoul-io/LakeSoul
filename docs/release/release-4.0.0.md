# LakeSoul 4.0.0 Release Guide

This document records the concrete versions, Maven coordinates, compatibility requirements, and implementation plan for the `4.0.0` release. The version-independent release policy and process live in [`release-guide.md`](release-guide.md); operator procedures live in [`upgrade-4.0.0.md`](upgrade-4.0.0.md).

> Current status: This document describes the release scheme to be adopted starting with `4.0.0`. The `v4.0.0` tag must not be created or published until every release gate succeeds on the final release commit and both protected publication environments are configured.

## 1. Target Versions

### 1.1 LakeSoul Core

The next official Core release is:

```text
4.0.0
```

| Stage | Maven | Rust Core | Git tag |
|---|---|---|---|
| Development | `4.0.0-SNAPSHOT` | `4.0.0-dev.0` | None |
| Final release | `4.0.0` | `4.0.0` | `v4.0.0` |
| `4.0.0` maintenance branch after release | `4.0.1-SNAPSHOT` | `4.0.1-dev.0` | None |
| `main` after the `4.0` branch is created | `4.1.0-SNAPSHOT` | `4.1.0-dev.0` | None |

The Core release branch is `release/4.0`.

### 1.2 Python

```text
Development version: 2.0.0.dev0
Final version:       2.0.0
Git tag:             py-v2.0.0
```

| Python | Cargo extension |
|---|---|
| `2.0.0.dev0` | `2.0.0-dev.0` |
| `2.0.0` | `2.0.0` |

## 2. Why `4.0.0` Is a Major Release

The main reasons for using a major version for `4.0.0` include default Vortex Compact writes, metadata Arrow schema changes, native ABI changes, the Spark 3.5 baseline, the Flink CDC upgrade, new Presto and Java baselines, and user-visible semantic changes.

## 3. Maven Coordinates

Starting with `4.0.0`, Maven `<version>` represents only the LakeSoul Core version. External runtime and Scala binary versions are encoded in the `artifactId`.

### 3.1 Maven Central GA

```text
com.dmetasoul:lakesoul-parent:4.0.0
com.dmetasoul:lakesoul-common:4.0.0
com.dmetasoul:lakesoul-io-java:4.0.0
com.dmetasoul:lakesoul-spark-3.5_2.12:4.0.0
com.dmetasoul:lakesoul-flink-1.20_2.12:4.0.0
com.dmetasoul:lakesoul-presto-0.296:4.0.0
```

Supported baselines:

| Artifact | Compatibility baseline |
|---|---|
| `lakesoul-spark-3.5_2.12` | Spark 3.5.8, Scala 2.12.15 |
| `lakesoul-flink-1.20_2.12` | Flink 1.20.0, Flink CDC 3.5.0, Scala 2.12, Java 11+ |
| `lakesoul-presto-0.296` | Presto 0.296, Java 17 |

### 3.2 Gluten Preview

The Gluten artifact uses:

```text
com.dmetasoul:lakesoul-spark-gluten-3.5_2.12:4.0.0
```

It is a GitHub Release Preview in `4.0.0`:

- it may be distributed as a GitHub Release JAR;
- it is not published to Maven Central;
- it does not block Core GA;
- release notes must identify Spark 3.5.8, Scala 2.12, Gluten 1.6.0, and platform limitations;
- it may be promoted to Maven Central GA only after Gluten dependencies can be resolved publicly and reproducibly.

### 3.3 Migration from Old Coordinates

Examples of old coordinates:

```text
com.dmetasoul:lakesoul-spark:3.5-3.0.0
com.dmetasoul:lakesoul-flink:1.20-3.0.0
com.dmetasoul:lakesoul-presto:0.29-3.0.0
```

`4.0.0` publishes only the new coordinates. It does not maintain two complete sets of JARs long term and does not require relocation artifacts. The upgrade guide must provide a clear mapping between old and new coordinates.

## 4. GitHub Release Assets

The GitHub Release provides shaded artifacts that users can deploy directly:

```text
lakesoul-spark-3.5_2.12-4.0.0.jar
lakesoul-flink-1.20_2.12-4.0.0.jar
lakesoul-presto-0.296-4.0.0.jar
lakesoul-spark-gluten-3.5_2.12-4.0.0.jar  # Preview
lakesoul-4.0.0-src.tar.gz
SHA256SUMS
SHA256SUMS.asc
SBOM.spdx.json
```

By default, `lakesoul-common` and `lakesoul-io-java` are available only through Maven Central and are not duplicated as GitHub Release assets.

Rust native libraries are embedded in JVM artifacts. They are not published independently to crates.io or as official standalone release artifacts.

## 5. Native Platform Support

`4.0.0` supports only Linux x86_64:

| Platform | Platform ID | Rust target | Build | Native smoke test | Connector E2E | Support level |
|---|---|---|---:|---:|---:|---|
| Linux x86_64 | `linux-x86_64` | `x86_64-unknown-linux-gnu` | Required | Required | Required | GA Production |

All other operating systems and CPU architectures are unsupported, including Linux aarch64, Linux musl, macOS, Windows, and 32-bit platforms.

## 6. Compatibility Requirements Specific to `4.0.0`

At minimum, the following work must be completed before publishing `4.0.0`.

### 6.1 File Formats

- Clearly document Vortex Compact as the default physical format.
- Verify reads of old Parquet, Vortex, and mixed snapshots.
- State clearly that `3.0.x` cannot read Vortex and that writing the first Vortex Compact file is a point of no return for in-place rollback.
- Provide a configuration that explicitly forces Parquet before that point during the upgrade window.
- Do not describe rewriting Vortex to Parquet or restoring a backup as in-place rollback.
- Mark this as a major migration risk in the release notes.

### 6.2 Metadata

- Provide an idempotent schema migration.
- Use an explicit schema version or migration record for migration statements.
- Validate the sequence of applying DDL before deploying binaries.
- Treat the first incompatible metadata migration as a point of no return for in-place rollback, and do not claim old-binary compatibility beyond that point.
- Fix the issue where a missing secondary PostgreSQL URL incorrectly falls back to the default local database.
- Document connection pool and replica identity changes.

### 6.3 Rollback and Recovery

- In-place rollback from `4.0.0` to `3.0.x` is unsupported after Vortex Compact files are written or incompatible metadata changes are applied.
- Document the exact points of no return before the upgrade begins.
- Require verified backups of both table data and PostgreSQL metadata before crossing either point of no return.
- Provide and test a recovery procedure that restores both table data and PostgreSQL metadata from the same pre-upgrade backup.
- Document forward fixes on `4.0.x` as the alternative when backup restoration is not selected.
- Do not claim support for mixed-version rolling rollback.

### 6.4 Native ABI

- Publish the Java JAR and native libraries as a matching set.
- Do not support mixing old and new JARs or native libraries.
- Include artifact and native build versions in loader errors.
- Run a real native loading smoke test on Linux x86_64.
- Complete the `META-INF/native/linux-x86_64/` resource layout before release.

### 6.5 Spark, Flink, and Presto

- The Spark upgrade guide must document Spark 3.5 and the new Maven coordinates.
- Document the semantic changes to range-partitioned overwrite.
- Test savepoint recovery from Flink CDC 3.0 to 3.5. Do not claim compatible recovery until it has been verified.
- The Presto upgrade guide must document Presto 0.296, Java 17, and changes to timestamp and name matching behavior.
- Do not promise an untested rolling upgrade with mixed Presto coordinator and worker versions.

## 7. Release Gates Specific to `4.0.0`

In addition to the standard gates in [`release-guide.md`](release-guide.md#83-release-pr-gates), the `4.0.0` release requires:

- the Core-related subset of Python compatibility tests;
- build both native libraries for `x86_64-unknown-linux-gnu`;
- run native loader smoke tests on Linux x86_64;
- run Spark, Flink, and Presto E2E tests on Linux x86_64;
- run metadata migration tests;
- run Parquet/Vortex compatibility and backup recovery tests, including restoration of table data and PostgreSQL metadata;
- verify both native libraries under `META-INF/native/linux-x86_64/`.

## 8. `4.0.0` Release Automation Implementation Order

Implement the following phases before the official release to avoid rewriting all CI at once.

### Phase 1: Versions and Documentation

- Adopt this release guide.
- Implement `script/release.py` set and check operations.
- Update Maven artifact IDs.
- Apply the versioning policy and `publish = false` to Core Rust crates.
- Mark Flight and S3 Proxy as Experimental in the documentation.
- Add compatibility and migration documentation.

### Phase 2: Linux x86_64 Native Build

- Refactor the native resource layout and Java loader for Linux x86_64.
- Build both native libraries for `x86_64-unknown-linux-gnu`.
- Use `rust/target` consistently.
- Select the Linux x86_64 runner and Rust target explicitly.
- Add native loader smoke tests on Linux x86_64.
- Verify both native libraries under `META-INF/native/linux-x86_64/`.

### Phase 3: Release Dry Run

- Extract a reusable release build workflow.
- Use `publish = false` for release PRs.
- Fix the Presto JDK 17 build.
- Add version, artifact, migration, format, and E2E gates.
- Generate checksums, a source archive, and an SBOM.

### Phase 4: Publish

- Trigger the official workflow from the final tag.
- Configure protected GitHub environments.
- Publish to Maven Central.
- Create the GitHub Release automatically.
- Upload assets automatically.
- Publish the website.
- Add post-release registry smoke tests.

## 9. Known Gaps in the Current Release Pipeline

The following issues must be confirmed and fixed before publishing `4.0.0`:

- the current Maven deployment workflow does not validate the tag against the POM version;
- the current workflow can be triggered with `workflow_dispatch` without a version input;
- the native artifact download directory does not match the directory read by the Maven profile;
- Presto requires Java 17, while the existing deployment reactor uses Java 11;
- the existing publish command skips tests;
- GitHub Release creation and JAR uploads are not automated;
- website publication must be decoupled from the Core and Python tag trigger boundaries;
- Gluten does not yet have the public, reproducible dependency chain required for Maven Central GA.

The `v4.0.0` tag must not be created or published until the issues above are resolved.
