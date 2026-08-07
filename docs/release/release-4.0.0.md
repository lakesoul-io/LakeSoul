# LakeSoul 4.0.0 Release Guide

This document defines the target versioning strategy, release artifacts, supported platforms, and release process for the LakeSoul monorepo.

> Current status: This document describes the release scheme to be adopted starting with `4.0.0`. The version synchronization scripts, multi-architecture native packaging, and GitHub Actions release gates described here must still be implemented incrementally. Until that work is complete, the existing `deployment.yml` must not be used to publish `4.0.0` directly.

## 1. Release Principles

LakeSoul uses a layered release model instead of requiring every component in the monorepo to share the same version and release cycle.

The basic principles are:

1. LakeSoul Core uses a unified version.
2. Python has an independent version and tag.
3. Rust crates are not published to crates.io. Core crates follow the Core version so that native ABI and build provenance can be traced.
4. Spark, Flink, Presto, and Scala versions are runtime compatibility dimensions. They are encoded in the Maven `artifactId` and are no longer mixed into the LakeSoul `<version>`.
5. Flight and S3 Proxy are currently Experimental components and are not part of the official Core release.
6. A release PR is the release candidate; RC versions and RC tags are not mandatory.
7. An official release may only be triggered by a signed final tag and must be approved through a protected environment before publication.
8. Build, test, and publish are separate stages. No `workflow_dispatch` invocation may bypass the final tag and publish a production release directly.

## 2. Release Domains and Versions

### 2.1 LakeSoul Core

LakeSoul Core includes:

- the Maven parent and shared JVM modules;
- the Java native bridge;
- the Spark, Flink, and Presto connectors;
- the Rust IO, metadata, and FFI implementations embedded in the connectors;
- Core release notes, upgrade documentation, and the website's latest stable version.

The next official Core release is:

```text
4.0.0
```

The corresponding version formats are:

| Stage | Maven | Rust Core | Git tag |
|---|---|---|---|
| Development | `4.0.0-SNAPSHOT` | `4.0.0-dev.0` | None |
| Final release | `4.0.0` | `4.0.0` | `v4.0.0` |
| `4.0.0` maintenance branch after release | `4.0.1-SNAPSHOT` | `4.0.1-dev.0` | None |
| `main` after the `4.0` branch is created | `4.1.0-SNAPSHOT` | `4.1.0-dev.0` | None |

The Core development version is represented by `<revision>` in the root `pom.xml`. Core Rust manifests must map it to the equivalent Cargo SemVer. CI must verify that both sides are consistent; changing only one side is not allowed.

The website version represents the latest stable release, not the current development version. While `4.0.0-SNAPSHOT` is under development, the website must continue to show the previous final release until `v4.0.0` is published.

### 2.2 Python

Python uses an independent version and release cycle:

```text
Development version: 2.0.0.dev0
Final version:       2.0.0
Git tag:             py-v2.0.0
```

The authoritative Python version is defined in `python/pyproject.toml`. The extension crate version in `python/Cargo.toml` must map to the equivalent Cargo SemVer:

| Python | Cargo extension |
|---|---|
| `2.0.0.dev0` | `2.0.0-dev.0` |
| `2.0.0` | `2.0.0` |

Python release notes must record:

- the compatible LakeSoul Core version range;
- the Git commit SHA used for the build;
- the supported Python, PyArrow, and platform matrix.

A Core tag must not publish Python. A Python tag must not trigger a Core Maven release or modify the website's latest stable Core version.

### 2.3 Rust Crates

LakeSoul Rust crates are not currently published to crates.io.

Requirements:

- crates that are not externally published must explicitly set `publish = false`;
- Core Rust crates follow the Core version to identify the source, native ABI, and dynamic libraries embedded in JARs;
- the Python extension crate follows the Python version;
- Flight and S3 Proxy are not required to follow the Core version until they have independent formal release policies;
- release workflows must not run `cargo publish`.

### 2.4 Experimental Components

The following components are not currently part of the official Core release:

- `lakesoul-flight`
- `lakesoul-s3-proxy`

They must not:

- block the `v4.0.0` Core release;
- be included as official `v4.0.0` GitHub Release assets;
- publish Docker images automatically in response to a Core tag;
- imply the same level of production support as the Core connectors.

Related documentation and deployment manifests must mark them clearly as `Experimental`. If they become production components, a separate decision will determine whether they follow the Core version or use independent service versions.

## 3. SemVer Rules

LakeSoul compatibility covers more than source APIs. It also includes file formats, the metadata schema, protocols, native ABI, and runtime baselines.

### 3.1 Major: `X.0.0`

Increase the major version when any of the following occurs:

- an older version cannot read persistent files written by the new version's default configuration;
- the metadata schema requires an irreversible migration, or a safe rollback is no longer possible after upgrading;
- public Java, Scala, or Python APIs are removed or their semantics change incompatibly;
- the JNI, JNR, or C ABI becomes incompatible;
- the minimum supported Spark, Flink, Presto, or Java runtime changes incompatibly;
- SQL, overwrite, CDC, or configuration defaults change in a breaking way;
- users must change dependency coordinates, code, configuration, or deployment topology when upgrading.

The main reasons for using a major version for `4.0.0` include default Vortex Compact writes, native ABI changes, the Spark 3.5 baseline, the Flink CDC upgrade, new Presto and Java baselines, and user-visible semantic changes.

### 3.2 Minor: `X.Y.0`

Use a minor version for backward-compatible functionality, such as:

- new APIs or optional configuration;
- new explicitly opt-in file format capabilities;
- additive and compatible metadata migrations;
- a new engine artifact published alongside existing artifacts without removing them;
- performance and capability improvements that do not prevent rollback.

### 3.3 Patch: `X.Y.Z`

Use a patch version for:

- bug fixes;
- performance improvements that do not change public semantics;
- compatible dependency security updates;
- CI, packaging, signing, and documentation fixes;
- fixes that remain fully compatible with existing file formats, metadata, and ABI.

## 4. Maven Coordinates

Starting with `4.0.0`, Maven `<version>` represents only the LakeSoul Core version. External runtime and Scala binary versions are encoded in the `artifactId`.

### 4.1 Maven Central GA

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

The `artifactId` uses the runtime compatibility series. The release compatibility matrix records the exact patch versions verified by CI.

### 4.2 Gluten Preview

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

### 4.3 Migration from Old Coordinates

Examples of old coordinates:

```text
com.dmetasoul:lakesoul-spark:3.5-3.0.0
com.dmetasoul:lakesoul-flink:1.20-3.0.0
com.dmetasoul:lakesoul-presto:0.29-3.0.0
```

`4.0.0` publishes only the new coordinates. It does not maintain two complete sets of JARs long term and does not require relocation artifacts. The upgrade guide must provide a clear mapping between old and new coordinates.

## 5. Official Release Artifacts

### 5.1 Maven Central

Maven Central publishes the GA artifacts listed in Section 4.1 and all files required by Central:

- the main artifact;
- POM;
- sources JAR;
- Javadoc JAR;
- GPG signatures;
- checksums.

### 5.2 GitHub Release

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

## 6. Native Platforms and Packaging

### 6.1 Resource Layout

A single JAR must support multiple operating systems and CPU architectures. Native resources use an architecture-aware layout:

```text
META-INF/native/
├── linux-x86_64/
│   ├── liblakesoul_io_c.so
│   └── liblakesoul_metadata_c.so
├── linux-aarch64/
│   ├── liblakesoul_io_c.so
│   └── liblakesoul_metadata_c.so
├── macos-x86_64/
│   ├── liblakesoul_io_c.dylib
│   └── liblakesoul_metadata_c.dylib
├── macos-aarch64/
│   ├── liblakesoul_io_c.dylib
│   └── liblakesoul_metadata_c.dylib
└── windows-x86_64/
    ├── lakesoul_io_c.dll
    └── lakesoul_metadata_c.dll
```

The Java loader selects a resource using normalized `os.name` and `os.arch` values. Error messages must include:

- the original OS and architecture;
- the normalized platform ID;
- the expected resource path;
- the current artifact version;
- the list of supported platforms.

Platform-specific Spark, Flink, or Presto connector JARs are not published.

### 6.2 Support Matrix

| Platform | Build | Native smoke test | Connector E2E | Support level |
|---|---:|---:|---:|---|
| Linux x86_64 | Required | Required | Required | GA Production |
| Linux aarch64 | Required | Required | May be added later | GA Native |
| macOS x86_64 | Required | Required | Not required | Developer |
| macOS aarch64 | Required | Required | Not required | Developer |
| Windows x86_64 | Required | Required | Not required | Developer |

`4.0.0` does not guarantee support for:

- Windows ARM64;
- Linux musl;
- 32-bit platforms;
- CPU architectures not listed above.

Workflows must explicitly specify the runner architecture and Rust target triple. The target architecture must not be inferred from floating labels such as `macos-latest`.

## 7. Branches and Tags

### 7.1 Branches

Core minor releases use:

```text
release/<major>.<minor>
```

For example:

```text
release/4.0
```

This branch maintains the entire `4.0.x` series, including `4.0.0`, `4.0.1`, and subsequent patches.

After `release/4.0` is created:

- the release branch accepts only release blockers, compatibility fixes, documentation, and packaging fixes;
- `main` advances to `4.1.0-SNAPSHOT`;
- fixes that also affect future versions should be merged into `main` first and then cherry-picked to `release/4.0`;
- fixes specific to release machinery may be committed directly to the release branch, but the PR must explain why.

### 7.2 Tags

Official Core tags:

```text
v4.0.0
v4.0.1
```

Official Python tags:

```text
py-v2.0.0
```

Tags must be:

- annotated;
- signed;
- immutable;
- created from the release commit on the corresponding `release/<major>.<minor>` branch for Core releases;
- fully consistent with the build metadata version.

An official tag must never be deleted and recreated with the same name.

Do not use:

```text
4.0.0
release-4.0.0
v4.0
latest
```

### 7.3 RC Releases Are Not Mandatory

The default release process does not create:

```text
4.0.0-rc.1
v4.0.0-rc.1
```

The release PR and its complete dry run serve as the release candidate. Maintainers may decide to publish an RC for a specific release only when explicit external acceptance, a community vote, or public migration testing is required.

## 8. Core Release Process

### 8.1 Preparation and Feature Freeze

1. Confirm the target version and release scope.
2. Complete the compatibility audit.
3. Confirm that every release blocker has an owner and status.
4. Create `release/4.0` from a `main` commit that has passed the full CI suite.
5. Advance `main` to `4.1.0-SNAPSHOT` / `4.1.0-dev.0`.
6. Create a release PR on the release branch that changes the development versions to `4.0.0`.

### 8.2 Release PR

The release PR is the internal candidate and must include:

- final Maven and Rust Core versions;
- new Maven coordinates;
- release notes;
- a compatibility matrix;
- metadata migration instructions;
- an upgrade guide;
- a rollback guide;
- a mapping between old and new Maven coordinates;
- website release content. The latest stable version must change only after the official release succeeds, or atomically through the publish workflow.

The release PR runs the same reusable build workflow as the official release, with:

```text
publish = false
```

Candidate artifacts are retained only as expiring GitHub Actions artifacts. No public RC Release is created.

### 8.3 Release PR Gates

The following checks are required:

- Core version consistency;
- effective Maven coordinates;
- `cargo fmt --all --check`;
- `cargo clippy`;
- Rust tests;
- Maven unit and integration tests;
- the Core-related subset of Python compatibility tests;
- native builds on all five platforms;
- native loader smoke tests on all five platforms;
- Spark, Flink, and Presto E2E on Linux x86_64;
- native IO and metadata smoke tests on Linux aarch64;
- metadata migration tests;
- Parquet/Vortex compatibility and rollback tests;
- JAR content verification;
- Maven sources, Javadocs, and signing dry run;
- checksum and SBOM generation;
- license, header, and dependency policy checks.

A release must not bypass a failing gate. A known issue that is genuinely unrelated to the release and cannot be fixed in time must be documented in the release notes, and a maintainer must explicitly approve the exception.

### 8.4 Final Tag and Publication

After the release PR is merged, the release manager creates a signed tag:

```text
v4.0.0
```

The tag workflow must:

1. validate the tag format;
2. verify that the tagged commit belongs to `release/4.0`;
3. verify that the Maven, Rust, and tag versions match exactly;
4. reject `SNAPSHOT`, `dev`, and unexpected prerelease versions;
5. rerun the release build and all release gates;
6. build immutable final artifacts;
7. enter a protected GitHub Environment and wait for maintainer approval;
8. publish to Maven Central after approval;
9. create the GitHub Release and upload its assets;
10. publish the website and `4.0.0` release notes;
11. report registry URLs, artifact checksums, and the commit SHA.

Recommended protected environments:

```text
maven-central
website-production
```

Production publication may only be triggered by a final tag. `workflow_dispatch` may run a dry run or retry safe build steps, but it must not publish a production release from an arbitrary branch or commit.

### 8.5 Post-release Tasks

1. Verify that all coordinates resolve from Maven Central.
2. Download the GitHub Release JARs and verify them against `SHA256SUMS`.
3. Run at least one Spark, Flink, and Presto installation smoke test in a clean environment.
4. Advance `release/4.0` to `4.0.1-SNAPSHOT` / `4.0.1-dev.0`.
5. Confirm that `main` is at `4.1.0-SNAPSHOT` / `4.1.0-dev.0`.
6. Synchronize required release documentation and fixes back to `main`.
7. Announce the release, upgrade constraints, and known issues.

## 9. Patch Releases

Patch releases are published from the corresponding minor release branch:

```text
release/4.0
```

Process:

1. Merge the fix into `main` first.
2. Cherry-pick it to `release/4.0`.
3. Create a release PR that changes `4.0.1-SNAPSHOT` to `4.0.1`.
4. Run the same release dry run.
5. Create the signed tag `v4.0.1`.
6. Publish after approval through the protected environment.
7. Advance the release branch to `4.0.2-SNAPSHOT`.

Patch releases do not use RCs by default.

## 10. Python Release Process

Python releases are independent of Core:

1. Update `python/pyproject.toml` to the final version in a Python release PR.
2. Synchronize `python/Cargo.toml`.
3. Validate the mapping between the Python tag, PEP 440 version, and Cargo SemVer.
4. Run Python tests and wheel metadata validation.
5. Build wheels and an sdist for supported platforms.
6. Create a signed tag such as `py-v2.0.0`.
7. Publish through PyPI Trusted Publishing.
8. Advance to the next `.dev0` version after publication.
9. Do not trigger a Core Maven release, Core GitHub Release, or website Core stable update.

Python publication must use a protected environment and OIDC Trusted Publishing. Long-lived PyPI tokens must not be stored.

## 11. Version Synchronization Tool

Do not add another manually maintained root `VERSION` file. Implement a release tool such as:

```text
python script/release.py check
python script/release.py set-core 4.0.0-SNAPSHOT
python script/release.py set-core 4.0.0
python script/release.py set-python 2.0.0.dev0
python script/release.py check-tag v4.0.0
```

The tool must:

- synchronize Core Maven and Rust versions;
- synchronize the Python PEP 440 and Cargo extension versions;
- validate Maven artifact IDs and effective versions;
- validate the website's latest stable version against the official release state;
- validate tag and version consistency;
- reject unsupported version formats;
- provide a `--check` mode for CI;
- report the files it will modify instead of rewriting them silently.

## 12. Compatibility Requirements Specific to `4.0.0`

At minimum, the following work must be completed before publishing `4.0.0`.

### 12.1 File Formats

- Clearly document Vortex Compact as the default physical format.
- Verify reads of old Parquet, Vortex, and mixed snapshots.
- State clearly that `3.0.x` cannot read Vortex, so a direct binary rollback is impossible after Vortex files have been written.
- Provide a configuration that explicitly forces Parquet during the upgrade window.
- Provide a rollback procedure that rewrites Vortex to Parquet or restores a backup.
- Mark this as a major migration risk in the release notes.

### 12.2 Metadata

- Provide an idempotent schema migration.
- Use an explicit schema version or migration record for migration statements.
- Validate the sequence of applying DDL before deploying binaries.
- Verify that a migrated database can still be used by any old binary to which rollback is permitted.
- Fix the issue where a missing secondary PostgreSQL URL incorrectly falls back to the default local database.
- Document connection pool and replica identity changes.

### 12.3 Native ABI

- Publish the Java JAR and native libraries as a matching set.
- Do not support mixing old and new JARs or native libraries.
- Include artifact and native build versions in loader errors.
- Run a real native loading smoke test on all five platforms.
- Complete the architecture-aware resource layout before release.

### 12.4 Spark, Flink, and Presto

- The Spark upgrade guide must document Spark 3.5 and the new Maven coordinates.
- Document the semantic changes to range-partitioned overwrite.
- Test savepoint recovery from Flink CDC 3.0 to 3.5. Do not claim compatible recovery until it has been verified.
- The Presto upgrade guide must document Presto 0.296, Java 17, and changes to timestamp and name matching behavior.
- Do not promise an untested rolling upgrade with mixed Presto coordinator and worker versions.

## 13. Release Security and Traceability

An official release must provide:

- a signed, annotated Git tag;
- GPG-signed Maven artifacts;
- a signature for GitHub Release checksums;
- an SBOM;
- GitHub artifact provenance or attestation when available;
- GitHub Actions pinned to fixed versions or commit SHAs;
- no floating `master` or `latest` references for release toolchains;
- explicit records of the source commit, Rust toolchain, JDK, Maven, and platform target;
- registry credentials available only to protected publish jobs;
- OIDC Trusted Publishing for PyPI;
- long-term traceability of release artifact build logs and checksums.

## 14. `4.0.0` Release Automation Implementation Order

Implement the following phases before the official release to avoid rewriting all CI at once.

### Phase 1: Versions and Documentation

- Adopt this release guide.
- Implement `script/release.py` set and check operations.
- Update Maven artifact IDs.
- Apply the versioning policy and `publish = false` to Core Rust crates.
- Mark Flight and S3 Proxy as Experimental in the documentation.
- Add compatibility and migration documentation.

### Phase 2: Native Build

- Refactor the architecture-aware resource layout and Java loader.
- Establish a five-platform native matrix.
- Use `rust/target` consistently.
- Specify target triples and runner architectures explicitly.
- Add native loader smoke tests.
- Verify both native libraries for every platform in the JAR.

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

## 15. Known Gaps in the Current Release Pipeline

The following issues must be confirmed and fixed before publishing `4.0.0`:

- the current Maven deployment workflow does not validate the tag against the POM version;
- the current workflow can be triggered with `workflow_dispatch` without a version input;
- Windows and macOS native artifacts use target paths inconsistent with `.cargo/config.toml`;
- the native artifact download directory does not match the directory read by the Maven profile;
- Presto requires Java 17, while the existing deployment reactor uses Java 11;
- the existing publish command skips tests;
- the current native JAR resource layout does not support multiple architectures for one operating system;
- GitHub Release creation and JAR uploads are not automated;
- website publication must be decoupled from the Core and Python tag trigger boundaries;
- Gluten does not yet have the public, reproducible dependency chain required for Maven Central GA.

The `v4.0.0` tag must not be created or published until the issues above are resolved.
