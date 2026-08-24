# LakeSoul Release Process

This document defines the versioning strategy, release artifacts, supported platforms, and release process for the LakeSoul monorepo. It is version-independent and applies to all future releases.

For the concrete versions, Maven coordinates, compatibility requirements, and implementation plan of a specific release, see the matching `release-<version>.md` (for example, [`release-4.0.0.md`](release-4.0.0.md)).

## 1. Release Principles

LakeSoul uses a layered release model instead of requiring every component in the monorepo to share the same version and release cycle.

The basic principles are:

1. LakeSoul Core uses a unified version.
2. Python has an independent version and tag.
3. Rust crates are not published to crates.io. Core crates follow the Core version so that native ABI and build provenance can be traced.
4. Spark, Flink, Presto, and Scala versions are runtime compatibility dimensions. They are encoded in the Maven `artifactId` and are no longer mixed into the LakeSoul `<version>`.
5. Flight and S3 Proxy are currently Experimental components and are not part of the official Core release.
6. A release PR is the release candidate; RC versions and RC tags are not mandatory.
7. An official release may only be triggered by a signed final tag after every release gate succeeds.
8. Build, test, and publish are separate stages. No `workflow_dispatch` invocation may bypass the final tag and publish a production release directly.

## 2. Release Domains and Versions

### 2.1 LakeSoul Core

LakeSoul Core includes:

- the Maven parent and shared JVM modules;
- the Java native bridge;
- the Spark, Flink, and Presto connectors;
- the Rust IO, metadata, and FFI implementations embedded in the connectors;
- Core release notes, upgrade documentation, and the website's latest stable version.

The Core version is a single unified version expressed in three forms:

| Stage | Maven | Rust Core | Git tag |
|---|---|---|---|
| Development | `<X.Y.Z>-SNAPSHOT` | `<X.Y.Z>-dev.0` | None |
| Final release | `<X.Y.Z>` | `<X.Y.Z>` | `v<X.Y.Z>` |
| `<X.Y.Z>` maintenance branch after release | `<X.Y.(Z+1)>-SNAPSHOT` | `<X.Y.(Z+1)>-dev.0` | None |
| `main` after the `<X.Y>` branch is created | `<X.(Y+1).0>-SNAPSHOT` | `<X.(Y+1).0>-dev.0` | None |

The Core development version is represented by `<revision>` in the root `pom.xml`. Core Rust manifests must map it to the equivalent Cargo SemVer. CI must verify that both sides are consistent; changing only one side is not allowed.

The website version represents the latest published stable release, not the current development or tagged-but-unpublished version. The release branch and final Core tag keep the previous stable value. Only the website publication job may set it to `<X.Y.Z>`, after Core artifacts have been published and verified.

### 2.2 Python

Python uses an independent version and release cycle:

```text
Development version: <X.Y.Z>.dev0
Final version:       <X.Y.Z>
Git tag:             py-v<X.Y.Z>
```

The authoritative Python version is defined in `python/pyproject.toml`. The extension crate version in `python/Cargo.toml` must map to the equivalent Cargo SemVer:

| Python | Cargo extension |
|---|---|
| `<X.Y.Z>.dev0` | `<X.Y.Z>-dev.0` |
| `<X.Y.Z>` | `<X.Y.Z>` |

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

- block a Core release;
- be included as official GitHub Release assets;
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

Starting with `4.0.0`, Maven `<version>` represents only the LakeSoul Core version. External runtime and Scala binary versions are encoded in the `artifactId`. This convention applies to subsequent Core releases.

The `artifactId` uses the runtime compatibility series. The release compatibility matrix records the exact patch versions verified by CI.

See the release-specific document for the concrete coordinates, supported baselines, and the mapping from old coordinates for a given release.

## 5. Official Release Artifacts

### 5.1 Maven Central

Maven Central publishes the GA artifacts and all files required by Central:

- the main artifact;
- POM;
- sources JAR;
- Javadoc JAR;
- GPG signatures;
- checksums.

### 5.2 GitHub Release

The GitHub Release provides shaded artifacts that users can deploy directly:

- shaded connector JARs for each runtime and engine artifact;
- a source archive;
- `SHA256SUMS` and its signature `SHA256SUMS.asc`;
- an SBOM (`SBOM.spdx.json`).

By default, `lakesoul-common` and `lakesoul-io-java` are available only through Maven Central and are not duplicated as GitHub Release assets.

Rust native libraries are embedded in JVM artifacts. They are not published independently to crates.io or as official standalone release artifacts.

## 6. Native Platform and Packaging

### 6.1 Supported Platform

Official LakeSoul Core release artifacts support only:

| Platform | Platform ID | Rust target | Support level |
|---|---|---|---|
| Linux x86_64 | `linux-x86_64` | `x86_64-unknown-linux-gnu` | GA Production |

Native resources use the following layout:

```text
META-INF/native/
└── linux-x86_64/
    ├── liblakesoul_io_c.so
    └── liblakesoul_metadata_c.so
```

The Java loader must reject every other operating system or CPU architecture. Its error message must include:

- the original OS and architecture;
- the normalized platform ID;
- the expected resource path;
- the current artifact version;
- `linux-x86_64` as the only supported platform.

Platform-specific Spark, Flink, or Presto connector JARs are not published.

### 6.2 Release Requirements

Every Core release must:

- build both native libraries for `x86_64-unknown-linux-gnu`;
- run native loader smoke tests on Linux x86_64;
- run required connector E2E tests on Linux x86_64;
- verify that both native libraries are present under `META-INF/native/linux-x86_64/`;
- state explicitly that other operating systems and CPU architectures are unsupported.

Release workflows must explicitly select a Linux x86_64 runner and the `x86_64-unknown-linux-gnu` Rust target.

## 7. Branches and Tags

### 7.1 Branches

Core minor releases use:

```text
release/<major>.<minor>
```

This branch maintains the entire `<major>.<minor>.x` series, including `<major>.<minor>.0` and subsequent patches.

After `release/<major>.<minor>` is created:

- the release branch accepts only release blockers, compatibility fixes, documentation, and packaging fixes;
- `main` advances to `<major>.<minor+1>.0-SNAPSHOT`;
- fixes that also affect future versions should be merged into `main` first and then cherry-picked to `release/<major>.<minor>`;
- fixes specific to release machinery may be committed directly to the release branch, but the PR must explain why.

### 7.2 Tags

Official Core tags:

```text
v<X.Y.Z>
```

Official Python tags:

```text
py-v<X.Y.Z>
```

Tags must be:

- annotated;
- signed;
- immutable;
- created from the release commit on the corresponding `release/<major>.<minor>` branch for Core releases;
- fully consistent with the build metadata version.

An official tag must never be deleted and recreated with the same name.

Do not use:

- a bare version (for example `4.0.0`);
- a `release-` prefix (for example `release-4.0.0`);
- a minor-only tag (for example `v4.0`);
- the `latest` tag.

### 7.3 RC Releases Are Not Mandatory

The default release process does not create:

```text
<X.Y.Z>-rc.<N>
v<X.Y.Z>-rc.<N>
```

The release PR and its complete dry run serve as the release candidate. Maintainers may decide to publish an RC for a specific release only when explicit external acceptance, a community vote, or public migration testing is required.

## 8. Core Release Process

### 8.1 Preparation and Feature Freeze

1. Confirm the target version and release scope.
2. Complete the compatibility audit.
3. Confirm that every release blocker has an owner and status.
4. Create `release/<major>.<minor>` from a `main` commit that has passed the full CI suite.
5. Advance `main` to `<major>.<minor+1>.0-SNAPSHOT` / `<major>.<minor+1>.0-dev.0`.
6. Create a release PR on the release branch that changes the development versions to `<major>.<minor>.0`.

### 8.2 Release PR

The release PR is the internal candidate and must include:

- final Maven and Rust Core versions;
- new Maven coordinates;
- release notes;
- a compatibility matrix;
- metadata migration instructions;
- an upgrade guide;
- documented rollback support and recovery procedures, including explicit points of no return and backup requirements;
- a mapping between old and new Maven coordinates;
- website release content. The release commit and tag retain the previous stable version; the website publication job updates its checked-out copy only after the official Core release succeeds.

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
- native builds and loader smoke tests for every platform required by the release-specific support matrix;
- connector E2E tests required by the release-specific compatibility matrix;
- release-specific metadata migration tests;
- release-specific file-format compatibility and rollback or recovery tests, as applicable;
- JAR content verification;
- Maven sources, Javadocs, and signing dry run;
- checksum and SBOM generation;
- license, header, and dependency policy checks.

A release must not bypass a failing gate. A known issue that is genuinely unrelated to the release and cannot be fixed in time must be documented in the release notes, and a maintainer must explicitly approve the exception.

### 8.4 Final Tag and Publication

After the release PR is merged, the release manager creates a signed tag:

```text
v<X.Y.Z>
```

The tag workflow must:

1. validate the tag format;
2. verify that the tagged commit belongs to `release/<major>.<minor>`;
3. verify that the Maven, Rust, and tag versions match exactly;
4. reject `SNAPSHOT`, `dev`, and unexpected prerelease versions;
5. rerun the release build and all release gates;
6. build immutable final artifacts;
7. publish to Maven Central after every release gate succeeds;
8. verify the published Maven Central artifacts;
9. create the GitHub Release and upload its assets;
10. publish the website and the release notes;
11. report registry URLs, artifact checksums, and the commit SHA.

Production credentials are repository Actions secrets. The caller must map
each secret explicitly to the reusable release workflow; `secrets: inherit` is
prohibited. Release workflows may reference registry, signing, and deployment
credentials only in production publication and verification jobs.

Production publication may only be triggered by a final tag. `workflow_dispatch` may run a dry run or retry safe build steps, but it must not publish a production release from an arbitrary branch or commit.

### 8.5 Post-release Tasks

1. Verify that all coordinates resolve from Maven Central.
2. Download the GitHub Release JARs and verify them against `SHA256SUMS`.
3. Run at least one Spark, Flink, and Presto installation smoke test in a clean environment.
4. Advance `release/<major>.<minor>` to `<X.Y.(Z+1)>-SNAPSHOT` / `<X.Y.(Z+1)>-dev.0`.
5. Confirm that `main` is at `<major>.<minor+1>.0-SNAPSHOT` / `<major>.<minor+1>.0-dev.0`.
6. Synchronize required release documentation and fixes back to `main`.
7. Announce the release, upgrade constraints, and known issues.

## 9. Patch Releases

Patch releases are published from the corresponding minor release branch:

```text
release/<major>.<minor>
```

Process:

1. Merge the fix into `main` first.
2. Cherry-pick it to `release/<major>.<minor>`.
3. Create a release PR that changes `<X.Y.(Z+1)>-SNAPSHOT` to `<X.Y.(Z+1)>`.
4. Run the same release dry run.
5. Create the signed tag `v<X.Y.(Z+1)>`.
6. Publish after all release gates succeed.
7. Advance the release branch to `<X.Y.(Z+2)>-SNAPSHOT`.

Patch releases do not use RCs by default.

## 10. Python Release Process

Python releases are independent of Core:

1. Update `python/pyproject.toml` to the final version in a Python release PR.
2. Synchronize `python/Cargo.toml`.
3. Validate the mapping between the Python tag, PEP 440 version, and Cargo SemVer.
4. Run Python tests and wheel metadata validation.
5. Build wheels and an sdist for supported platforms.
6. Create a signed tag such as `py-v<X.Y.Z>`.
7. Publish through PyPI Trusted Publishing.
8. Advance to the next `.dev0` version after publication.
9. Do not trigger a Core Maven release, Core GitHub Release, or website Core stable update.

Python publication must use OIDC Trusted Publishing. Long-lived PyPI tokens must not be stored.

## 11. Version Synchronization Tool

Do not add another manually maintained root `VERSION` file. Implement a release tool such as:

```text
python script/release.py check
python script/release.py set-core <X.Y.Z>-SNAPSHOT
python script/release.py set-core <X.Y.Z>
python script/release.py set-python <X.Y.Z>.dev0
python script/release.py check-tag v<X.Y.Z>
python script/release.py set-website-stable <X.Y.Z>  # publication job only
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

## 12. Release Security and Traceability

An official release must provide:

- a signed, annotated Git tag;
- GPG-signed Maven artifacts;
- a signature for GitHub Release checksums;
- an SBOM;
- GitHub artifact provenance or attestation when available;
- GitHub Actions pinned to fixed versions or commit SHAs;
- no floating `master` or `latest` references for release toolchains;
- explicit records of the source commit, Rust toolchain, JDK, Maven, and platform target;
- repository publication credentials explicitly mapped to the release workflow and referenced only by production publish or verification jobs;
- OIDC Trusted Publishing for PyPI;
- long-term traceability of release artifact build logs and checksums.
