# Release Documentation Structure Design

## Problem

`docs/release/release-4.0.0.md` currently combines two different kinds of information:

- repository-wide release policy and repeatable Core/Python release procedures;
- versions, compatibility constraints, migration risks, implementation work, and blockers specific to LakeSoul 4.0.0.

Keeping both in one version-named document makes the general policy difficult to discover and encourages later releases to copy rules that can drift independently.

## Decision

Use two release documents:

```text
docs/release/
├── release-guide.md
└── release-4.0.0.md
```

`release-guide.md` is the authoritative repository-wide policy and process for both the LakeSoul Core and Python release domains. `release-4.0.0.md` is the release manifest and readiness plan for the 4.0.0 Core release.

Do not introduce additional Core/Python process documents or a release-directory index yet. Two documents are sufficient for the current material and avoid unnecessary navigation.

## General Release Guide

`docs/release/release-guide.md` contains rules intended to remain valid across releases:

1. Release principles and release-domain boundaries.
2. Core, Python, Rust crate, and Experimental component version policies.
3. SemVer classification rules.
4. Maven coordinate naming rules.
5. Official artifact types and publication channels.
6. Native resource layout and platform support-level definitions.
7. Branch and tag conventions.
8. Core preparation, release PR, gate, final-tag publication, and post-release procedures.
9. Patch-release procedure.
10. Independent Python release procedure.
11. Version synchronization tool contract.
12. Security, signing, provenance, credential, and traceability requirements.

Concrete versions are replaced with parameters or non-normative examples where the number is not itself policy:

```text
release/<major>.<minor>
v<core-version>
py-v<python-version>
```

Historical policy changes that remain relevant, such as the Maven coordinate scheme adopted in 4.0.0, may identify the release in which they took effect. Current runtime patch levels and one-release exceptions do not belong in the general guide.

## LakeSoul 4.0.0 Document

`docs/release/release-4.0.0.md` starts with a link to the general guide and states that it contains only 4.0.0 parameters and readiness requirements. Its sections are:

1. Status and scope.
2. Release manifest, including concrete Core/Python versions, branch, and tags.
3. Maven coordinates and exact compatibility matrix.
4. Exact Maven Central and GitHub Release artifacts.
5. Concrete platform support matrix and unsupported targets.
6. Upgrade and compatibility requirements for file formats, metadata, native ABI, Spark, Flink, and Presto.
7. Gates specific to 4.0.0.
8. Release automation implementation phases.
9. Known gaps and publication blockers.

The document retains the 4.0.0-specific facts currently distributed through the guide, including:

- Core `4.0.0`, Python `2.0.0`, `release/4.0`, and `v4.0.0` mappings;
- Spark 3.5.8, Flink 1.20.0/Flink CDC 3.5.0, and Presto 0.296 baselines;
- migration from old Maven coordinates;
- Gluten Preview status;
- exact release asset names;
- the five-platform support matrix;
- Vortex, metadata, native ABI, connector, and rollback risks;
- the four-phase automation implementation order;
- current pipeline gaps and the prohibition on publishing `v4.0.0` before they are resolved.

## Authority and Duplication Rules

- `release-guide.md` is the only source of repository-wide release policy and standard procedure.
- A version-specific document references the guide instead of restating its steps.
- A version-specific release may add stricter gates.
- It may not silently weaken the general policy.
- Any required deviation is recorded in an explicit `Release-specific Exceptions` section with its approval requirement.
- Version-specific matrices, filenames, risks, blockers, and implementation status are not copied into the general guide.

## Migration Method

Restructure the existing content rather than rewrite its policy:

1. Move general sections into `release-guide.md` and parameterize version-bound examples.
2. Keep 4.0.0 facts in `release-4.0.0.md`, grouped under the version-specific structure.
3. Replace removed general sections in the 4.0.0 document with one authoritative link.
4. Preserve every normative requirement; delete only duplicated wording after its destination is established.
5. Check relative Markdown links and search both documents for accidental duplicate policy blocks and misplaced concrete versions.

## Verification

The restructuring is complete when:

- every requirement from the original document appears in exactly one authoritative location, except for short links or intentional release parameters;
- the general guide can be followed for a later Core or Python release without copying 4.0.0 compatibility data;
- the 4.0.0 document is understandable as a concrete release manifest and blocker list when read with the linked guide;
- headings and relative links render correctly;
- no placeholders, unresolved cross-references, or contradictory authority statements remain.
