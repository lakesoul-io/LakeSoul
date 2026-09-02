#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Synchronize and validate LakeSoul release versions."""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
MAVEN_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
CORE_PATTERN = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-SNAPSHOT)?\Z")
PYTHON_PATTERN = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(\.dev0)?\Z")
FINAL_PATTERN = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)\Z")

MAVEN_MODULES = {
    "pom.xml": "lakesoul-parent",
    "lakesoul-common/pom.xml": "lakesoul-common",
    "native-io/lakesoul-io-java/pom.xml": "lakesoul-io-java",
    "lakesoul-spark/pom.xml": None,
    "lakesoul-flink/pom.xml": None,
    "lakesoul-presto/pom.xml": None,
    "lakesoul-spark-gluten/pom.xml": None,
}


class ReleaseError(Exception):
    """A release metadata invariant was violated."""


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class CoreVersion:
    version: Version
    snapshot: bool

    @property
    def maven(self) -> str:
        suffix = "-SNAPSHOT" if self.snapshot else ""
        return f"{self.version}{suffix}"

    @property
    def cargo(self) -> str:
        suffix = "-dev.0" if self.snapshot else ""
        return f"{self.version}{suffix}"


@dataclass(frozen=True)
class PythonVersion:
    version: Version
    development: bool

    @property
    def python(self) -> str:
        suffix = ".dev0" if self.development else ""
        return f"{self.version}{suffix}"

    @property
    def cargo(self) -> str:
        suffix = "-dev.0" if self.development else ""
        return f"{self.version}{suffix}"


def parse_core(value: str) -> CoreVersion:
    match = CORE_PATTERN.fullmatch(value)
    if not match:
        raise ReleaseError(
            f"unsupported Core version {value!r}; expected X.Y.Z or X.Y.Z-SNAPSHOT"
        )
    return CoreVersion(
        Version(*(int(part) for part in match.group(1, 2, 3))),
        match.group(4) is not None,
    )


def parse_python(value: str) -> PythonVersion:
    match = PYTHON_PATTERN.fullmatch(value)
    if not match:
        raise ReleaseError(
            f"unsupported Python version {value!r}; expected X.Y.Z or X.Y.Z.dev0"
        )
    return PythonVersion(
        Version(*(int(part) for part in match.group(1, 2, 3))),
        match.group(4) is not None,
    )


def parse_final(value: str, description: str) -> Version:
    match = FINAL_PATTERN.fullmatch(value)
    if not match:
        raise ReleaseError(f"invalid {description} {value!r}; expected X.Y.Z")
    return Version(*(int(part) for part in match.group(1, 2, 3)))


def read_text(root: Path, relative: str) -> str:
    try:
        return (root / relative).read_text(encoding="utf-8")
    except OSError as error:
        raise ReleaseError(f"cannot read {relative}: {error}") from error


def one_match(
    text: str, pattern: str, relative: str, description: str
) -> re.Match[str]:
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE))
    if len(matches) != 1:
        raise ReleaseError(
            f"expected exactly one {description} in {relative}, found {len(matches)}"
        )
    return matches[0]


def matched_value(root: Path, relative: str, pattern: str, description: str) -> str:
    return one_match(read_text(root, relative), pattern, relative, description).group(1)


def replace_value(
    root: Path,
    relative: str,
    pattern: str,
    replacement: str,
    description: str,
) -> tuple[str, str] | None:
    text = read_text(root, relative)
    match = one_match(text, pattern, relative, description)
    if match.group(1) == replacement:
        return None
    updated = text[: match.start(1)] + replacement + text[match.end(1) :]
    return relative, updated


def core_version(root: Path) -> CoreVersion:
    value = matched_value(
        root,
        "pom.xml",
        r"<revision>([^<]+)</revision>",
        "Maven revision",
    )
    return parse_core(value)


def python_version(root: Path) -> PythonVersion:
    value = matched_value(
        root,
        "python/pyproject.toml",
        r'^version\s*=\s*"([^"]+)"',
        "Python project version",
    )
    return parse_python(value)


def website_version(root: Path) -> Version:
    value = matched_value(
        root,
        "website/docusaurus.config.js",
        r"^\s*VERSION:\s*['\"]([^'\"]+)['\"]",
        "website VERSION replacement",
    )
    return parse_final(value, "website stable version")


def xml_text(element: ET.Element, path: str, relative: str) -> str:
    child = element.find(path, MAVEN_NAMESPACE)
    if child is None or child.text is None:
        raise ReleaseError(f"missing {path} in {relative}")
    return child.text.strip()


def parse_pom(root: Path, relative: str) -> ET.Element:
    try:
        return ET.parse(root / relative).getroot()
    except (OSError, ET.ParseError) as error:
        raise ReleaseError(f"cannot parse {relative}: {error}") from error


def compatibility_series(version: str) -> str:
    parts = version.split(".")
    if len(parts) < 2 or any(not part.isdigit() for part in parts):
        raise ReleaseError(f"invalid runtime compatibility version {version!r}")
    return ".".join(parts[:2])


def expected_artifact_ids(root: Path) -> dict[str, str]:
    root_pom = parse_pom(root, "pom.xml")
    scala = xml_text(root_pom, "m:properties/m:scala.binary.version", "pom.xml")
    spark = xml_text(root_pom, "m:properties/m:spark.version", "pom.xml")
    flink_pom = parse_pom(root, "lakesoul-flink/pom.xml")
    flink = xml_text(
        flink_pom, "m:properties/m:flink.version", "lakesoul-flink/pom.xml"
    )
    presto_pom = parse_pom(root, "lakesoul-presto/pom.xml")
    presto = xml_text(
        presto_pom, "m:properties/m:presto.version", "lakesoul-presto/pom.xml"
    )
    return {
        **{path: artifact for path, artifact in MAVEN_MODULES.items() if artifact},
        "lakesoul-spark/pom.xml": (
            f"lakesoul-spark-{compatibility_series(spark)}_{scala}"
        ),
        "lakesoul-flink/pom.xml": (
            f"lakesoul-flink-{compatibility_series(flink)}_{scala}"
        ),
        "lakesoul-presto/pom.xml": f"lakesoul-presto-{presto}",
        "lakesoul-spark-gluten/pom.xml": (
            f"lakesoul-spark-gluten-{compatibility_series(spark)}_{scala}"
        ),
    }


def validate_maven(root: Path, core: CoreVersion, errors: list[str]) -> None:
    for relative, expected_artifact in expected_artifact_ids(root).items():
        pom = parse_pom(root, relative)
        artifact = xml_text(pom, "m:artifactId", relative)
        if artifact != expected_artifact:
            errors.append(
                f"{relative}: artifactId {artifact!r} != {expected_artifact!r}"
            )

        declared_version = xml_text(pom, "m:version", relative)
        if declared_version != "${revision}":
            errors.append(
                f"{relative}: project version must be ${{revision}}, got {declared_version!r}"
            )

        if relative != "pom.xml":
            parent_version = xml_text(pom, "m:parent/m:version", relative)
            if parent_version != "${revision}":
                errors.append(
                    f"{relative}: parent version must be ${{revision}}, got {parent_version!r}"
                )

        for dependency in pom.findall(".//m:dependency", MAVEN_NAMESPACE):
            group = dependency.find("m:groupId", MAVEN_NAMESPACE)
            if (
                group is None
                or group.text is None
                or group.text.strip() != "com.dmetasoul"
            ):
                continue
            version = dependency.find("m:version", MAVEN_NAMESPACE)
            actual = (
                version.text.strip() if version is not None and version.text else None
            )
            if actual != "${revision}":
                dependency_artifact = xml_text(dependency, "m:artifactId", relative)
                errors.append(
                    f"{relative}: internal dependency {dependency_artifact} version "
                    f"must be ${{revision}}, got {actual!r}"
                )

    revision = matched_value(
        root, "pom.xml", r"<revision>([^<]+)</revision>", "Maven revision"
    )
    if revision != core.maven:
        errors.append(f"pom.xml: revision {revision!r} != {core.maven!r}")


def validate_rust(root: Path, core: CoreVersion, errors: list[str]) -> None:
    experimental = {"lakesoul-flight", "lakesoul-s3-proxy"}
    manifests = sorted(root.glob("rust/*/Cargo.toml"))
    python_manifest = root / "python/Cargo.toml"
    if python_manifest.exists():
        manifests.append(python_manifest)

    for manifest in manifests:
        relative = manifest.relative_to(root).as_posix()
        text = read_text(root, relative)
        package = re.search(
            r"^\[package\]\s*(.*?)(?=^\[|\Z)", text, flags=re.MULTILINE | re.DOTALL
        )
        if package is None:
            errors.append(f"{relative}: missing [package] section")
            continue
        metadata = package.group(1)
        if not re.search(r"^publish\s*=\s*false\s*$", metadata, re.MULTILINE):
            errors.append(f"{relative}: package must set publish = false")

        crate = manifest.parent.name
        if crate == "python":
            continue
        if crate in experimental:
            if not re.search(
                r'^description\s*=\s*"[^"]*Experimental[^"]*Core GA[^"]*"\s*$',
                metadata,
                re.MULTILINE,
            ):
                errors.append(
                    f"{relative}: Experimental crate must be marked outside Core GA"
                )
            continue

        inherits_workspace = re.search(
            r"^version\.workspace\s*=\s*true\s*$", metadata, re.MULTILINE
        ) or re.search(
            r"^version\s*=\s*\{\s*workspace\s*=\s*true\s*\}\s*$",
            metadata,
            re.MULTILINE,
        )
        if inherits_workspace:
            continue
        explicit = re.search(r'^version\s*=\s*"([^"]+)"\s*$', metadata, re.MULTILINE)
        if explicit is None:
            errors.append(f"{relative}: package version does not inherit the workspace")
        elif explicit.group(1) != core.cargo:
            errors.append(
                f"{relative}: package version {explicit.group(1)!r} != {core.cargo!r}"
            )


def validate_release_workflows(root: Path, errors: list[str]) -> None:
    publish_command = re.compile(
        r"\bcargo\s+(?:\+\S+\s+)?publish\b", flags=re.IGNORECASE
    )
    workflows = list(root.glob(".github/workflows/*.yml"))
    workflows.extend(root.glob(".github/workflows/*.yaml"))
    for workflow in sorted(workflows):
        relative = workflow.relative_to(root).as_posix()
        if publish_command.search(read_text(root, relative)):
            errors.append(f"{relative}: release workflows must not run cargo publish")


def validate(root: Path) -> list[str]:
    errors: list[str] = []
    try:
        core = core_version(root)
        python = python_version(root)
        stable = website_version(root)

        cargo_core = matched_value(
            root,
            "Cargo.toml",
            r'^version\s*=\s*"([^"]+)"',
            "workspace package version",
        )
        if cargo_core != core.cargo:
            errors.append(
                f"Cargo.toml: workspace version {cargo_core!r} != {core.cargo!r}"
            )

        cargo_python = matched_value(
            root,
            "python/Cargo.toml",
            r'^version\s*=\s*"([^"]+)"',
            "Python Cargo version",
        )
        if cargo_python != python.cargo:
            errors.append(
                f"python/Cargo.toml: version {cargo_python!r} != {python.cargo!r}"
            )
        lock_path = root / "python/uv.lock"
        if lock_path.exists():
            lock_python = matched_value(
                root,
                "python/uv.lock",
                r'\[\[package\]\]\nname = "lakesoul"\nversion = "([^"]+)"',
                "Python lockfile version",
            )
            if lock_python != python.python:
                errors.append(
                    f"python/uv.lock: version {lock_python!r} != {python.python!r}"
                )

        if stable >= core.version:
            errors.append(
                f"website stable version {stable} must precede unpublished Core {core.maven}"
            )

        validate_maven(root, core, errors)
        validate_rust(root, core, errors)
        validate_release_workflows(root, errors)
    except ReleaseError as error:
        errors.append(str(error))
    return errors


def apply_updates(
    root: Path, updates: Iterable[tuple[str, str] | None], check: bool
) -> None:
    changed = [update for update in updates if update is not None]
    if not changed:
        print("Versions already synchronized.")
        return

    verb = "Would update" if check else "Updating"
    for relative, _ in changed:
        print(f"{verb} {relative}")

    if check:
        raise ReleaseError("version files are not synchronized")
    for relative, content in changed:
        (root / relative).write_text(content, encoding="utf-8")


def set_core(root: Path, value: str, check: bool = False) -> None:
    target = parse_core(value)
    stable = website_version(root)
    if stable >= target.version:
        raise ReleaseError(
            f"website stable version {stable} must precede unpublished Core {target.maven}"
        )

    apply_updates(
        root,
        [
            replace_value(
                root,
                "pom.xml",
                r"<revision>([^<]+)</revision>",
                target.maven,
                "Maven revision",
            ),
            replace_value(
                root,
                "Cargo.toml",
                r'^version\s*=\s*"([^"]+)"',
                target.cargo,
                "workspace package version",
            ),
        ],
        check,
    )


def set_website_stable(root: Path, value: str, check: bool = False) -> None:
    target = parse_final(value, "website stable version")
    core = core_version(root)
    if core.snapshot or target != core.version:
        raise ReleaseError(
            f"website stable version {target} requires matching final Core, got {core.maven}"
        )
    apply_updates(
        root,
        [
            replace_value(
                root,
                "website/docusaurus.config.js",
                r"^\s*VERSION:\s*['\"]([^'\"]+)['\"]",
                str(target),
                "website VERSION replacement",
            )
        ],
        check,
    )


def set_python(root: Path, value: str, check: bool = False) -> None:
    target = parse_python(value)
    apply_updates(
        root,
        [
            replace_value(
                root,
                "python/pyproject.toml",
                r'^version\s*=\s*"([^"]+)"',
                target.python,
                "Python project version",
            ),
            replace_value(
                root,
                "python/Cargo.toml",
                r'^version\s*=\s*"([^"]+)"',
                target.cargo,
                "Python Cargo version",
            ),
            replace_value(
                root,
                "python/uv.lock",
                r'\[\[package\]\]\nname = "lakesoul"\nversion = "([^"]+)"',
                target.python,
                "Python lockfile version",
            ),
        ],
        check,
    )


def require_synchronized(root: Path) -> None:
    errors = validate(root)
    if errors:
        raise ReleaseError("repository versions are inconsistent: " + "; ".join(errors))


def check_tag(root: Path, tag: str) -> None:
    if tag.startswith("py-v"):
        tagged = parse_final(tag[4:], "Python tag")
        require_synchronized(root)
        current = python_version(root)
        if current.development:
            raise ReleaseError(
                f"Python tag {tag!r} requires a final Python version, got {current.python}"
            )
        if tagged != current.version:
            raise ReleaseError(
                f"Python tag {tag!r} does not match Python version {current.python}"
            )
        print(f"Python tag {tag} matches version {current.python}.")
        return

    if tag.startswith("v"):
        tagged = parse_final(tag[1:], "Core tag")
        require_synchronized(root)
        current = core_version(root)
        if current.snapshot:
            raise ReleaseError(
                f"Core tag {tag!r} requires a final Core version, got {current.maven}"
            )
        if tagged != current.version:
            raise ReleaseError(
                f"Core tag {tag!r} does not match Core version {current.maven}"
            )
        stable = website_version(root)
        if stable >= tagged:
            raise ReleaseError(
                f"Core tag {tag!r} requires the website stable version to precede "
                f"the unpublished Core version, got {stable}"
            )
        print(f"Core tag {tag} matches Maven and Rust versions.")
        return

    raise ReleaseError(f"unsupported tag {tag!r}; expected vX.Y.Z or py-vX.Y.Z")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    subparsers.add_parser("check", help="validate all version invariants")

    core_parser = subparsers.add_parser(
        "set-core", help="synchronize Maven and Rust Core versions"
    )
    core_parser.add_argument("version")
    core_parser.add_argument(
        "--check", action="store_true", help="report required changes without writing"
    )
    website_parser = subparsers.add_parser(
        "set-website-stable",
        help="set the website stable version after Core publication",
    )
    website_parser.add_argument("version")
    website_parser.add_argument(
        "--check", action="store_true", help="report required changes without writing"
    )

    python_parser = subparsers.add_parser(
        "set-python", help="synchronize Python and extension crate versions"
    )
    python_parser.add_argument("version")
    python_parser.add_argument(
        "--check", action="store_true", help="report required changes without writing"
    )

    tag_parser = subparsers.add_parser(
        "check-tag", help="validate an official tag against repository versions"
    )
    tag_parser.add_argument("tag")
    return parser


def main(argv: list[str] | None = None, root: Path = ROOT) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.operation == "check":
            errors = validate(root)
            if errors:
                for error in errors:
                    print(f"ERROR: {error}", file=sys.stderr)
                return 1
            print("Release versions are synchronized.")
        elif args.operation == "set-core":
            set_core(root, args.version, args.check)
        elif args.operation == "set-python":
            set_python(root, args.version, args.check)
        elif args.operation == "set-website-stable":
            set_website_stable(root, args.version, args.check)
        elif args.operation == "check-tag":
            check_tag(root, args.tag)
    except ReleaseError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
