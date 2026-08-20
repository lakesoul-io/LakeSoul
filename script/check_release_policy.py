#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
import sys
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
DYNAMIC_MAVEN = re.compile(r"^(?:LATEST|RELEASE)$|^[\[(].*[\])]$", re.IGNORECASE)
DYNAMIC_CARGO = re.compile(r"^(?:\*|latest)$", re.IGNORECASE)


def _manifests(root: Path, pattern: str) -> list[Path]:
    return sorted(path for path in root.glob(pattern) if "target" not in path.parts)


def _header_files(root: Path) -> list[Path]:
    paths = [
        root / "Cargo.toml",
        root / "pom.xml",
        root / "python/Cargo.toml",
        root / "python/pyproject.toml",
        root / ".github/workflows/deployment.yml",
        root / ".github/workflows/native-build.yml",
        root / ".github/workflows/release-build.yml",
        root / ".github/workflows/website-publish.yml",
    ]
    paths.extend(_manifests(root, "rust/*/Cargo.toml"))
    paths.extend(_manifests(root, "*/pom.xml"))
    paths.extend(sorted(root.glob("script/release*.py")))
    paths.extend(sorted(root.glob("script/verify_*.py")))
    return sorted(set(paths))


def _walk_dependencies(table: dict[str, Any]) -> list[tuple[str, Any]]:
    dependencies: list[tuple[str, Any]] = []
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        dependencies.extend(
            (name, value) for name, value in table.get(section, {}).items()
        )
    for target in table.get("target", {}).values():
        if isinstance(target, dict):
            dependencies.extend(_walk_dependencies(target))
    return dependencies


def check_headers(root: Path, errors: list[str]) -> None:
    for path in _header_files(root):
        if not path.is_file():
            errors.append(f"missing release policy file: {path.relative_to(root)}")
            continue
        header = path.read_text(encoding="utf-8", errors="replace")[:1024]
        if "SPDX-License-Identifier: Apache-2.0" not in header:
            errors.append(f"{path.relative_to(root)}: missing Apache-2.0 SPDX header")


def check_cargo(root: Path, errors: list[str]) -> None:
    manifests = [root / "Cargo.toml", root / "python/Cargo.toml"]
    manifests.extend(_manifests(root, "rust/*/Cargo.toml"))
    for manifest in sorted(set(manifests)):
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        package = data.get("package")
        if package and package.get("publish") is not False:
            errors.append(
                f"{manifest.relative_to(root)}: package.publish must be false"
            )
        for name, dependency in _walk_dependencies(data):
            if isinstance(dependency, str):
                if DYNAMIC_CARGO.match(dependency):
                    errors.append(
                        f"{manifest.relative_to(root)}: {name} uses dynamic version {dependency!r}"
                    )
                continue
            if not isinstance(dependency, dict):
                continue
            version = dependency.get("version")
            if isinstance(version, str) and DYNAMIC_CARGO.match(version):
                errors.append(
                    f"{manifest.relative_to(root)}: {name} uses dynamic version {version!r}"
                )
            if "git" in dependency and "rev" not in dependency:
                errors.append(
                    f"{manifest.relative_to(root)}: Git dependency {name} must pin rev"
                )


def check_maven(root: Path, errors: list[str]) -> None:
    for pom in [root / "pom.xml", *_manifests(root, "*/pom.xml")]:
        document = ET.parse(pom).getroot()
        for version in document.findall(".//m:dependency/m:version", NS):
            value = (version.text or "").strip()
            if DYNAMIC_MAVEN.search(value):
                errors.append(
                    f"{pom.relative_to(root)}: Maven dependency uses dynamic version {value!r}"
                )
    root_pom = ET.parse(root / "pom.xml").getroot()
    license_name = root_pom.findtext("m:licenses/m:license/m:name", namespaces=NS)
    if not license_name or not license_name.startswith("Apache 2.0"):
        errors.append("pom.xml: project license must be Apache 2.0")


def check_python(root: Path, errors: list[str]) -> None:
    data = tomllib.loads((root / "python/pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    license_value = project.get("license")
    if license_value not in ({"text": "Apache-2.0"}, "Apache-2.0"):
        errors.append("python/pyproject.toml: project license must be Apache-2.0")
    dependencies = list(project.get("dependencies", []))
    for values in project.get("optional-dependencies", {}).values():
        dependencies.extend(values)
    for dependency in dependencies:
        if " @ http" in dependency or dependency.startswith(
            ("git+", "http://", "https://")
        ):
            errors.append(
                "python/pyproject.toml: dependency must come from the package index: "
                + dependency
            )


def validate(root: Path = ROOT) -> list[str]:
    errors: list[str] = []
    check_headers(root, errors)
    check_cargo(root, errors)
    check_maven(root, errors)
    check_python(root, errors)
    return errors


def main() -> int:
    try:
        errors = validate()
    except (OSError, tomllib.TOMLDecodeError, ET.ParseError, KeyError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Release license, header, and dependency policies are satisfied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
