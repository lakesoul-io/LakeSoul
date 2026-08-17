#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

try:
    from . import release
except ImportError:
    import release

ROOT = Path(__file__).resolve().parents[1]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
ARTIFACT_MODULES = {
    "lakesoul-common/pom.xml": "lakesoul-common",
    "native-io/lakesoul-io-java/pom.xml": "lakesoul-io-java",
    "lakesoul-spark/pom.xml": "lakesoul-spark",
    "lakesoul-flink/pom.xml": "lakesoul-flink",
    "lakesoul-presto/pom.xml": "lakesoul-presto",
}


class VerificationError(RuntimeError):
    pass


def _text(element: ET.Element, name: str) -> str:
    child = element.find(f"m:{name}", NS)
    if child is None or child.text is None:
        raise VerificationError(f"effective POM project is missing {name}")
    return child.text.strip()


def verify_coordinates(effective_pom: Path, root: Path = ROOT) -> None:
    core = release.core_version(root)
    artifact_ids = release.expected_artifact_ids(root)
    expected_ids = {
        "lakesoul-parent",
        *(artifact_ids[pom] for pom in ARTIFACT_MODULES),
    }
    document = ET.parse(effective_pom).getroot()
    projects = (
        document.findall("m:project", NS)
        if document.tag.endswith("projects")
        else [document]
    )
    actual: dict[str, tuple[str, str]] = {}
    for project in projects:
        artifact_id = _text(project, "artifactId")
        coordinate = (_text(project, "groupId"), _text(project, "version"))
        if artifact_id in actual:
            raise VerificationError(f"duplicate effective coordinate for {artifact_id}")
        actual[artifact_id] = coordinate

    missing = sorted(expected_ids - actual.keys())
    if missing:
        raise VerificationError(
            f"effective POM is missing artifacts: {', '.join(missing)}"
        )
    errors = [
        f"{artifact_id}: {group_id}:{version}"
        for artifact_id in sorted(expected_ids)
        for group_id, version in [actual[artifact_id]]
        if group_id != "com.dmetasoul" or version != core.maven
    ]
    if errors:
        raise VerificationError(
            "effective coordinates do not use com.dmetasoul and the Core version: "
            + "; ".join(errors)
        )


def _verify_zip(path: Path, root: Path = ROOT, *, sources: bool = False) -> None:
    if not path.is_file():
        raise VerificationError(f"missing release artifact: {path.relative_to(root)}")
    try:
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if not name.endswith("/")]
            if not names:
                raise VerificationError(
                    f"empty release artifact: {path.relative_to(root)}"
                )
            if sources and not any(
                name.endswith((".java", ".scala")) for name in names
            ):
                raise VerificationError(
                    f"sources artifact has no Java or Scala source: {path.relative_to(root)}"
                )
    except zipfile.BadZipFile as error:
        raise VerificationError(f"invalid JAR: {path.relative_to(root)}") from error


def verify_artifacts(root: Path = ROOT, *, signatures: bool = False) -> None:
    core = release.core_version(root)
    artifact_ids = release.expected_artifact_ids(root)
    for pom, base_artifact_id in ARTIFACT_MODULES.items():
        artifact_id = artifact_ids[pom]
        if base_artifact_id not in artifact_id:
            raise VerificationError(
                f"unexpected artifactId mapping for {pom}: {artifact_id}"
            )
        target = root / Path(pom).parent / "target"
        main = target / f"{artifact_id}-{core.maven}.jar"
        sources = target / f"{artifact_id}-{core.maven}-sources.jar"
        javadoc = target / f"{artifact_id}-{core.maven}-javadoc.jar"
        _verify_zip(main, root)
        _verify_zip(sources, root, sources=True)
        _verify_zip(javadoc, root)
        if signatures:
            for artifact in (main, sources, javadoc):
                signature = Path(f"{artifact}.asc")
                if not signature.is_file():
                    raise VerificationError(
                        f"missing detached signature: {signature.relative_to(root)}"
                    )
                result = subprocess.run(
                    ["gpg", "--batch", "--verify", str(signature), str(artifact)],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                if result.returncode:
                    raise VerificationError(
                        f"invalid signature for {artifact.relative_to(root)}: "
                        f"{result.stderr.strip()}"
                    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    coordinates = subparsers.add_parser("coordinates")
    coordinates.add_argument("effective_pom", type=Path)
    artifacts = subparsers.add_parser("artifacts")
    artifacts.add_argument("--signatures", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.operation == "coordinates":
            verify_coordinates(args.effective_pom)
            print("Effective Maven coordinates are synchronized.")
        else:
            verify_artifacts(signatures=args.signatures)
            checked = " and signatures" if args.signatures else ""
            print(f"Release JARs and classifiers{checked} are valid.")
    except (OSError, ET.ParseError, VerificationError, release.ReleaseError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
