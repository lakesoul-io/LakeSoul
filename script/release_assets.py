#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Assemble and verify immutable LakeSoul Core release assets."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import BinaryIO

try:
    from . import release
except ImportError:
    import release

ROOT = Path(__file__).resolve().parents[1]
GA_CONNECTOR_POMS = (
    "lakesoul-spark/pom.xml",
    "lakesoul-flink/pom.xml",
    "lakesoul-presto/pom.xml",
)
CONNECTOR_POMS = (*GA_CONNECTOR_POMS, "lakesoul-spark-gluten/pom.xml")
CENTRAL_POMS = (
    "pom.xml",
    "lakesoul-common/pom.xml",
    "native-io/lakesoul-io-java/pom.xml",
    "lakesoul-spark/pom.xml",
    "lakesoul-flink/pom.xml",
    "lakesoul-presto/pom.xml",
)
DEFAULT_CENTRAL = "https://repo1.maven.org/maven2"


class AssetError(RuntimeError):
    pass


def _copy_stream(source: BinaryIO, destination: BinaryIO) -> None:
    while chunk := source.read(1024 * 1024):
        destination.write(chunk)


def _source_archive(root: Path, destination: Path, version: str, commit: str) -> None:
    prefix = f"lakesoul-{version}-src/"
    process = subprocess.Popen(
        ["git", "archive", "--format=tar", f"--prefix={prefix}", commit],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    try:
        with (
            destination.open("wb") as raw,
            gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed,
        ):
            _copy_stream(process.stdout, compressed)
        stderr = process.communicate()[1].decode("utf-8", errors="replace")
    except BaseException:
        process.kill()
        process.wait()
        destination.unlink(missing_ok=True)
        raise
    if process.returncode:
        destination.unlink(missing_ok=True)
        raise AssetError(f"git archive failed for {commit}: {stderr.strip()}")


def expected_asset_names(root: Path = ROOT) -> list[str]:
    core = release.core_version(root).maven
    artifact_ids = release.expected_artifact_ids(root)
    return [
        *(f"{artifact_ids[pom]}-{core}.jar" for pom in CONNECTOR_POMS),
        f"lakesoul-{core}-src.tar.gz",
        "SBOM.spdx.json",
    ]


def assemble(output: Path, root: Path = ROOT, *, commit: str = "HEAD") -> None:
    core = release.core_version(root).maven
    artifact_ids = release.expected_artifact_ids(root)
    output.mkdir(parents=True, exist_ok=True)
    for pom in CONNECTOR_POMS:
        artifact_id = artifact_ids[pom]
        source = root / Path(pom).parent / "target" / f"{artifact_id}-{core}.jar"
        if not source.is_file():
            raise AssetError(f"missing connector artifact: {source.relative_to(root)}")
        shutil.copyfile(source, output / source.name)
    _source_archive(root, output / f"lakesoul-{core}-src.tar.gz", core, commit)


def write_checksums(directory: Path, root: Path = ROOT) -> Path:
    expected = expected_asset_names(root)
    missing = [name for name in expected if not (directory / name).is_file()]
    if missing:
        raise AssetError(f"cannot checksum missing assets: {', '.join(missing)}")
    manifest = directory / "SHA256SUMS"
    lines = []
    for name in sorted(expected):
        digest = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        lines.append(f"{digest}  {name}\n")
    manifest.write_text("".join(lines), encoding="utf-8")
    return manifest


def _parse_checksums(path: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        parts = line.split("  ", 1)
        if len(parts) != 2 or len(parts[0]) != 64:
            raise AssetError(f"invalid SHA256SUMS line {line_number}")
        digest, name = parts
        if name in checksums:
            raise AssetError(f"duplicate checksum entry: {name}")
        checksums[name] = digest
    return checksums


def verify(directory: Path, root: Path = ROOT, *, signature: bool = False) -> None:
    expected = set(expected_asset_names(root))
    manifest = directory / "SHA256SUMS"
    if not manifest.is_file():
        raise AssetError("missing SHA256SUMS")
    checksums = _parse_checksums(manifest)
    if set(checksums) != expected:
        missing = sorted(expected - checksums.keys())
        extra = sorted(checksums.keys() - expected)
        raise AssetError(
            f"checksum asset set mismatch; missing={missing}, extra={extra}"
        )
    for name, expected_digest in checksums.items():
        path = directory / name
        if not path.is_file():
            raise AssetError(f"missing checksummed asset: {name}")
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected_digest:
            raise AssetError(f"checksum mismatch for {name}")

    sbom = json.loads((directory / "SBOM.spdx.json").read_text(encoding="utf-8"))
    if not str(sbom.get("spdxVersion", "")).startswith("SPDX-") or not sbom.get(
        "SPDXID"
    ):
        raise AssetError("SBOM.spdx.json is not an SPDX JSON document")

    core = release.core_version(root).maven
    archive = directory / f"lakesoul-{core}-src.tar.gz"
    expected_prefix = f"lakesoul-{core}-src/"
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        if not members or any(
            member.name != expected_prefix.removesuffix("/")
            and not member.name.startswith(expected_prefix)
            for member in members
        ):
            raise AssetError("source archive has an invalid top-level directory")

    if signature:
        detached = directory / "SHA256SUMS.asc"
        if not detached.is_file():
            raise AssetError("missing SHA256SUMS.asc")
        result = subprocess.run(
            ["gpg", "--batch", "--verify", str(detached), str(manifest)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode:
            raise AssetError(f"invalid SHA256SUMS signature: {result.stderr.strip()}")


def _download(url: str, destination: Path, retries: int, delay: float) -> None:
    error: Exception | None = None
    for attempt in range(retries):
        try:
            with (
                urllib.request.urlopen(url, timeout=60) as response,
                destination.open("wb") as output,
            ):
                shutil.copyfileobj(response, output)
            if destination.stat().st_size == 0:
                raise AssetError(f"downloaded empty artifact: {url}")
            return
        except (OSError, urllib.error.URLError) as caught:
            error = caught
            destination.unlink(missing_ok=True)
            if attempt + 1 < retries:
                time.sleep(delay)
    raise AssetError(f"cannot download {url}: {error}")


def download_central(
    output: Path,
    root: Path = ROOT,
    *,
    base_url: str = DEFAULT_CENTRAL,
    retries: int = 12,
    delay: float = 30,
) -> None:
    core = release.core_version(root)
    if core.snapshot:
        raise AssetError("Maven Central verification requires a final Core version")
    version = core.maven
    artifact_ids = release.expected_artifact_ids(root)
    output.mkdir(parents=True, exist_ok=True)
    for pom in CENTRAL_POMS:
        artifact_id = artifact_ids[pom]
        relative = f"com/dmetasoul/{artifact_id}/{version}"
        suffixes = [".pom", ".pom.asc"]
        if pom != "pom.xml":
            suffixes.extend(
                [
                    ".jar",
                    ".jar.asc",
                    "-sources.jar",
                    "-sources.jar.asc",
                    "-javadoc.jar",
                    "-javadoc.jar.asc",
                ]
            )
        for suffix in suffixes:
            name = f"{artifact_id}-{version}{suffix}"
            _download(
                f"{base_url.rstrip('/')}/{relative}/{name}",
                output / name,
                retries,
                delay,
            )

    for pom in GA_CONNECTOR_POMS:
        name = f"{artifact_ids[pom]}-{version}.jar"
        with zipfile.ZipFile(output / name) as jar:
            entries = set(jar.namelist())
            if not any(
                entry.startswith("META-INF/native/linux-x86_64/") for entry in entries
            ):
                raise AssetError(f"published connector lacks native resources: {name}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    assemble_parser = subparsers.add_parser("assemble")
    assemble_parser.add_argument("output", type=Path)
    assemble_parser.add_argument("--commit", default="HEAD")
    checksums = subparsers.add_parser("checksums")
    checksums.add_argument("directory", type=Path)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("directory", type=Path)
    verify_parser.add_argument("--signature", action="store_true")
    central = subparsers.add_parser("download-central")
    central.add_argument("output", type=Path)
    central.add_argument("--base-url", default=DEFAULT_CENTRAL)
    central.add_argument("--retries", type=int, default=12)
    central.add_argument("--delay", type=float, default=30)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.operation == "assemble":
            assemble(args.output, args.root, commit=args.commit)
        elif args.operation == "checksums":
            write_checksums(args.directory, args.root)
        elif args.operation == "verify":
            verify(args.directory, args.root, signature=args.signature)
        else:
            download_central(
                args.output,
                args.root,
                base_url=args.base_url,
                retries=args.retries,
                delay=args.delay,
            )
    except (AssetError, OSError, ValueError, release.ReleaseError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
