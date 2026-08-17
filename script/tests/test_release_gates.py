# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import gzip
import json
import tarfile
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from script import check_release_policy, release_assets, verify_release_artifacts


class ReleaseArtifactGateTest(unittest.TestCase):
    def test_effective_coordinates_must_match_core_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            effective = root / "effective.xml"
            effective.write_text(
                """<?xml version="1.0"?>
<projects xmlns="http://maven.apache.org/POM/4.0.0">
  <project><groupId>com.dmetasoul</groupId><artifactId>lakesoul-parent</artifactId><version>4.0.0</version></project>
  <project><groupId>com.dmetasoul</groupId><artifactId>lakesoul-common</artifactId><version>4.0.1</version></project>
</projects>
""",
                encoding="utf-8",
            )
            with (
                patch.object(
                    verify_release_artifacts,
                    "ARTIFACT_MODULES",
                    {"lakesoul-common/pom.xml": "lakesoul-common"},
                ),
                patch.object(
                    verify_release_artifacts.release,
                    "core_version",
                    return_value=SimpleNamespace(maven="4.0.0"),
                ),
                patch.object(
                    verify_release_artifacts.release,
                    "expected_artifact_ids",
                    return_value={"lakesoul-common/pom.xml": "lakesoul-common"},
                ),
            ):
                with self.assertRaisesRegex(
                    verify_release_artifacts.VerificationError,
                    "effective coordinates",
                ):
                    verify_release_artifacts.verify_coordinates(effective, root)

    def test_artifact_gate_requires_real_sources_jar(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "module/target"
            target.mkdir(parents=True)
            for suffix, member in (
                ("", "com/example/Main.class"),
                ("-sources", "com/example/Main.java"),
                ("-javadoc", "META-INF/MANIFEST.MF"),
            ):
                with zipfile.ZipFile(
                    target / f"artifact-4.0.0{suffix}.jar", "w"
                ) as jar:
                    jar.writestr(member, "content")
            with (
                patch.object(
                    verify_release_artifacts,
                    "ARTIFACT_MODULES",
                    {"module/pom.xml": "artifact"},
                ),
                patch.object(
                    verify_release_artifacts.release,
                    "core_version",
                    return_value=SimpleNamespace(maven="4.0.0"),
                ),
                patch.object(
                    verify_release_artifacts.release,
                    "expected_artifact_ids",
                    return_value={"module/pom.xml": "artifact"},
                ),
            ):
                verify_release_artifacts.verify_artifacts(root)

                sources = target / "artifact-4.0.0-sources.jar"
                with zipfile.ZipFile(sources, "w") as jar:
                    jar.writestr("META-INF/MANIFEST.MF", "content")
                with self.assertRaisesRegex(
                    verify_release_artifacts.VerificationError,
                    "has no Java or Scala source",
                ):
                    verify_release_artifacts.verify_artifacts(root)

    def test_release_asset_manifest_covers_exact_asset_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            assets = Path(directory)
            names = [
                "connector.jar",
                "lakesoul-4.0.0-src.tar.gz",
                "SBOM.spdx.json",
            ]
            (assets / "connector.jar").write_bytes(b"jar")
            (assets / "SBOM.spdx.json").write_text(
                json.dumps({"spdxVersion": "SPDX-2.3", "SPDXID": "SPDXRef-DOCUMENT"}),
                encoding="utf-8",
            )
            tar_data = BytesIO()
            with tarfile.open(fileobj=tar_data, mode="w") as archive:
                archive.addfile(tarfile.TarInfo("lakesoul-4.0.0-src"))
                member = tarfile.TarInfo("lakesoul-4.0.0-src/README.md")
                content = b"source"
                member.size = len(content)
                archive.addfile(member, BytesIO(content))
            archive_path = assets / "lakesoul-4.0.0-src.tar.gz"
            with (
                archive_path.open("wb") as raw,
                gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=raw,
                    mtime=0,
                ) as compressed,
            ):
                compressed.write(tar_data.getvalue())

            with (
                patch.object(
                    release_assets, "expected_asset_names", return_value=names
                ),
                patch.object(
                    release_assets.release,
                    "core_version",
                    return_value=SimpleNamespace(maven="4.0.0"),
                ),
            ):
                release_assets.write_checksums(assets)
                release_assets.verify(assets)
                (assets / "connector.jar").write_bytes(b"changed")
                with self.assertRaisesRegex(
                    release_assets.AssetError, "checksum mismatch"
                ):
                    release_assets.verify(assets)

    def test_central_download_requires_all_classifiers_and_signatures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = root / "repository"
            output = root / "output"
            artifact_id = "connector"
            version = "4.0.0"
            coordinate = repository / "com/dmetasoul" / artifact_id / version
            coordinate.mkdir(parents=True)
            for suffix in (
                ".pom",
                ".pom.asc",
                ".jar.asc",
                "-sources.jar",
                "-sources.jar.asc",
                "-javadoc.jar",
                "-javadoc.jar.asc",
            ):
                (coordinate / f"{artifact_id}-{version}{suffix}").write_bytes(
                    b"artifact"
                )
            with zipfile.ZipFile(
                coordinate / f"{artifact_id}-{version}.jar", "w"
            ) as jar:
                jar.writestr(
                    "META-INF/native/linux-x86_64/liblakesoul_io_c.so", b"native"
                )

            with (
                patch.object(release_assets, "CENTRAL_POMS", ("module/pom.xml",)),
                patch.object(release_assets, "CONNECTOR_POMS", ("module/pom.xml",)),
                patch.object(release_assets, "GA_CONNECTOR_POMS", ("module/pom.xml",)),
                patch.object(
                    release_assets.release,
                    "core_version",
                    return_value=SimpleNamespace(maven=version, snapshot=False),
                ),
                patch.object(
                    release_assets.release,
                    "expected_artifact_ids",
                    return_value={"module/pom.xml": artifact_id},
                ),
            ):
                release_assets.download_central(
                    output,
                    root,
                    base_url=repository.as_uri(),
                    retries=1,
                    delay=0,
                )

            self.assertEqual(
                hashlib.sha256(
                    (coordinate / f"{artifact_id}-{version}.jar").read_bytes()
                ).hexdigest(),
                hashlib.sha256(
                    (output / f"{artifact_id}-{version}.jar").read_bytes()
                ).hexdigest(),
            )


class ReleasePolicyGateTest(unittest.TestCase):
    def test_git_dependencies_require_immutable_revision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "python").mkdir()
            (root / "Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
            (root / "python/Cargo.toml").write_text(
                """
[package]
name = "fixture"
version = "1.0.0"
publish = false

[dependencies]
safe = { git = "https://example.invalid/safe", rev = "abc123" }
floating = { git = "https://example.invalid/floating", branch = "main" }
""",
                encoding="utf-8",
            )
            errors: list[str] = []
            check_release_policy.check_cargo(root, errors)
            self.assertEqual(
                [
                    "python/Cargo.toml: Git dependency floating must pin rev",
                ],
                errors,
            )

    def test_repository_release_policy_is_satisfied(self) -> None:
        self.assertEqual([], check_release_policy.validate())


if __name__ == "__main__":
    unittest.main()
