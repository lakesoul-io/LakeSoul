# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from script import check_release_policy, verify_release_artifacts


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
