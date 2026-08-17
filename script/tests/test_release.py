# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import io
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "lakesoul_release", REPOSITORY / "script/release.py"
)
assert SPEC is not None and SPEC.loader is not None
release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release
SPEC.loader.exec_module(release)

FIXTURE_FILES = [
    "pom.xml",
    "Cargo.toml",
    "lakesoul-common/pom.xml",
    "native-io/lakesoul-io-java/pom.xml",
    "lakesoul-spark/pom.xml",
    "lakesoul-flink/pom.xml",
    "lakesoul-presto/pom.xml",
    "lakesoul-spark-gluten/pom.xml",
    "python/pyproject.toml",
    "python/Cargo.toml",
    "website/docusaurus.config.js",
]
FIXTURE_FILES.extend(
    path.relative_to(REPOSITORY).as_posix()
    for path in REPOSITORY.glob("rust/*/Cargo.toml")
)


class ReleaseToolTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        for relative in FIXTURE_FILES:
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPOSITORY / relative, destination)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def run_main(self, *arguments: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = release.main(list(arguments), root=self.root)
        return result, stdout.getvalue(), stderr.getvalue()

    def test_repository_versions_are_synchronized(self) -> None:
        self.assertEqual([], release.validate(self.root))

    def test_check_reports_core_cargo_mismatch(self) -> None:
        cargo = self.root / "Cargo.toml"
        cargo.write_text(
            cargo.read_text(encoding="utf-8").replace(
                'version = "4.0.0-dev.0"', 'version = "4.0.1-dev.0"', 1
            ),
            encoding="utf-8",
        )

        result, _, stderr = self.run_main("check")

        self.assertEqual(1, result)
        self.assertIn("workspace version '4.0.1-dev.0' != '4.0.0-dev.0'", stderr)

    def test_check_reports_core_crate_version_override(self) -> None:
        cargo = self.root / "rust/lakesoul-io/Cargo.toml"
        cargo.write_text(
            cargo.read_text(encoding="utf-8").replace(
                "version.workspace = true", 'version = "4.0.1-dev.0"', 1
            ),
            encoding="utf-8",
        )

        result, _, stderr = self.run_main("check")

        self.assertEqual(1, result)
        self.assertIn("package version '4.0.1-dev.0' != '4.0.0-dev.0'", stderr)

    def test_check_requires_publish_false_for_every_crate(self) -> None:
        cargo = self.root / "rust/lakesoul-common/Cargo.toml"
        cargo.write_text(
            cargo.read_text(encoding="utf-8").replace("publish = false\n", "", 1),
            encoding="utf-8",
        )

        result, _, stderr = self.run_main("check")

        self.assertEqual(1, result)
        self.assertIn("package must set publish = false", stderr)

    def test_check_requires_experimental_crate_marker(self) -> None:
        cargo = self.root / "rust/lakesoul-flight/Cargo.toml"
        cargo.write_text(
            cargo.read_text(encoding="utf-8").replace("Experimental ", "", 1),
            encoding="utf-8",
        )

        result, _, stderr = self.run_main("check")

        self.assertEqual(1, result)
        self.assertIn("Experimental crate must be marked outside Core GA", stderr)

    def test_check_rejects_cargo_publish_in_workflow(self) -> None:
        workflow = self.root / ".github/workflows/release.yml"
        workflow.parent.mkdir(parents=True)
        workflow.write_text("steps:\n  - run: cargo publish\n", encoding="utf-8")

        result, _, stderr = self.run_main("check")

        self.assertEqual(1, result)
        self.assertIn("release workflows must not run cargo publish", stderr)

    def test_check_reports_runtime_artifact_id_mismatch(self) -> None:
        pom = self.root / "lakesoul-spark/pom.xml"
        pom.write_text(
            pom.read_text(encoding="utf-8").replace(
                "lakesoul-spark-3.5_2.12", "lakesoul-spark", 1
            ),
            encoding="utf-8",
        )

        result, _, stderr = self.run_main("check")

        self.assertEqual(1, result)
        self.assertIn(
            "artifactId 'lakesoul-spark' != 'lakesoul-spark-3.5_2.12'", stderr
        )

    def test_set_core_synchronizes_development_versions_only(self) -> None:
        result, stdout, stderr = self.run_main("set-core", "4.1.0-SNAPSHOT")

        self.assertEqual(0, result, stderr)
        self.assertIn("Updating pom.xml", stdout)
        self.assertIn("Updating Cargo.toml", stdout)
        self.assertEqual("4.1.0-SNAPSHOT", release.core_version(self.root).maven)
        self.assertIn(
            'version = "4.1.0-dev.0"',
            (self.root / "Cargo.toml").read_text(encoding="utf-8"),
        )
        self.assertEqual("3.0.0", str(release.website_version(self.root)))

    def test_set_core_final_synchronizes_website(self) -> None:
        result, _, stderr = self.run_main("set-core", "4.0.0")

        self.assertEqual(0, result, stderr)
        self.assertEqual("4.0.0", release.core_version(self.root).maven)
        self.assertIn(
            'version = "4.0.0"',
            (self.root / "Cargo.toml").read_text(encoding="utf-8"),
        )
        self.assertEqual("4.0.0", str(release.website_version(self.root)))
        self.assertEqual([], release.validate(self.root))

    def test_set_python_maps_pep440_development_version_to_cargo(self) -> None:
        result, _, stderr = self.run_main("set-python", "2.1.0.dev0")

        self.assertEqual(0, result, stderr)
        self.assertEqual("2.1.0.dev0", release.python_version(self.root).python)
        self.assertIn(
            'version = "2.1.0-dev.0"',
            (self.root / "python/Cargo.toml").read_text(encoding="utf-8"),
        )
        self.assertEqual([], release.validate(self.root))

    def test_set_check_mode_reports_without_modifying(self) -> None:
        before = (self.root / "pom.xml").read_text(encoding="utf-8")

        result, stdout, stderr = self.run_main("set-core", "4.1.0-SNAPSHOT", "--check")

        self.assertEqual(1, result)
        self.assertIn("Would update pom.xml", stdout)
        self.assertIn("version files are not synchronized", stderr)
        self.assertEqual(before, (self.root / "pom.xml").read_text(encoding="utf-8"))

    def test_core_and_python_tags_require_matching_final_versions(self) -> None:
        self.assertEqual(0, self.run_main("set-core", "4.0.0")[0])
        self.assertEqual(0, self.run_main("check-tag", "v4.0.0")[0])
        self.assertEqual(0, self.run_main("set-python", "2.0.0")[0])
        self.assertEqual(0, self.run_main("check-tag", "py-v2.0.0")[0])

    def test_tags_reject_development_and_unsupported_formats(self) -> None:
        result, _, stderr = self.run_main("check-tag", "v4.0.0")
        self.assertEqual(1, result)
        self.assertIn("requires a final Core version", stderr)

        result, _, stderr = self.run_main("check-tag", "release-4.0.0")
        self.assertEqual(1, result)
        self.assertIn("unsupported tag", stderr)

    def test_set_operations_reject_unsupported_versions(self) -> None:
        for operation, version in (
            ("set-core", "4.0"),
            ("set-core", "04.0.0-SNAPSHOT"),
            ("set-python", "2.0.0-dev.0"),
        ):
            with self.subTest(operation=operation, version=version):
                result, _, stderr = self.run_main(operation, version)
                self.assertEqual(1, result)
                self.assertIn("unsupported", stderr)


if __name__ == "__main__":
    unittest.main()
