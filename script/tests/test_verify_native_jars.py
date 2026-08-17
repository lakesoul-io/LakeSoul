# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "verify_native_jars", REPOSITORY / "script/verify_native_jars.py"
)
assert SPEC is not None and SPEC.loader is not None
verify_native_jars = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verify_native_jars
SPEC.loader.exec_module(verify_native_jars)


class VerifyNativeJarsTest(unittest.TestCase):
    def test_accepts_nonempty_required_libraries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "release.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                for library in verify_native_jars.REQUIRED_LIBRARIES:
                    archive.writestr(library, b"native")

            self.assertEqual([], verify_native_jars.verify_jar(jar))

    def test_rejects_missing_library(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "release.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                archive.writestr(verify_native_jars.REQUIRED_LIBRARIES[0], b"native")

            errors = verify_native_jars.verify_jar(jar)

            self.assertEqual(1, len(errors))
            self.assertIn(verify_native_jars.REQUIRED_LIBRARIES[1], errors[0])

    def test_rejects_empty_library(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "release.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                archive.writestr(verify_native_jars.REQUIRED_LIBRARIES[0], b"")
                archive.writestr(verify_native_jars.REQUIRED_LIBRARIES[1], b"native")

            errors = verify_native_jars.verify_jar(jar)

            self.assertEqual(1, len(errors))
            self.assertIn("is empty", errors[0])


if __name__ == "__main__":
    unittest.main()
