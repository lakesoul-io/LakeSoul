# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY / "script/build-env/build-rust.sh"


class BuildEnvRustTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.project = Path(temporary.name)
        (self.project / "python").mkdir()
        self.source = self.project / "rust/target/x86_64-unknown-linux-gnu/release/deps"
        self.destination = self.project / "rust/target/release"
        self.source.mkdir(parents=True)
        (self.destination / "deps").mkdir(parents=True)
        for name in (
            "liblakesoul_io_c.so",
            "liblakesoul_metadata_c.so",
            "liblakesoul_python.so",
            "liblakesoul_other.so",
        ):
            (self.source / name).write_bytes(b"new-" + name.encode())
        self.python_library = self.destination / "liblakesoul_python.so"
        self.python_library.write_bytes(b"python-without-hdfs")
        self.python_dependency = self.destination / "deps/liblakesoul_python.so"
        os.link(self.python_library, self.python_dependency)

        binaries = self.project / "bin"
        binaries.mkdir()
        # Execute the real container command in the temporary project, without Docker or builds.
        docker = binaries / "docker"
        docker.write_text(
            "#!/usr/bin/env python3\n"
            "import os, subprocess, sys\n"
            "assert sys.argv[-3:-1] == ['bash', '-c']\n"
            "raise SystemExit(subprocess.call(sys.argv[-3:], "
            "cwd=os.environ['FAKE_BUILD_PROJECT']))\n"
        )
        docker.chmod(0o755)
        for name in ("rustc", "cargo", "uvx"):
            executable = binaries / name
            executable.write_text("#!/usr/bin/env bash\nexit 0\n")
            executable.chmod(0o755)
        self.environment = {
            **os.environ,
            "HOME": str(self.project / "home"),
            "USER": "build-env-test",
            "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
            "FAKE_BUILD_PROJECT": str(self.project),
        }

    def run_script(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT)],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_copies_only_java_libraries_and_preserves_python_hardlinks(self) -> None:
        result = self.run_script()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        for name in ("liblakesoul_io_c.so", "liblakesoul_metadata_c.so"):
            self.assertEqual(
                (self.source / name).read_bytes(),
                (self.destination / name).read_bytes(),
            )
        self.assertEqual(b"python-without-hdfs", self.python_library.read_bytes())
        self.assertEqual(b"python-without-hdfs", self.python_dependency.read_bytes())
        self.assertTrue(self.python_library.samefile(self.python_dependency))
        self.assertFalse((self.destination / "liblakesoul_other.so").exists())

    def test_fails_when_a_required_java_library_is_missing(self) -> None:
        (self.source / "liblakesoul_metadata_c.so").unlink()
        result = self.run_script()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("liblakesoul_metadata_c.so", result.stderr)


if __name__ == "__main__":
    unittest.main()
