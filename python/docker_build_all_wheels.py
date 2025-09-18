# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import shutil
import tempfile
import subprocess
import contextlib


#
# Build all LakeSoul wheels with docker via cibuildwheel.
#

CI_BUILD_WHEEL_VERSION = "2.19"


class LakeSoulWheelDockerBuilder(object):
    def __init__(self):
        self._project_root_dir = self._get_project_root_dir()

    def _get_project_root_dir(self):
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return dir_path

    def _build_docker_image(self):
        script_path = "docker/lakesoul-python-wheel-build-env/build_docker_image.py"
        script_path = os.path.join(self._project_root_dir, script_path)
        args = [sys.executable, script_path]
        subprocess.check_call(args)

    def _venv_context(self):
        @contextlib.contextmanager
        def context():
            try:
                dir_path = tempfile.mkdtemp()
                yield dir_path
            finally:
                shutil.rmtree(dir_path)  # type: ignore

        return context()

    def _build_all_wheels(self, dir_path):
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        args = [
            "uvx",
            "-p",
            py_version,
            f"cibuildwheel@{CI_BUILD_WHEEL_VERSION}",
            "--platform",
            "linux",
            "--output-dir",
            "dist",
        ]
        subprocess.check_call(args)

    def run(self):
        self._build_docker_image()
        with self._venv_context() as dir_path:
            self._build_all_wheels(dir_path)


def main():
    builder = LakeSoulWheelDockerBuilder()
    builder.run()


if __name__ == "__main__":
    main()
