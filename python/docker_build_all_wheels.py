# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import io
import os
import re
import sys
import shutil
import tempfile
import textwrap
import subprocess
import contextlib

#
# Build all LakeSoul wheels with docker via cibuildwheel.
#
class LakeSoulWheelDockerBuilder(object):
    def __init__(self):
        self._project_root_dir = self._get_project_root_dir()

    def _get_project_root_dir(self):
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return dir_path

    def _build_docker_image(self):
        script_path = 'docker/lakesoul-python-wheel-build-env/build_docker_image.py'
        script_path = os.path.join(self._project_root_dir, script_path)
        args = [sys.executable, script_path]
        subprocess.check_call(args)

    def _patch_cibuildwheel(self, dir_path):
        version = 'python%d.%d' % (sys.version_info.major, sys.version_info.minor)
        site_path = os.path.join(dir_path, 'lib', version, 'site-packages')
        file_path = os.path.join(site_path, 'cibuildwheel', 'oci_container.py')
        with io.open(file_path) as fin:
            text = fin.read()
        pattern = r'^( +network_args = \["--network=host"])$\n\n( +)(shell_args = \[)'
        match = re.search(pattern, text, re.M)
        if match is None:
            return
        source = """
            import os
            http_proxy = os.environ.get("http_proxy")
            https_proxy = os.environ.get("https_proxy")
            no_proxy = os.environ.get("no_proxy")
            if http_proxy is not None or https_proxy is not None or no_proxy is not None:
                network_args += ["--network", "host"]
                if http_proxy is not None:
                    network_args += ["--env", "http_proxy=%s" % http_proxy]
                if https_proxy is not None:
                    network_args += ["--env", "https_proxy=%s" % https_proxy]
                if no_proxy is not None:
                    network_args += ["--env", "no_proxy=%s" % no_proxy]
        """
        source = textwrap.dedent(source)
        source = textwrap.indent(source, match.group(2) + ' ' * 4)
        string = match.group(1)
        string += '\n' + match.group(2) + 'else:'
        string += source
        string += '\n' + match.group(2) + match.group(3)
        text = re.sub(pattern, string, text, 1, re.M)
        with io.open(file_path, 'w') as fout:
            fout.write(text)

    def _venv_context(self):
        @contextlib.contextmanager
        def context():
            try:
                dir_path = tempfile.mkdtemp()
                args = [sys.executable, '-m', 'venv', dir_path]
                subprocess.check_call(args)
                py_path = os.path.join(dir_path, 'bin', 'python')
                args = [py_path, '-m', 'pip', 'install', '--upgrade', 'pip']
                subprocess.check_call(args)
                args = [py_path, '-m', 'pip', 'install', 'cibuildwheel~=2.15']
                subprocess.check_call(args)
                self._patch_cibuildwheel(dir_path)
                yield dir_path
            finally:
                shutil.rmtree(dir_path)
        return context()

    def _build_all_wheels(self, dir_path):
        cibw_path = os.path.join(dir_path, 'bin', 'cibuildwheel')
        args = [cibw_path, '--platform', 'linux']
        subprocess.check_call(args)

    def run(self):
        self._build_docker_image()
        with self._venv_context() as dir_path:
            self._build_all_wheels(dir_path)

def main():
    builder = LakeSoulWheelDockerBuilder()
    builder.run()

if __name__ == '__main__':
    main()
