# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import io
import os
import re
import json
import subprocess

class ManylinuxDockerImageBuilder(object):
    def __init__(self):
        self._dockerfile_dir = self._get_dockerfile_dir()
        self._project_root_dir = self._get_project_root_dir()
        self._pyproject_toml_path = self._get_pyproject_toml_path()
        self._manylinux_image_name = self._get_manylinux_image_name()

    def _get_dockerfile_dir(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return dir_path

    def _get_project_root_dir(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.dirname(os.path.dirname(dir_path))
        return dir_path

    def _get_pyproject_toml_path(self):
        file_path = os.path.join(self._project_root_dir, 'pyproject.toml')
        return file_path

    def _get_manylinux_image_name(self):
        with io.open(self._pyproject_toml_path) as fin:
            text = fin.read()
        match = re.search('^manylinux-x86_64-image = "(.+)"$', text, re.M)
        if match is None:
            message = "fail to read manylinux image name "
            message += "from %r" % self._pyproject_toml_path
            raise RuntimeError(message)
        image_name = match.group(1)
        return image_name

    def _parse_args(self):
        import argparse
        parser = argparse.ArgumentParser(description='build manylinux docker image')
        parser.add_argument('-f', '--force-rebuild', action='store_true',
            help='force rebuilding of the docker image')
        args = parser.parse_args()
        self._force_rebuild = args.force_rebuild

    def _docker_image_exists(self):
        args = ['docker', 'images', '--format', 'json']
        output = subprocess.check_output(args)
        for line in output.splitlines():
            image = json.loads(line)
            name = '%s:%s' % (image['Repository'], image['Tag'])
            if name == self._manylinux_image_name:
                return True
        return False

    def _handle_proxy(self, args):
        import os
        http_proxy = os.environ.get('http_proxy')
        https_proxy = os.environ.get('https_proxy')
        no_proxy = os.environ.get('no_proxy')
        if http_proxy is not None or https_proxy is not None or no_proxy is not None:
            args += ['--network', 'host']
            if http_proxy is not None:
                args += ['--build-arg', 'http_proxy=%s' % http_proxy]
            if https_proxy is not None:
                args += ['--build-arg', 'https_proxy=%s' % https_proxy]
            if no_proxy is not None:
                args += ['--build-arg', 'no_proxy=%s' % no_proxy]

    def _build_docker_image(self):
        args = ['docker', 'build']
        self._handle_proxy(args)
        args += ['--tag', self._manylinux_image_name]
        args += [self._dockerfile_dir]
        subprocess.check_call(args)

    def run(self):
        self._parse_args()
        if not self._docker_image_exists() or self._force_rebuild:
            self._build_docker_image()

def main():
    builder = ManylinuxDockerImageBuilder()
    builder.run()

if __name__ == '__main__':
    main()
