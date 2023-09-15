# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import os
import glob
import shutil
import tempfile
import subprocess
import contextlib

#
# Build LakeSoul wheel on host machine directly.
#
# This script is tested with the following configuration:
#
#   * CentOS 7
#   * GCC 11 (both devtoolset-11 and GCC 11 built from source)
#   * Rust nightly
#   * Python 3.8 (installed via pyenv)
#   * CMake 3.26
#   * Ninja 1.11
#
# Use docker_build_all_wheels.py to build with docker via cibuildwheel.
#
class LakeSoulWheelHostBuilder(object):
    def __init__(self):
        self._project_root_dir = self._get_project_root_dir()

    def _get_project_root_dir(self):
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return dir_path

    def _get_python_path(self):
        dir_path = os.path.join(self._project_root_dir, 'cpp/build/lakesoul')
        file_path = os.path.join(dir_path, 'python-env/.env/bin/python')
        return file_path

    def _get_python_version(self):
        string = "import sys; "
        string += "m = sys.version_info.major; "
        string += "n = sys.version_info.minor; "
        string += "print('%d.%d' % (m, n))"
        args = [self._get_python_path(), '-c', string]
        output = subprocess.check_output(args)
        if not isinstance(output, str):
            output = output.decode('utf-8')
        version = output.strip()
        return version

    def _get_pyarrow_abi_tag(self):
        string = "import pyarrow; "
        string += "xs = pyarrow.__version__.split('.'); "
        string += "m = int(xs[0]); "
        string += "n = int(xs[1]); "
        string += "print('%d%02d' % (m, n))"
        args = [self._get_python_path(), '-c', string]
        output = subprocess.check_output(args)
        if not isinstance(output, str):
            output = output.decode('utf-8')
        abi_tag = output.strip()
        return abi_tag

    def _compile_cpp(self):
        file_path = os.path.join(self._project_root_dir, 'cpp', 'compile.sh')
        args = file_path,
        subprocess.check_call(args)

    def _clear_files(self):
        dir_path = os.path.join(self._project_root_dir, 'python', 'lakesoul.egg-info')
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
        dir_path = os.path.join(self._project_root_dir, 'build', 'bdist.linux-x86_64')
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
        python_version = self._get_python_version()
        dir_name = 'lib.linux-x86_64-cpython-%s' % python_version.replace('.', '')
        dir_path = os.path.join(self._project_root_dir, 'build', dir_name)
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
        dir_path = os.path.join(self._project_root_dir, 'build')
        if os.path.isdir(dir_path):
            os.rmdir(dir_path)

    def _wheel_context(self):
        @contextlib.contextmanager
        def context():
            saved_path = os.getcwd()
            self._clear_files()
            try:
                dir_path = tempfile.mkdtemp()
                os.chdir(dir_path)
                yield
            finally:
                os.chdir(saved_path)
                shutil.rmtree(dir_path)
                self._clear_files()
        return context()

    def _get_wheel_path(self):
        files = glob.glob('lakesoul-*.whl')
        assert len(files) == 1
        file_path = files[0]
        file_path = os.path.realpath(file_path)
        return file_path

    def _get_repaired_wheel_path(self):
        files = glob.glob('wheelhouse/lakesoul-*.whl')
        assert len(files) == 1
        file_path = files[0]
        file_path = os.path.realpath(file_path)
        return file_path

    def _build_wheel(self):
        args = ['env']
        args += ['_LAKESOUL_DATASET_SO=%s' % os.path.join(
                 self._project_root_dir, 'cpp/build/_lakesoul_dataset.so')]
        args += ['_LAKESOUL_METADATA_SO=%s' % os.path.join(
                 self._project_root_dir, 'rust/target/release/liblakesoul_metadata_c.so')]
        args += ['_LAKESOUL_METADATA_GENERATED=%s' % os.path.join(
                 self._project_root_dir, 'cpp/build/python/lakesoul/metadata/generated')]
        args += [self._get_python_path(), '-m', 'pip', 'wheel', self._project_root_dir, '--no-deps']
        subprocess.check_call(args)

    def _repair_wheel(self):
        pa_abi_tag = self._get_pyarrow_abi_tag()
        args = [self._get_python_path(), '-m', 'auditwheel', 'repair', '--plat', 'manylinux2014_x86_64']
        args += ['--exclude', 'libarrow_python.so']
        args += ['--exclude', 'libarrow_dataset.so.%s' % pa_abi_tag]
        args += ['--exclude', 'libarrow_acero.so.%s' % pa_abi_tag]
        args += ['--exclude', 'libparquet.so.%s' % pa_abi_tag]
        args += ['--exclude', 'libarrow.so.%s' % pa_abi_tag]
        args += [self._get_wheel_path()]
        subprocess.check_call(args)

    def _copy_wheel(self):
        dir_path = os.path.join(self._project_root_dir, 'wheelhouse')
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)
        wheel_path = self._get_repaired_wheel_path()
        shutil.copy(wheel_path, dir_path)

    def run(self):
        self._compile_cpp()
        with self._wheel_context():
            self._build_wheel()
            self._repair_wheel()
            self._copy_wheel()

def main():
    builder = LakeSoulWheelHostBuilder()
    builder.run()

if __name__ == '__main__':
    main()
