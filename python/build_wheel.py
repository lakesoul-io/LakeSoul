# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

class LakeSoulWheelBuilder(object):
    def __init__(self):
        self._project_root_dir = self._get_project_root_dir()

    def _get_project_root_dir(self):
        import os
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        return dir_path

    def _compile_cpp(self):
        import os
        import subprocess
        file_path = os.path.join(self._project_root_dir, 'cpp', 'compile.sh')
        args = file_path,
        subprocess.check_call(args)

    def _clear_files(self):
        import os
        import shutil
        dir_path = os.path.join(self._project_root_dir, 'python', 'build')
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
        dir_path = os.path.join(self._project_root_dir, 'python', 'lakesoul.egg-info')
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)

    def _wheel_context(self):
        import os
        import shutil
        import tempfile
        import contextlib
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
        import os
        import glob
        files = glob.glob('lakesoul-*.whl')
        assert len(files) == 1
        file_path = files[0]
        file_path = os.path.realpath(file_path)
        return file_path

    def _get_repaired_wheel_path(self):
        import os
        import glob
        files = glob.glob('wheelhouse/lakesoul-*.whl')
        assert len(files) == 1
        file_path = files[0]
        file_path = os.path.realpath(file_path)
        return file_path

    def _get_pyarrow_abi_tag(self):
        import os
        import subprocess
        string = "import pyarrow; "
        string += "xs = pyarrow.__version__.split('.'); "
        string += "m = int(xs[0]); "
        string += "n = int(xs[1]); "
        string += "print('%d%02d' % (m, n))"
        args = [os.path.join(self._project_root_dir, 'cpp/build/lakesoul/python-env/.env/bin/python')]
        args += ['-c', string]
        output = subprocess.check_output(args)
        if not isinstance(output, str):
            output = output.decode('utf-8')
        abi_tag = output.strip()
        return abi_tag

    def _build_wheel(self):
        import os
        import subprocess
        args = ['env']
        args += ['_LAKESOUL_DATASET_SO=%s' % os.path.join(
                 self._project_root_dir, 'cpp/build/_lakesoul_dataset.so')]
        args += ['_LAKESOUL_METADATA_SO=%s' % os.path.join(
                 self._project_root_dir, 'native-metadata/target/release/liblakesoul_metadata_c.so')]
        args += ['_LAKESOUL_METADATA_GENERATED=%s' % os.path.join(
                 self._project_root_dir, 'cpp/build/python/lakesoul/metadata/generated')]
        args += [os.path.join(self._project_root_dir, 'cpp/build/lakesoul/python-env/.env/bin/python')]
        args += ['-m', 'pip', 'wheel', os.path.join(self._project_root_dir, 'python'), '--no-deps']
        subprocess.check_call(args)

    def _repair_wheel(self):
        import os
        import subprocess
        abi_tag = self._get_pyarrow_abi_tag()
        args = [os.path.join(self._project_root_dir, 'cpp/build/lakesoul/python-env/.env/bin/python')]
        args += ['-m', 'auditwheel', 'repair', '--plat', 'manylinux2014_x86_64']
        args += ['--exclude', 'libarrow_python.so']
        args += ['--exclude', 'libarrow_dataset.so.%s' % abi_tag]
        args += ['--exclude', 'libarrow_acero.so.%s' % abi_tag]
        args += ['--exclude', 'libparquet.so.%s' % abi_tag]
        args += ['--exclude', 'libarrow.so.%s' % abi_tag]
        args += [self._get_wheel_path()]
        subprocess.check_call(args)

    def _copy_wheel(self):
        import os
        import shutil
        dir_path = os.path.join(self._project_root_dir, 'python', 'wheelhouse')
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
    builder = LakeSoulWheelBuilder()
    builder.run()

if __name__ == '__main__':
    main()
