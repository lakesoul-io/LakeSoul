# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from setuptools import setup
from setuptools import Extension
from setuptools.command.build_ext import build_ext

class LakeSoulDatasetExtension(Extension):
    def __init__(self, name):
        super().__init__(name, sources=[])

class lakesoul_build_ext(build_ext):
    def run(self):
        for ext in self.extensions:
            self._build_lakesoul(ext)

    def _get_project_root_dir(self):
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return dir_path

    def _get_lakesoul_dataset_so_path(self):
        import os
        key = '_LAKESOUL_DATASET_SO'
        path = os.environ.get(key)
        if path is not None and os.path.isfile(path):
            return path
        path = os.path.join(self._get_project_root_dir(), 'cpp/build/_lakesoul_dataset.so')
        if os.path.isfile(path):
            return path
        message = "'_lakesoul_dataset.so' is not specified via "
        message += "environment variable %r " % key
        message += "nor found at default location %r " % path
        raise RuntimeError(message)

    def _get_lakesoul_metadata_so_path(self):
        import os
        key = '_LAKESOUL_METADATA_SO'
        path = os.environ.get(key)
        if path is not None and os.path.isfile(path):
            return path
        path = os.path.join(self._get_project_root_dir(), 'rust/target/release/liblakesoul_metadata_c.so')
        if os.path.isfile(path):
            return path
        message = "'liblakesoul_metadata_c.so' is not specified via "
        message += "environment variable %r " % key
        message += "nor found at default location %r " % path
        raise RuntimeError(message)

    def _get_lakesoul_metadata_generated_path(self):
        import os
        key = '_LAKESOUL_METADATA_GENERATED'
        path = os.environ.get(key)
        if path is not None and os.path.isdir(path):
            return path
        path = os.path.join(self._get_project_root_dir(), 'cpp/build/python/lakesoul/metadata/generated')
        if os.path.isdir(path):
            return path
        message = "'generated' is not specified via "
        message += "environment variable %r " % key
        message += "nor found at default location %r " % path
        raise RuntimeError(message)

    def _copy_lakesoul_metadata_files(self, metadata_dir_path):
        import os
        import glob
        import shutil
        lakesoul_metadata_so_path = self._get_lakesoul_metadata_so_path()
        so_path = os.path.join(metadata_dir_path, 'lib', 'liblakesoul_metadata_c.so')
        print(f'copy so cwd: {os.getcwd()} from {lakesoul_metadata_so_path} to {so_path}')
        shutil.copy(lakesoul_metadata_so_path, so_path)
        lakesoul_metadata_generated_path = self._get_lakesoul_metadata_generated_path()
        glob_pattern = os.path.join(lakesoul_metadata_generated_path, '*.py')
        for src_path in glob.glob(glob_pattern):
            dst_path = os.path.join(metadata_dir_path, 'generated', os.path.basename(src_path))
            print(f'copy py cwd: {os.getcwd()} from {src_path} to {dst_path}')
            shutil.copy(src_path, dst_path)

    def _build_lakesoul(self, ext):
        import os
        import shutil
        lakesoul_dataset_so_path = self._get_lakesoul_dataset_so_path()
        ext_so_path = self.get_ext_fullpath(ext.name)
        print(f'ext copy so cwd: {os.getcwd()} from {lakesoul_dataset_so_path} to {ext_so_path}')
        shutil.copy(lakesoul_dataset_so_path, ext_so_path)
        metadata_dir_path = os.path.join(os.path.dirname(os.path.dirname(ext_so_path)), 'metadata')
        self._copy_lakesoul_metadata_files(metadata_dir_path)

setup(
    ext_modules=[LakeSoulDatasetExtension('lakesoul/arrow/_lakesoul_dataset')],
    cmdclass={ 'build_ext': lakesoul_build_ext },
)
