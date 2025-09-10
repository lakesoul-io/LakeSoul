# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

def _check_library_path(dir_path):
    import os
    ld_library_path = os.environ.get('LD_LIBRARY_PATH')
    if ld_library_path is None:
        return False
    dir_path = os.path.realpath(dir_path)
    for item in ld_library_path.split(os.pathsep):
        item_path = os.path.realpath(item)
        if item_path == dir_path:
            return True
    return False

def _add_library_path(dir_path):
    import os
    if _check_library_path(dir_path):
        return
    string = os.path.realpath(dir_path)
    ld_library_path = os.environ.get('LD_LIBRARY_PATH')
    if ld_library_path is not None:
        string += os.pathsep + ld_library_path
    os.environ['LD_LIBRARY_PATH'] = string

def _configure_pyarrow_path():
    import pyarrow as pa
    dir_path = pa.__path__[0]
    _add_library_path(dir_path)
