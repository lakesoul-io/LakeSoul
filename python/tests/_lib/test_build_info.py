# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from lakesoul import _lib


def test_python_extension_build_info_is_separate_from_core() -> None:
    assert _lib.__version__
    assert f"LakeSoul Python extension {_lib.__version__}" in _lib.__build_info__
    assert "commit " in _lib.__build_info__

    assert _lib.__core_version__
    assert f"LakeSoul {_lib.__core_version__}" in _lib.__core_build_info__
    assert "Python extension" not in _lib.__core_build_info__
