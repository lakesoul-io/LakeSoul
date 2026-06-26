# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


def test_import():
    from lakesoul._lib._metadata import _NativeMetadataClient  # noqa: F401
    from lakesoul._lib._reader import _one_reader, _sync_reader  # noqa: F401
    from lakesoul._lib._writer import _NativeFileInfo, _NativeWriter  # noqa: F401
