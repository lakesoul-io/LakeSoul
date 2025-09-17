# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


def test_import():
    from lakesoul._lib._metadata import exec_query
    from lakesoul._lib._dataset import sync_reader, one_reader
