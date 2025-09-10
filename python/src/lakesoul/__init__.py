# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from lakesoul._lib._dataset import double,ReaderFactory
def main():
    print("?")
    print(double(3))
    f = ReaderFactory()
    print(f.hello())