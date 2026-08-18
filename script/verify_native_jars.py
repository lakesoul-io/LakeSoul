# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import zipfile


EXPECTED_NATIVE_LIBRARIES = (
    "META-INF/native/linux-x86_64/liblakesoul_io_c.so",
    "META-INF/native/linux-x86_64/liblakesoul_metadata_c.so",
)


def verify_jar(path):
    with zipfile.ZipFile(path) as archive:
        entries = set(archive.namelist())
        missing = [name for name in EXPECTED_NATIVE_LIBRARIES if name not in entries]
        if missing:
            raise ValueError(f"missing native libraries: {', '.join(missing)}")

        empty = [
            name
            for name in EXPECTED_NATIVE_LIBRARIES
            if archive.getinfo(name).file_size == 0
        ]
        if empty:
            raise ValueError(f"empty native libraries: {', '.join(empty)}")


def main():
    if len(sys.argv) < 2:
        raise SystemExit(f"usage: {os.path.basename(sys.argv[0])} JAR [JAR ...]")

    failures = []
    for path in sys.argv[1:]:
        try:
            verify_jar(path)
            print(f"verified native libraries in {path}")
        except (
            FileNotFoundError,
            IsADirectoryError,
            zipfile.BadZipFile,
            ValueError,
        ) as error:
            failures.append(f"{path}: {error}")

    if failures:
        raise SystemExit("native JAR verification failed:\n  " + "\n  ".join(failures))


if __name__ == "__main__":
    main()
