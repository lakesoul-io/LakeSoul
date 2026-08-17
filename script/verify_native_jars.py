#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Verify that release JARs contain both supported LakeSoul native libraries."""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

NATIVE_DIRECTORY = "META-INF/native/linux-x86_64"
REQUIRED_LIBRARIES = (
    f"{NATIVE_DIRECTORY}/liblakesoul_io_c.so",
    f"{NATIVE_DIRECTORY}/liblakesoul_metadata_c.so",
)


def verify_jar(path: Path) -> list[str]:
    errors: list[str] = []
    if not path.is_file():
        return [f"{path}: JAR does not exist"]
    try:
        with zipfile.ZipFile(path) as jar:
            entries = jar.infolist()
            for required in REQUIRED_LIBRARIES:
                matches = [entry for entry in entries if entry.filename == required]
                if not matches:
                    errors.append(f"{path}: missing {required}")
                elif len(matches) != 1:
                    errors.append(f"{path}: contains {required} {len(matches)} times")
                elif matches[0].file_size == 0:
                    errors.append(f"{path}: {required} is empty")
    except (OSError, zipfile.BadZipFile) as error:
        errors.append(f"{path}: cannot read JAR: {error}")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jars", nargs="+", type=Path, help="release JARs to verify")
    args = parser.parse_args(argv)

    errors: list[str] = []
    for jar in args.jars:
        errors.extend(verify_jar(jar))
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    for jar in args.jars:
        print(f"Verified native libraries in {jar}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
