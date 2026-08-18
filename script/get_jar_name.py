# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import os
import sys


def load_maven_properties(path):
    properties = {}
    with open(path, encoding="iso-8859-1") as config_file:
        for line in config_file:
            line = line.strip()
            if not line or line.startswith(("#", "!")):
                continue
            key, separator, value = line.partition("=")
            if not separator:
                key, separator, value = line.partition(":")
            if not separator:
                raise ValueError(f"invalid property in {path}: {line}")
            properties[key.strip()] = value.strip()
    return properties


def main():
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {os.path.basename(sys.argv[0])} MODULE_DIRECTORY")

    path = os.path.join(sys.argv[1], "target/maven-archiver/pom.properties")
    properties = load_maven_properties(path)
    print(f"{properties['artifactId']}-{properties['version']}.jar")


if __name__ == "__main__":
    main()
