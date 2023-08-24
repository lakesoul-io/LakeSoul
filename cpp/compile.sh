#!/bin/bash

# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

if [ -f $(dirname ${BASH_SOURCE[0]})/build/build.ninja ]
then
    cmake --build $(dirname ${BASH_SOURCE[0]})/build --config Release
    exit
fi

set -e

rm -rf $(dirname ${BASH_SOURCE[0]})/build
cmake -G Ninja -S $(dirname ${BASH_SOURCE[0]}) \
      -B $(dirname ${BASH_SOURCE[0]})/build    \
      -DCMAKE_BUILD_TYPE=Release
cmake --build $(dirname ${BASH_SOURCE[0]})/build --config Release
