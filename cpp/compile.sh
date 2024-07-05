#!/bin/bash

# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

if [ -f $(dirname ${BASH_SOURCE[0]})/build/build.ninja ]
then
    cmake --build $(dirname ${BASH_SOURCE[0]})/build
    exit
fi

set -e

rm -rf $(dirname ${BASH_SOURCE[0]})/build
cmake -S $(dirname ${BASH_SOURCE[0]})       \
      -B $(dirname ${BASH_SOURCE[0]})/build \
      -G Ninja -DCMAKE_BUILD_TYPE=Release   \
      -DPython_EXECUTABLE=$(which python)   \
      -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build $(dirname ${BASH_SOURCE[0]})/build
