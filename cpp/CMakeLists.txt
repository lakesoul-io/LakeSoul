# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.13 FATAL_ERROR)
project(LakeSoulCPP VERSION 1.0.0.0 LANGUAGES CXX)

# Find Python interpreter
find_package(Python REQUIRED COMPONENTS Interpreter)
message(STATUS "Found Python ${Python_VERSION} at ${Python_EXECUTABLE}")
if(Python_VERSION VERSION_LESS 3.8)
    message(FATAL_ERROR "Python 3.8+ required")
endif()

# Create Python venv and install requirements
set(VENV_DIR ${PROJECT_BINARY_DIR}/lakesoul/python-env/.env)
set(VENV_PYTHON ${VENV_DIR}/bin/python)
set(VENV_CYTHON ${VENV_DIR}/bin/cython)
if(NOT EXISTS ${VENV_PYTHON})
    execute_process(COMMAND ${Python_EXECUTABLE} -m venv ${VENV_DIR}
                    RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to create Python venv")
    endif()
    execute_process(COMMAND ${VENV_PYTHON} -m pip install --upgrade pip
                    RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to upgrade Python venv pip")
    endif()
    execute_process(COMMAND ${VENV_PYTHON} -m pip install -r ${PROJECT_SOURCE_DIR}/build-requirements.txt
                    RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to install Python venv requirements")
    endif()
endif()

# Find pyarrow
execute_process(COMMAND ${VENV_PYTHON} -c "import pyarrow; print(pyarrow.__path__[0])"
                RESULT_VARIABLE rc
                OUTPUT_VARIABLE PYARROW_DIR)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fail to get pyarrow package path")
endif()
string(STRIP "${PYARROW_DIR}" PYARROW_DIR)
if(NOT EXISTS ${PYARROW_DIR})
    message(FATAL_ERROR "Fail to find pyarrow")
endif()
set(content)
string(APPEND content "import pyarrow; ")
string(APPEND content "xs = pyarrow.__version__.split('.'); ")
string(APPEND content "m = int(xs[0]); ")
string(APPEND content "n = int(xs[1]); ")
string(APPEND content "print('%d%02d' % (m, n))")
execute_process(COMMAND ${VENV_PYTHON} -c "${content}"
                RESULT_VARIABLE rc
                OUTPUT_VARIABLE PYARROW_ABI_TAG)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fail to get pyarrow abi tag")
endif()
string(STRIP "${PYARROW_ABI_TAG}" PYARROW_ABI_TAG)
message(STATUS "Found pyarrow ${PYARROW_ABI_TAG} at ${PYARROW_DIR}")

# Find numpy include dir
execute_process(COMMAND ${VENV_PYTHON} -c "import numpy as np; print(np.get_include())"
                RESULT_VARIABLE rc
                OUTPUT_VARIABLE NUMPY_INCLUDE_DIR)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fail to find numpy include dir")
endif()
string(STRIP "${NUMPY_INCLUDE_DIR}" NUMPY_INCLUDE_DIR)
if(NOT EXISTS ${NUMPY_INCLUDE_DIR})
    message(FATAL_ERROR "Fail to find numpy include dir")
endif()

# Find Python include dir
execute_process(COMMAND ${VENV_PYTHON} -c "import sysconfig; print(sysconfig.get_path('include'))"
                RESULT_VARIABLE rc
                OUTPUT_VARIABLE PYTHON_INCLUDE_DIR)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fail to find Python include dir")
endif()
string(STRIP "${PYTHON_INCLUDE_DIR}" PYTHON_INCLUDE_DIR)
if(NOT EXISTS ${PYTHON_INCLUDE_DIR})
    message(FATAL_ERROR "Fail to find Python include dir")
endif()

# Find Cargo nightly
find_program(Cargo_EXECUTABLE cargo)
if(NOT Cargo_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found")
endif()
message(STATUS "Found Cargo at ${Cargo_EXECUTABLE}")
execute_process(COMMAND ${Cargo_EXECUTABLE} --version
                RESULT_VARIABLE rc
                OUTPUT_VARIABLE cargo_version)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fail to get Cargo version")
endif()
string(FIND "${cargo_version}" "-nightly" index)
if(index EQUAL -1)
    message(FATAL_ERROR "Cargo nightly required")
endif()

# Build liblakesoul_io_c.so
if(NOT EXISTS ${PROJECT_SOURCE_DIR}/../rust/target/release/liblakesoul_io_c.so)
    execute_process(COMMAND ${Cargo_EXECUTABLE} build -r
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../rust/lakesoul-io-c
                    RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to build liblakesoul_io_c.so")
    endif()
endif()

# Build liblakesoul_metadata_c.so
if(NOT EXISTS ${PROJECT_SOURCE_DIR}/../rust/target/release/liblakesoul_metadata_c.so)
    execute_process(COMMAND ${Cargo_EXECUTABLE} build -r
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../rust/lakesoul-metadata-c
                    RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to build liblakesoul_metadata_c.so")
    endif()
endif()

# Create CMake imported targets
add_library(liblakesoul_io_c SHARED IMPORTED)
set_target_properties(liblakesoul_io_c PROPERTIES
    IMPORTED_LOCATION "${PROJECT_SOURCE_DIR}/../rust/target/release/liblakesoul_io_c.so"
    INTERFACE_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/../rust/lakesoul-io-c")

add_library(libarrow SHARED IMPORTED)
set_target_properties(libarrow PROPERTIES
    IMPORTED_LOCATION "${PYARROW_DIR}/libarrow.so.${PYARROW_ABI_TAG}"
    INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_DIR}/include"
    INTERFACE_COMPILE_DEFINITIONS "_GLIBCXX_USE_CXX11_ABI=0")

add_library(libarrow_dataset SHARED IMPORTED)
set_target_properties(libarrow_dataset PROPERTIES
    IMPORTED_LOCATION "${PYARROW_DIR}/libarrow_dataset.so.${PYARROW_ABI_TAG}"
    INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_DIR}/include"
    INTERFACE_COMPILE_DEFINITIONS "_GLIBCXX_USE_CXX11_ABI=0"
    INTERFACE_LINK_LIBRARIES "libarrow")

add_library(libarrow_python SHARED IMPORTED)
set_target_properties(libarrow_python PROPERTIES
    IMPORTED_LOCATION "${PYARROW_DIR}/libarrow_python.so"
    INTERFACE_INCLUDE_DIRECTORIES "${NUMPY_INCLUDE_DIR};${PYTHON_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "libarrow;libarrow_dataset")

# Generate .py from .proto
add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated/entity_pb2.py
                          ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated/entity_pb2_grpc.py
                   COMMAND ${CMAKE_COMMAND} -E make_directory
                           ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated
                   COMMAND ${VENV_PYTHON} -m grpc.tools.protoc
                           -I=${PROJECT_SOURCE_DIR}/../rust/proto/src
                           --python_out=${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated
                           --grpc_python_out=${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated
                           ${PROJECT_SOURCE_DIR}/../rust/proto/src/entity.proto
                   MAIN_DEPENDENCY ${PROJECT_SOURCE_DIR}/../rust/proto/src/entity.proto)

# Generate .cpp from .pyx
add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/python/lakesoul/arrow/_lakesoul_dataset.cpp
                   COMMAND ${VENV_CYTHON} ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset.pyx
                           --cplus -o ${PROJECT_BINARY_DIR}/python/lakesoul/arrow/_lakesoul_dataset.cpp
                   MAIN_DEPENDENCY ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset.pyx
                   DEPENDS ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset.pxd
                           ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset_cpp.pxd
                           ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated/entity_pb2.py)

# Create Python extension module
add_library(lakesoul_dataset SHARED
            src/lakesoul/lakesoul_dataset.cpp
            src/lakesoul/lakesoul_fragment.cpp
            src/lakesoul/lakesoul_data_reader.cpp
            ${PROJECT_BINARY_DIR}/python/lakesoul/arrow/_lakesoul_dataset.cpp)
target_include_directories(lakesoul_dataset PRIVATE include)
target_link_libraries(lakesoul_dataset PRIVATE liblakesoul_io_c libarrow_python)
set_target_properties(lakesoul_dataset PROPERTIES PREFIX "_")
