# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from ctypes import *


class NonNull(Structure):
    pass


I32Callback = CFUNCTYPE(c_int, c_char_p)


def reload_lib(path):
    global \
        lib, \
        execute_query, \
        create_tokio_runtime, \
        free_tokio_runtime, \
        create_tokio_postgres_client, \
        free_tokio_postgres_client, \
        export_bytes_result, \
        free_bytes_result
    lib = CDLL(path)
    # pub extern "C" fn execute_query(
    #     callback: extern "C" fn(i32, *const c_char),
    #     runtime: NonNull<Result<TokioRuntime>>,
    #     client: NonNull<Result<TokioPostgresClient>>,
    #     query_type: i32,
    #     joined_string: *const c_char,
    # ) -> NonNull<Result<BytesResult>>
    execute_query = lib.execute_query
    execute_query.restype = POINTER(NonNull)
    execute_query.argtypes = [
        CFUNCTYPE(c_void_p, c_int, c_char_p),
        POINTER(NonNull),
        POINTER(NonNull),
        c_int,
        c_char_p,
    ]

    # pub extern "C" fn export_bytes_result(
    #     callback: extern "C" fn(bool, *const c_char),
    #     bytes: NonNull<Result<BytesResult>>,
    #     len: i32,
    #     addr: c_ptrdiff_t,
    # )
    export_bytes_result = lib.export_bytes_result
    export_bytes_result.restype = c_void_p
    export_bytes_result.argtypes = [
        CFUNCTYPE(c_void_p, c_bool, c_char_p),
        POINTER(NonNull),
        c_int,
        c_char_p,
    ]

    # pub extern "C" fn free_bytes_result(bytes: NonNull<Result<BytesResult>>)
    free_bytes_result = lib.free_bytes_result
    free_bytes_result.restype = c_void_p
    free_bytes_result.argtypes = [POINTER(NonNull)]

    # pub extern "C" fn create_tokio_runtime() -> NonNull<Result<TokioRuntime>>
    create_tokio_runtime = lib.create_tokio_runtime
    create_tokio_runtime.restype = POINTER(NonNull)
    create_tokio_runtime.argtypes = []

    # pub extern "C" fn free_tokio_runtime(runtime: NonNull<Result<TokioRuntime>>)
    free_tokio_runtime = lib.free_tokio_runtime
    free_tokio_runtime.restype = c_void_p
    free_tokio_runtime.argtypes = [POINTER(NonNull)]

    # pub extern "C" fn create_tokio_postgres_client(
    #     callback: extern "C" fn(bool, *const c_char),
    #     config: *const c_char,
    #     runtime: NonNull<Result<TokioRuntime>>,
    # ) -> NonNull<Result<TokioPostgresClient>>
    create_tokio_postgres_client = lib.create_tokio_postgres_client
    create_tokio_postgres_client.restype = POINTER(NonNull)
    create_tokio_postgres_client.argtypes = [
        CFUNCTYPE(c_void_p, c_bool, c_char_p),
        c_char_p,
        POINTER(NonNull),
    ]

    # pub extern "C" fn free_tokio_postgres_client(client: NonNull<Result<TokioPostgresClient>>)
    free_tokio_postgres_client = lib.free_tokio_postgres_client
    free_tokio_postgres_client.restype = c_void_p
    free_tokio_postgres_client.argtypes = [POINTER(NonNull)]
