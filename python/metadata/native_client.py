# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import threading
import concurrent.futures
import importlib
from ctypes import *

from lib.const import PARAM_DELIM, DAO_TYPE_QUERY_LIST_OFFSET
import lib
import generated.entity_pb2 as entity_pb2

global config
config = "host={} port={} dbname={} user={} password={}".format("localhost", "5433", "test_lakesoul_meta",
                                                                "yugabyte", "yugabyte")


def reset_pg_conf(conf):
    global config
    config = " ".join(conf)


class NativeMetadataClient:
    def __init__(self):
        self._lock = threading.Lock()
        importlib.reload(lib)
        self._buffer = create_string_buffer(4096)
        self._large_buffer = create_string_buffer(65536)
        self._runtime = lib.lakesoul_metadata_c.create_tokio_runtime()

        def callback(bool, msg):
            print("create connection callback: status={} msg={}".format(bool, msg.decode("utf-8")))
            if not bool:
                exit(1)

        def target():
            global config
            return lib.lakesoul_metadata_c.create_tokio_postgres_client(CFUNCTYPE(c_void_p, c_bool, c_char_p)(callback),
                                                                        config.encode("utf-8"),
                                                                        self._runtime)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(target)
            self._client = future.result(5)

        self._prepared = lib.lakesoul_metadata_c.create_prepared_statement()

    def __del__(self):
        lib.lakesoul_metadata_c.free_tokio_runtime(self._runtime)
        lib.lakesoul_metadata_c.free_tokio_postgres_client(self._client)
        lib.lakesoul_metadata_c.free_prepared_statement(self._prepared)

    def execute_query(self, query_type, params):
        joined_params = PARAM_DELIM.join(params).encode("utf-8")
        buffer = self._buffer
        if query_type >= DAO_TYPE_QUERY_LIST_OFFSET:
            buffer = self._large_buffer
        buffer.value = b''

        def callback(len, msg):
            print("execute_query query_type={} callback: len={} msg={}".format(query_type, len, msg.decode("utf-8")))

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lib.lakesoul_metadata_c.execute_query,
                                     CFUNCTYPE(c_void_p, c_int, c_char_p)(callback), self._runtime, self._client,
                                     self._prepared, query_type, joined_params, buffer)
            future.result(2.0)

        if len(buffer.value) == 0:
            return None
        else:
            wrapper = entity_pb2.JniWrapper()
            wrapper.ParseFromString(buffer.value)
            return wrapper

    def get_lock(self):
        return self._lock


global INSTANCE

INSTANCE = None


def get_instance():
    global INSTANCE
    if INSTANCE is None:
        INSTANCE = NativeMetadataClient()
        return INSTANCE
    else:
        return INSTANCE


def query(query_type, params):
    instance = get_instance()
    lock = instance.get_lock()
    with lock:
        return instance.execute_query(query_type[0], params)
