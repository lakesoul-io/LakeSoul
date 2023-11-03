# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import threading
import concurrent.futures
import importlib
from ctypes import *

from .lib.const import PARAM_DELIM, DAO_TYPE_QUERY_LIST_OFFSET
from . import lib
from .generated import entity_pb2

global config
config = None


def reset_pg_conf(conf):
    global config
    config = " ".join(conf)


def get_pg_conf_from_env():
    import os
    conf = []
    fields = 'host', 'port', 'dbname', 'user', 'password'
    for field in fields:
        key = 'LAKESOUL_METADATA_PG_%s' % field.upper()
        value = os.environ.get(key)
        if value is not None:
            conf.append('%s=%s' % (field, value))
    if conf:
        return conf
    return None


class NativeMetadataClient:
    def __init__(self):
        self._lock = threading.Lock()
        importlib.reload(lib)
        self._runtime = lib.lakesoul_metadata_c.create_tokio_runtime()
        self._free_tokio_runtime = lib.lakesoul_metadata_c.free_tokio_runtime
        self._query_result_len = 0
        self._bool = False

        def callback(bool, msg):
            print("create connection callback: status={} msg={}".format(bool, msg.decode("utf-8")))
            if not bool:
                message = "fail to initialize lakesoul.metadata.native_client.NativeMetadataClient"
                raise RuntimeError(message)

        def target():
            global config
            if config is None:
                conf = get_pg_conf_from_env()
                if conf is None:
                    message = "set LAKESOUL_METADATA_PG_* environment variables or "
                    message += "call lakesoul.metadata.native_client.reset_pg_conf "
                    message += "to configure LakeSoul metadata"
                    raise RuntimeError(message)
                reset_pg_conf(conf)
            return lib.lakesoul_metadata_c.create_tokio_postgres_client(CFUNCTYPE(c_void_p, c_bool, c_char_p)(callback),
                                                                        config.encode("utf-8"),
                                                                        self._runtime)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(target)
            self._client = future.result(5)
            self._free_tokio_postgres_client = lib.lakesoul_metadata_c.free_tokio_postgres_client

        self._prepared = lib.lakesoul_metadata_c.create_prepared_statement()
        self._free_prepared_statement = lib.lakesoul_metadata_c.free_prepared_statement

    def __del__(self):
        if hasattr(self, '_runtime'):
            self._free_tokio_runtime(self._runtime)
            del self._free_tokio_runtime
            del self._runtime
        if hasattr(self, '_client'):
            self._free_tokio_postgres_client(self._client)
            del self._free_tokio_postgres_client
            del self._client
        if hasattr(self, '_prepared'):
            self._free_prepared_statement(self._prepared)
            del self._free_prepared_statement
            del self._prepared

    def execute_query(self, query_type, params):
        joined_params = PARAM_DELIM.join(params).encode("utf-8")

        def execute_query_callback(len, msg):
            print("execute_query query_type={} callback: len={} msg={}".format(query_type, len, msg.decode("utf-8")))
            self._query_result_len = len

        def export_bytes_result_callback(bool, msg):
            print(
                "export_bytes_result callback: bool={} msg={}".format(query_type, bool, msg.decode("utf-8")))
            self._bool = bool

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lib.lakesoul_metadata_c.execute_query,
                                     CFUNCTYPE(c_void_p, c_int, c_char_p)(execute_query_callback), self._runtime,
                                     self._client,
                                     self._prepared, query_type, joined_params)
            bytes = future.result(2.0)

            buffer = create_string_buffer(self._query_result_len)
            future = executor.submit(lib.lakesoul_metadata_c.export_bytes_result,
                                     CFUNCTYPE(c_void_p, c_bool, c_char_p)(export_bytes_result_callback), bytes,
                                     self._query_result_len, buffer)
            future.result(2.0)

            ret = None
            if len(buffer.value) > 0:
                wrapper = entity_pb2.JniWrapper()
                wrapper.ParseFromString(buffer.value)
                ret = wrapper

            lib.lakesoul_metadata_c.free_bytes_result(bytes)

            return ret

    def get_lock(self):
        return self._lock


global INSTANCE

INSTANCE = None


def get_instance():
    global INSTANCE
    if INSTANCE is None:
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        file_path = os.path.join(dir_path, 'lib', 'liblakesoul_metadata_c.so')
        lib.reload_lib(file_path)
        INSTANCE = NativeMetadataClient()
        return INSTANCE
    else:
        return INSTANCE


def query(query_type, params):
    instance = get_instance()
    lock = instance.get_lock()
    with lock:
        return instance.execute_query(query_type[0], params)
