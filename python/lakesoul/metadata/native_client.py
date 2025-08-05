# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import threading
import concurrent.futures
import importlib
from ctypes import CFUNCTYPE, c_void_p, c_bool, c_char_p, create_string_buffer, c_int

from .lib.const import PARAM_DELIM
from . import lib
from .generated import entity_pb2

global config
config = "host=127.0.0.1 port=5432 user=lakesoul_test password=lakesoul_test"


def reset_pg_conf(conf):
    global config
    config = " ".join(conf)


def parse_jdbc_url(url):
    from urllib.parse import urlparse

    if url.startswith("jdbc:"):
        url = url[5:]
    parsed_url = urlparse(url)
    host, port = parsed_url.netloc.split(":")
    return host, port, parsed_url.path[1:]


def get_pg_conf_from_env():
    import os

    conf = []
    home_path = os.environ.get("LAKESOUL_HOME")
    if home_path is not None:
        import configparser

        tmp_conf = configparser.ConfigParser()
        with open(home_path, "r") as stream:
            tmp_conf.read_string("[top]\n" + stream.read())
            tmp_conf = tmp_conf["top"]
            host, port, dbname = parse_jdbc_url(tmp_conf["lakesoul.pg.url"])
            conf.append("host=%s" % host)
            conf.append("port=%s" % port)
            conf.append("dbname=%s" % dbname)
            conf.append("user=%s" % tmp_conf["lakesoul.pg.username"])
            conf.append("password=%s" % tmp_conf["lakesoul.pg.password"])
        return conf
    env_vars = ["LAKESOUL_PG_URL", "LAKESOUL_PG_USERNAME", "LAKESOUL_PG_PASSWORD"]
    for evar in env_vars:
        if evar not in os.environ:
            # or if you want to raise exception
            print(
                'Warning: Environment variable "%s" was not set and will use default meta db: '
                "jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified"
                % evar
            )
            return None
    host, port, dbname = parse_jdbc_url(os.environ.get("LAKESOUL_PG_URL"))
    conf.append("host=%s" % host)
    conf.append("port=%s" % port)
    conf.append("dbname=%s" % dbname)
    conf.append("user=%s" % os.environ.get("LAKESOUL_PG_USERNAME"))
    conf.append("password=%s" % os.environ.get("LAKESOUL_PG_PASSWORD"))

    return conf


class NativeMetadataClient:
    def __init__(self):
        self._lock = threading.Lock()
        importlib.reload(lib)
        self._runtime = lib.lakesoul_metadata_c.create_tokio_runtime()
        self._free_tokio_runtime = lib.lakesoul_metadata_c.free_tokio_runtime
        self._query_result_len = 0
        self._bool = False

        def callback(bool, msg):
            # print("create connection callback: status={} msg={}".format(bool, msg.decode("utf-8")))
            if not bool:
                message = "fail to initialize lakesoul.metadata.native_client.NativeMetadataClient"
                raise RuntimeError(message)

        def target():
            global config
            conf = get_pg_conf_from_env()
            if conf is not None:
                reset_pg_conf(conf)
            return lib.lakesoul_metadata_c.create_tokio_postgres_client(
                CFUNCTYPE(c_void_p, c_bool, c_char_p)(callback),
                config.encode("utf-8"),
                self._runtime,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(target)
            self._client = future.result(5)
            self._free_tokio_postgres_client = (
                lib.lakesoul_metadata_c.free_tokio_postgres_client
            )

    def __del__(self):
        if hasattr(self, "_runtime"):
            self._free_tokio_runtime(self._runtime)
            del self._free_tokio_runtime
            del self._runtime
        if hasattr(self, "_client"):
            self._free_tokio_postgres_client(self._client)
            del self._free_tokio_postgres_client
            del self._client

    def execute_query(self, query_type, params):
        joined_params = PARAM_DELIM.join(params).encode("utf-8")

        def execute_query_callback(len, msg):
            # print("execute_query query_type={} callback: len={} msg={}".format(query_type, len, msg.decode("utf-8")))
            self._query_result_len = len

        def export_bytes_result_callback(bool, msg):
            # print("export_bytes_result callback: bool={} msg={}".format(query_type, bool, msg.decode("utf-8")))
            self._bool = bool

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                lib.lakesoul_metadata_c.execute_query,
                CFUNCTYPE(c_void_p, c_int, c_char_p)(execute_query_callback),
                self._runtime,
                self._client,
                query_type,
                joined_params,
            )
            bytes = future.result(2.0)

            buffer = create_string_buffer(self._query_result_len)
            future = executor.submit(
                lib.lakesoul_metadata_c.export_bytes_result,
                CFUNCTYPE(c_void_p, c_bool, c_char_p)(export_bytes_result_callback),
                bytes,
                self._query_result_len,
                buffer,
            )
            future.result(2.0)

            ret = None
            if len(buffer.value) > 0:
                wrapper = entity_pb2.JniWrapper()  # type: ignore
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
        file_path = os.path.join(dir_path, "lib", "liblakesoul_metadata_c.so")
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
