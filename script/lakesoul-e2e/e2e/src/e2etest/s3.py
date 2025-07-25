# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import List

from e2etest.vars import (
    S3_OPERATOR,
    E2E_CLASSPATH,
    LAKESOUL_FLINK_E2E_PATH,
    LAKESOUL_FLINK_PATH,
    LAKESOUL_SPARK_E2E_PATH,
    LAKESOUL_SPARK_PATH,
)


def s3_upload_jars():
    """upload installed lakesoul jars to s3"""
    jars: List[Path] = [
        LAKESOUL_FLINK_PATH,
        LAKESOUL_SPARK_PATH,
        LAKESOUL_FLINK_E2E_PATH,
        LAKESOUL_SPARK_E2E_PATH,
    ]

    for jar in jars:
        with open(jar, "rb") as data:
            S3_OPERATOR.write(f"{E2E_CLASSPATH}/{jar.name}", data.read())


def s3_delete_jars():
    """delete jars at s3"""
    jars: List[Path] = [LAKESOUL_FLINK_PATH, LAKESOUL_SPARK_PATH]

    for jar in jars:
        S3_OPERATOR.delete(f"{E2E_CLASSPATH}/{jar.name}")


def s3_list_prefix_dir(prefix: str) -> List[str]:
    """list objects whicih has prefix

    Args:
        prefix (str): prefix of ths object
    """
    if not prefix.endswith("/"):
        prefix += "/"

    ret = []
    for e in S3_OPERATOR.list(prefix):
        ret.append(e.path)

    return ret


def s3_delete_dir(dir: str):
    S3_OPERATOR.remove_all(dir)


def s3_read(name) -> str:
    return S3_OPERATOR.read(name).decode("utf-8")


def s3_create_dir(dir_name: str):
    """
    在 S3 桶中创建一个“目录”

    S3 中没有真正的目录，这里是上传一个空对象，key 以 '/' 结尾。
    """
    # 初始化 S3 客户端
    # 确保目录名以 '/' 结尾
    if not dir_name.endswith("/"):
        dir_name += "/"

    # 上传一个空字节对象，key 为目录名
    S3_OPERATOR.create_dir(dir_name)
