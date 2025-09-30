# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from typing import Sequence
from .const import DaoType
from .native_client import query

from .generated import entity_pb2


def select_table_info_by_table_name(
    table_name: str, namespace: str
) -> entity_pb2.TableInfo:
    wrapper = query(
        DaoType.SelectTableInfoByTableNameAndNameSpace, [table_name, namespace]
    )
    if wrapper:
        return wrapper.table_info[0]
    raise RuntimeError(f"table {table_name} is not found in namseapce {namespace}")


def get_partition_info_by_table_id(
    table_id: str,
) -> Sequence[entity_pb2.PartitionInfo]:
    wrapper = query(DaoType.ListPartitionByTableId, [table_id])
    if wrapper:
        return wrapper.partition_info
    return []


def get_partition_info_by_table_id_and_desc(
    table_id: str, desc: str
) -> Sequence[entity_pb2.PartitionInfo]:
    wrapper = query(DaoType.ListPartitionByTableIdAndDesc, [table_id, desc])
    if wrapper:
        return wrapper.partition_info
    return []


def list_data_commit_info(
    table_id: str, partition_desc: str, commit_id_list: Sequence[entity_pb2.Uuid]
) -> Sequence[entity_pb2.DataCommitInfo]:
    joined_commit_id = ""
    for commit_id in commit_id_list:
        joined_commit_id += "{:016x}{:016x}".format(commit_id.high, commit_id.low)
    wrapper = query(
        DaoType.ListDataCommitInfoByTableIdAndPartitionDescAndCommitList,
        [table_id, partition_desc, joined_commit_id],
    )
    if wrapper:
        return wrapper.data_commit_info
    return []
