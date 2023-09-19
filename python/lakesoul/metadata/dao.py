# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from .lib.const import DaoType
from .native_client import *


def select_table_info_by_table_name(table_name, namespace="default"):
    wrapper = query(DaoType.SelectTableInfoByTableNameAndNameSpace, [table_name, namespace])
    if wrapper is None:
        message = "table %r is not found in namespace %r" % (table_name, namespace)
        raise RuntimeError(message)
    return wrapper.table_info[0]


def get_partition_desc_by_table_id(table_id):
    wrapper = query(DaoType.ListPartitionByTableId, [table_id])
    return wrapper.partition_info


def list_data_commit_info_by_table_id_and_partition_desc_and_commit_list(table_id, partition_desc, commit_id_list):
    joined_commit_id = ""
    for commit_id in commit_id_list:
        joined_commit_id += "{:016x}{:016x}".format(commit_id.high, commit_id.low)
    wrapper = query(DaoType.ListDataCommitInfoByTableIdAndPartitionDescAndCommitList,
                    [table_id, partition_desc, joined_commit_id])
    return wrapper.data_commit_info
