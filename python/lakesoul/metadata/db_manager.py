# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from .dao import *
from .utils import to_arrow_schema


class DBManager:
    def __init__(self):
        pass

    def get_table_info_by_name(self, table_name, namespace):
        return select_table_info_by_table_name(table_name, namespace)

    def get_all_partition_info(self, table_id):
        return get_partition_desc_by_table_id(table_id)

    def get_table_single_partition_data_info(self, partition_info):
        return list_data_commit_info_by_table_id_and_partition_desc_and_commit_list(partition_info.table_id,
                                                                                    partition_info.partition_desc,
                                                                                    partition_info.snapshot)

    def get_table_info_by_id(self, table_id):
        pass

    def get_data_file_info_list(self, table_info):
        pass

    def split_data_info_list_to_range_and_hash_partition(self, table_id, data_file_info_list):
        pass

    def get_data_files_by_table_name(self, table_name, partitions=None, namespace="default"):
        part_filter = []
        partitions = partitions or {}
        for part_key, part_value in partitions.items():
            part_filter.append("{}={}".format(part_key, part_value))
        table_info = self.get_table_info_by_name(table_name, namespace)
        partition_list = self.get_all_partition_info(table_info.table_id)
        data_files = []
        for partition in partition_list:
            partition_desc = partition.partition_desc
            filtered = False
            for part in part_filter:
                if part not in partition_desc:
                    filtered = True
                    break
            if filtered:
                continue
            data_commit_info_list = self.get_table_single_partition_data_info(partition)
            for data_commit_info in data_commit_info_list:
                for file_op in data_commit_info.file_ops:
                    data_files.append(file_op.path)
        return data_files

    def get_arrow_schema_by_table_name(self, table_name, namespace="default", exclude_partition=False):
        table_info = self.get_table_info_by_name(table_name, namespace)
        schema = table_info.table_schema
        exclude_partitions = None
        if exclude_partition and len(table_info.partitions) > 0:
            exclude_partitions = frozenset(table_info.partitions.split(";")[0].split(","))

        return to_arrow_schema(schema, exclude_partitions)
