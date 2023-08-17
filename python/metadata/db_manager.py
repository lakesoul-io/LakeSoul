from dao import *
from utils import to_arrow_schema


class DBManager():
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

    def get_data_files_by_table_name(self, table_name, namespace="default"):
        table_info = self.get_table_info_by_name(table_name, namespace)
        partition_list = self.get_all_partition_info(table_info.table_id)
        data_files = []
        for partition in partition_list:
            data_commit_info_list = self.get_table_single_partition_data_info(partition)
            for data_commit_info in data_commit_info_list:
                for file_op in data_commit_info.file_ops:
                    data_files.append(file_op.path)
        return data_files

    def get_spark_schema_by_table_name(self, table_name, namespace="default"):
        table_info = self.get_table_info_by_name(table_name, namespace)
        return table_info.table_schema

    def get_arrow_schema_by_table_name(self, table_name, namespace="default"):
        schema = self.get_spark_schema_by_table_name(table_name, namespace)
        return to_arrow_schema(schema)
