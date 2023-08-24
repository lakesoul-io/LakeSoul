# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

def _get_test_schema_and_file_list(table_name):
    from ..metadata.db_manager import DBManager
    db_manager = DBManager()
    data_files = db_manager.get_data_files_by_table_name(table_name)
    arrow_schema = db_manager.get_arrow_schema_by_table_name(table_name)
    return arrow_schema, data_files

def lakesoul_dataset(table_name):
    from ._lakesoul_dataset import LakeSoulDataset
    schema, file_list = _get_test_schema_and_file_list(table_name)
    dataset = LakeSoulDataset(schema)
    for data_file in file_list:
        dataset._add_file_url(data_file)
    return dataset
