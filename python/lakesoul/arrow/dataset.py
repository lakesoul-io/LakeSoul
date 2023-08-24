# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# TODO: cf: fix debug code
def _get_test_schema_and_file_list(table_name):
    if table_name == 'test':
        import pyarrow as pa
        schema = pa.schema([
            pa.field('o_id', pa.int32()),
            pa.field('o_c_id', pa.int32())
        ])
        import os
        dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        dir_path = os.path.dirname(os.path.dirname(os.path.dirname(dir_path)))
        file_path = os.path.join(dir_path, 'part-00003-729adf2f-12c6-4b4f-9fd9-45456323ba4b_00003.c000.snappy.parquet')
        file_list = file_path,
        return schema, file_list
    else:
        from db_manager import DBManager
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
