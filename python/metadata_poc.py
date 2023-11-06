# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from lakesoul.metadata.db_manager import DBManager
from lakesoul.metadata.lib import reload_lib
from lakesoul.metadata.native_client import reset_pg_conf

if __name__ == '__main__':
    reset_pg_conf(
        ["host=localhost", "port=5433", " dbname=test_lakesoul_meta", " user=yugabyte", "password=yugabyte"])

    db_manager = DBManager()
    table_name = "test_datatypes"
    data_files = db_manager.get_data_files_by_table_name(table_name)
    print(data_files)
    data_files = db_manager.get_data_files_by_table_name(table_name)
    print(data_files)
    arrow_schema = db_manager.get_arrow_schema_by_table_name(table_name)
    print(arrow_schema)
    # data_files = db_manager.get_data_files_by_table_name("imdb")
    # print(data_files)
    # data_files = db_manager.get_data_files_by_table_name("imdb", partitions={"split": "train"})
    # print(data_files)
    # arrow_schema = db_manager.get_arrow_schema_by_table_name("imdb")
    # print(arrow_schema)
