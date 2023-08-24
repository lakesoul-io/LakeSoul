# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from db_manager import DBManager
from lib import reload_lib
from native_client import reset_pg_conf

if __name__ == '__main__':
    reload_lib("/home/huazeng/liblakesoul_metadata_c.so")
    reset_pg_conf(
        ["host=localhost", "port=5432", " dbname=lakesoul_test", " user=lakesoul_test", "password=lakesoul_test"])

    db_manager = DBManager()
    data_files = db_manager.get_data_files_by_table_name("titanic")
    print(data_files)
    data_files = db_manager.get_data_files_by_table_name("titanic", partitions={"split": "train"})
    print(data_files)
    arrow_schema = db_manager.get_arrow_schema_by_table_name("titanic")
    print(arrow_schema)
    data_files = db_manager.get_data_files_by_table_name("imdb")
    print(data_files)
    data_files = db_manager.get_data_files_by_table_name("imdb", partitions={"split": "train"})
    print(data_files)
    arrow_schema = db_manager.get_arrow_schema_by_table_name("imdb")
    print(arrow_schema)
