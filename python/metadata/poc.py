import json

from db_manager import DBManager

if __name__ == '__main__':
    db_manager = DBManager()
    data_files = db_manager.get_data_files_by_table_name("tbl1692096521988")
    arrow_schema = db_manager.get_arrow_schema_by_table_name("tbl1692096521988")
    print(data_files)
    print(arrow_schema)
