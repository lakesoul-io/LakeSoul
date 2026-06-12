import pyarrow

def exec_query(query_type: int, joined_string: str) -> bytes: ...
def commit_data_files(
    table_name: str,
    namespace: str,
    files: list[tuple[str, str, int, list[str]]],
) -> None: ...
def drop_table(
    table_name: str,
    namespace: str = "default",
) -> None: ...
def _create_table(
    table_name: str,
    namespace: str,
    table_path: str,
    table_schema: pyarrow.Schema,
    properties: str = "{}",
    partitions: str = ";",
    domain: str = "public",
) -> None: ...
