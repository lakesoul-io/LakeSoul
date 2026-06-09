def exec_query(query_type: int, joined_string: str) -> bytes: ...
def commit_data_files(
    table_name: str,
    namespace: str,
    files: list[tuple[str, str, int, list[str]]],
) -> None: ...
def create_table(
    table_name: str,
    namespace: str,
    table_path: str,
    table_schema: str,
    properties: str = "{}",
    partitions: str = ";",
    domain: str = "public",
) -> None: ...
