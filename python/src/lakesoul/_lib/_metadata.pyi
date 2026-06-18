import pyarrow

class _NativeMetadataClient:
    def __init__(
        self,
        config: str,
        secondary_config: str | None = None,
        max_retry: int = 3,
    ) -> None: ...
    @staticmethod
    def from_env() -> "_NativeMetadataClient": ...
    def exec_query(self, query_type: int, joined_string: str) -> bytes: ...
    def commit_data_files(
        self,
        table_name: str,
        namespace: str,
        files: list[tuple[str, str, int, list[str]]],
    ) -> None: ...
    def create_table(
        self,
        table_name: str,
        namespace: str,
        table_path: str,
        table_schema: pyarrow.Schema,
        properties: str = "{}",
        partitions: str = ";",
        domain: str = "public",
    ) -> None: ...
    def drop_table(
        self,
        table_name: str,
        namespace: str = "default",
    ) -> None: ...

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
