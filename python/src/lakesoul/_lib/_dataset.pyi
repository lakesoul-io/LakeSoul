import pyarrow

def sync_reader(
    batch_size: int,
    thread_num: int,
    schema: pyarrow.Schema,
    file_urls: list[str],
    primary_keys: list[str],
    partition_info: list[tuple[str, str]],
    oss_conf: list[tuple[str, str]],
    partition_schema: pyarrow.Schema | None = None,
    filter: bytes | None = None,
) -> pyarrow.RecordBatchReader: ...
def one_reader(
    batch_size: int,
    thread_num: int,
    schema: pyarrow.Schema,
    file_urls: list[list[str]],
    primary_keys: list[list[str]],
    partition_info: list[tuple[str, str]],
    oss_conf: list[tuple[str, str]],
    partition_schema: pyarrow.Schema | None = None,
    filter: bytes | None = None,
) -> pyarrow.RecordBatchReader: ...
