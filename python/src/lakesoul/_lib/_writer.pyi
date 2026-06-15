from collections.abc import Mapping

import pyarrow

class _NativeFileInfo:
    @property
    def partition(self) -> str: ...
    @property
    def path(self) -> str: ...
    @property
    def size(self) -> int: ...
    @property
    def existing_columns(self) -> list[str]: ...
    @property
    def row_count(self) -> int: ...
    @property
    def other_info(self) -> Mapping[str, str]: ...

class _NativeWriter:
    def __init__(
        self,
        *,
        path: str,
        schema: pyarrow.Schema,
        format: str = "parquet",
        primary_keys: list[str] = ...,
        partition_by: list[str] = ...,
        hash_bucket_num: int = 1,
        batch_size: int = 8192,
        thread_num: int = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: dict[str, str] | None = None,
        options: dict[str, str] | None = None,
    ) -> None: ...
    @property
    def closed(self) -> bool: ...
    def write(self, batch: pyarrow.RecordBatch) -> int: ...
    def finish(self) -> list[_NativeFileInfo]: ...
    def abort(self) -> None: ...
