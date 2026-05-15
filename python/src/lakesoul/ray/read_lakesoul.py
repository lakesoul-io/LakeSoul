# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping, Optional, Tuple

import ray
from ray.data import Dataset
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask

from ..metadata.meta_ops import (
    get_arrow_schema_by_table_name,
    get_data_files_and_pks_by_table_name,
)

if TYPE_CHECKING:
    import pyarrow as pa


def _read_lakesoul_data_file(
    table_name: str,
    batch_size: int = 16,
    thread_count: int = 1,
    rank: Optional[int] = None,
    world_size: Optional[int] = None,
    partitions: Optional[Dict[str, str]] = None,
    retain_partition_columns: bool = False,
    namespace: str = "default",
    object_store_configs: Optional[Dict[str, str]] = None,
) -> Iterator["pa.Table"]:
    import pyarrow as pa

    from ..arrow import lakesoul_dataset

    normalized_partitions: Optional[Dict[str, str]] = (
        dict(partitions) if partitions is not None else None
    )

    arrow_dataset = lakesoul_dataset(
        table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        rank=rank,
        world_size=world_size,
        partitions=normalized_partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
        object_store_configs=object_store_configs or {},
    )
    for batch in arrow_dataset.to_batches():
        yield pa.Table.from_batches([batch])


class _LakeSoulDatasourceReader(Reader):
    def __init__(
        self,
        table_name: str,
        batch_size: int = 16,
        thread_count: int = 1,
        partitions: Optional[Dict[str, str]] = None,
        retain_partition_columns: bool = False,
        namespace: str = "default",
    ) -> None:
        self._table_name = table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _get_table_metadata(self) -> Tuple[List[str], "pa.Schema"]:
        data_files, pk_cols = get_data_files_and_pks_by_table_name(
            table_name=self._table_name,
            partitions=self._partitions or {},
            namespace=self._namespace,
        )
        arrow_schema = get_arrow_schema_by_table_name(
            table_name=self._table_name,
            namespace=self._namespace,
            exclude_partition=not self._retain_partition_columns,
        )
        table_metadata = data_files, arrow_schema
        return table_metadata

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        table_metadata = self._get_table_metadata()
        data_files, arrow_schema = table_metadata
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=arrow_schema,
            input_files=None,
            exec_stats=None,
        )
        for index, _data_file in enumerate(data_files):
            read_task = ReadTask(
                lambda table_name=self._table_name,
                batch_size=self._batch_size,
                thread_count=self._thread_count,
                rank=index,
                world_size=len(data_files),
                partitions=self._partitions,
                retain_partition_columns=self._retain_partition_columns,
                namespace=self._namespace: _read_lakesoul_data_file(
                    table_name=table_name,
                    batch_size=batch_size,
                    thread_count=thread_count,
                    rank=rank,
                    world_size=world_size,
                    partitions=partitions,
                    retain_partition_columns=retain_partition_columns,
                    namespace=namespace,
                ),
                metadata,
            )
            read_tasks.append(read_task)
        return read_tasks


class LakeSoulDatasource(Datasource):
    def create_reader(
        self,
        table_name: str,
        batch_size: int = 16,
        thread_count: int = 1,
        partitions: Optional[Mapping[str, str]] = None,
        retain_partition_columns: bool = False,
        namespace: str = "default",
    ) -> Reader:
        normalized_partitions: Optional[Dict[str, str]] = (
            dict(partitions) if partitions is not None else None
        )
        return _LakeSoulDatasourceReader(
            table_name=table_name,
            batch_size=batch_size,
            thread_count=thread_count,
            partitions=normalized_partitions,
            retain_partition_columns=retain_partition_columns,
            namespace=namespace,
        )

    def do_write(self, *args: Any, **kwargs: Any) -> None:
        message = "write to LakeSoul is not implemented yet"
        raise NotImplementedError(message)


def read_lakesoul(
    table_name: str,
    batch_size: int = 16,
    thread_count: int = 1,
    partitions: Optional[Mapping[str, str]] = None,
    retain_partition_columns: bool = False,
    namespace: str = "default",
    parallelism: Optional[int] = None,  # useless
) -> Dataset:
    kwargs: Dict[str, int] = {}
    if parallelism is not None:
        kwargs["parallelism"] = parallelism
    ds = ray.data.read_datasource(
        LakeSoulDatasource(),
        table_name=table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
        **kwargs,
    )
    return ds


ray.data.read_lakesoul = read_lakesoul
