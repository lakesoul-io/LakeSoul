# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping, Optional, Tuple

import ray
from ray.data import Dataset
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

from ..metadata.meta_ops import (
    get_arrow_schema_by_table_name,
    get_data_files_and_pks_by_table_name,
)

if TYPE_CHECKING:
    import pyarrow as pa


def _cache_dataset_schema(ds: Dataset, arrow_schema: "pa.Schema") -> None:
    plan = getattr(ds, "_plan", None)
    if plan is None:
        return

    cache_schema = getattr(plan, "cache_schema", None)
    if cache_schema is not None:
        cache_schema(arrow_schema)
        return

    cache = getattr(plan, "_cache", None)
    logical_plan = getattr(ds, "_logical_plan", None)
    dag = getattr(logical_plan, "dag", None)
    set_schema = getattr(cache, "set_schema", None)
    if set_schema is not None and dag is not None:
        set_schema(dag, arrow_schema)


def _read_lakesoul_data_file(
    table_name: str,
    batch_size: int = 16,
    thread_count: int = 1,  # 0 means use cpu count
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


def _read_empty_lakesoul_table(arrow_schema: "pa.Schema") -> Iterator["pa.Table"]:
    import pyarrow as pa

    yield pa.Table.from_batches([], schema=arrow_schema)


class LakeSoulDatasource(Datasource):
    def __init__(
        self,
        table_name: str,
        batch_size: int = 16,
        thread_count: int = 1,
        partitions: Optional[Dict[str, str]] = None,
        retain_partition_columns: bool = False,
        namespace: str = "default",
        object_store_configs: Optional[Dict[str, str]] = None,
    ) -> None:
        self._table_name = table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace
        self._object_store_configs = object_store_configs or {}

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _get_table_metadata(self) -> Tuple[List[str], "pa.Schema"]:
        data_files, _ = get_data_files_and_pks_by_table_name(
            table_name=self._table_name,
            partitions=self._partitions or {},
            namespace=self._namespace,
        )
        arrow_schema = get_arrow_schema_by_table_name(
            table_name=self._table_name,
            namespace=self._namespace,
            retain_partition_columns=self._retain_partition_columns,
        )
        return data_files, arrow_schema

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[Any] = None,
    ) -> List[ReadTask]:
        # useless parallelism parameter
        read_tasks: List[ReadTask] = []
        data_files, arrow_schema = self._get_table_metadata()
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            input_files=None,
            exec_stats=None,
        )
        if not data_files:
            empty_metadata = BlockMetadata(
                num_rows=0,
                size_bytes=0,
                input_files=[],
                exec_stats=None,
            )
            return [
                ReadTask(
                    lambda arrow_schema=arrow_schema: _read_empty_lakesoul_table(
                        arrow_schema
                    ),
                    empty_metadata,
                    schema=arrow_schema,
                    per_task_row_limit=per_task_row_limit,
                )
            ]
        for index, _data_file in enumerate(data_files):
            read_task = ReadTask(
                lambda table_name=self._table_name,
                batch_size=self._batch_size,
                thread_count=self._thread_count,
                rank=index,
                world_size=len(data_files),
                partitions=self._partitions,
                retain_partition_columns=self._retain_partition_columns,
                # entry
                namespace=self._namespace,
                object_store_configs=self._object_store_configs: _read_lakesoul_data_file(
                    table_name=table_name,
                    batch_size=batch_size,
                    thread_count=thread_count,
                    rank=rank,
                    world_size=world_size,
                    partitions=partitions,
                    retain_partition_columns=retain_partition_columns,
                    namespace=namespace,
                    object_store_configs=object_store_configs,
                ),
                metadata,
                schema=arrow_schema,
                per_task_row_limit=per_task_row_limit,
            )
            read_tasks.append(read_task)
        return read_tasks

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
    object_store_configs: Optional[Mapping[str, str]] = None,
) -> Dataset:
    arrow_schema = get_arrow_schema_by_table_name(
        table_name=table_name,
        namespace=namespace,
        retain_partition_columns=retain_partition_columns,
    )
    normalized_partitions: Optional[Dict[str, str]] = (
        dict(partitions) if partitions is not None else None
    )
    normalized_object_store_configs: Dict[str, str] = (
        dict(object_store_configs) if object_store_configs is not None else {}
    )
    ds = ray.data.read_datasource(
        LakeSoulDatasource(
            table_name=table_name,
            batch_size=batch_size,
            thread_count=thread_count,
            partitions=normalized_partitions,
            retain_partition_columns=retain_partition_columns,
            namespace=namespace,
            object_store_configs=normalized_object_store_configs,
        )
    )
    _cache_dataset_schema(ds, arrow_schema)
    return ds


ray.data.read_lakesoul = read_lakesoul
