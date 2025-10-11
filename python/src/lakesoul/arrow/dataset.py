# SPDX-FileCopyrightText: 2023,2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
import copy
from typing import Any, Callable, Iterator, final
from typing_extensions import override


import functools
import logging

import pyarrow as pa
import pyarrow.dataset as ds
from ..metadata.meta_ops import (
    get_scan_plan_partitions,
    get_schemas_by_table_name,
    LakeSoulScanPlanPartition,
)

from lakesoul._lib._dataset import one_reader  # type: ignore

DEFAULT_BATCH_SIZE: int = 2**10


@final
class Dataset(ds.Dataset):
    def __init__(  # pyright: ignore[reportMissingSuperCall]
        self,
        lakesoul_table_name: str,
        batch_size: int = 1024,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        partitions: dict[str, str] | None = None,
        retain_partition_columns: bool = False,
        namespace: str = "default",
        object_store_configs: dict[str, str] | None = None,
    ):
        self._lakesoul_table_name = lakesoul_table_name
        self._thread_count = thread_count
        self._namespace = namespace
        self._retain_partition_columns = retain_partition_columns

        rank, world_size = self._check_rank_and_world_size(rank, world_size)
        self._rank = rank
        self._world_size = world_size

        partitions = partitions or {}
        self._partitions = partitions

        object_store_configs = object_store_configs or {}
        self._oss_conf = object_store_configs

        if batch_size <= 0:
            raise ValueError(
                f"batch_size must be non-negative int; {batch_size} is invalid"
            )
        self._batch_size = batch_size

        if thread_count < 0:
            raise ValueError(
                f"thread_count must be non-negative int; {thread_count} is invalid"
            )
        if thread_count == 0:
            import multiprocessing

            thread_count = multiprocessing.cpu_count()
        self._thread_count = thread_count
        scan_partitions = get_scan_plan_partitions(
            table_name=self._lakesoul_table_name,
            partitions=self._partitions,
            namespace=self._namespace,
        )
        target_schema, partition_schema = get_schemas_by_table_name(
            table_name=self._lakesoul_table_name,
            namespace=self._namespace,
            exclude_partition=not self._retain_partition_columns,
        )
        self._schema = target_schema
        self._partition_schema = partition_schema
        filtered_scan_partitions = self._filter_scan_partitions(
            scan_partitions, self._rank, self._world_size
        )
        file_urls = []
        pks = []
        for scan_part in filtered_scan_partitions:
            file_urls.append(scan_part.files)
            pks.append(scan_part.primary_keys)
        self._file_urls = file_urls
        self._pks = pks
        self._filter = None

    def count_rows(
        self,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> int:
        return self.scanner(
            columns=None,
            filter=filter,
            batch_size=batch_size,
            batch_readahead=batch_readahead,
            fragment_readahead=fragment_readahead,
            fragment_scan_options=fragment_scan_options,
            use_threads=use_threads,
            cache_metadata=cache_metadata,
            memory_pool=memory_pool,
        ).count_rows()

    def filter(self, expression: ds.Expression):
        self._filter = expression

    def get_fragments(self, filter: ds.Expression | None = None) -> Iterator[Fragment]:
        partitions = list(self._partitions.items())
        oss_conf = list(self._oss_conf.items())
        for urls, keys in zip(self._file_urls, self._pks):
            yield Fragment(
                self._batch_size,
                self._thread_count,
                self._schema,
                urls,
                keys,
                partitions,
                oss_conf,
                self._partition_schema,
                filter if filter is not None else self._filter,
            )

    def head(
        self,
        num_rows: int,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> pa.Table:
        return self.scanner(
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            cache_metadata,
            memory_pool,
        ).head(num_rows)

    def join(
        self,
        right_dataset: ds.Dataset,
        keys,
        right_keys,
        join_type,
        left_suffix,
        right_suffix,
        coalesce_keys,
        use_threads,
    ):
        raise NotImplementedError("this method is not supported")

    def join_asof(
        self,
        right_dataset: ds.Dataset,
        on: str,
        by,
        tolerance: int,
        right_on=None,
        right_by=None,
    ):
        raise NotImplementedError("this method is not supported")

    @property
    def partition_expression(self):
        raise NotImplementedError("this method is not supported")

    def replace_schema(self, schema: pa.Schema):
        raise NotImplementedError("this method is not supported")

    def scanner(
        self,
        columns: list[str] | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads: bool = True,
        cache_metadata=None,
        memory_pool=None,
    ):
        print(columns)
        return Scanner.from_dataset(
            self,
            columns=columns,  # pyright: ignore[reportCallIssue]
            filter=filter if filter is not None else self._filter,  # pyright: ignore[reportCallIssue]
            batch_size=batch_size,  # pyright: ignore[reportCallIssue]
            batch_readahead=batch_readahead,  # pyright: ignore[reportCallIssue]
            fragment_readahead=fragment_readahead,  # pyright: ignore[reportCallIssue]
            fragment_scan_options=fragment_scan_options,  # pyright: ignore[reportCallIssue]
            use_threads=use_threads,  # pyright: ignore[reportCallIssue]
            cache_metadata=cache_metadata,  # pyright: ignore[reportCallIssue]
            memory_pool=memory_pool,  # pyright: ignore[reportCallIssue]
        )

    @property
    def schema(self):
        return self._schema

    def sort_by(self, sorting, **kwargs):
        raise NotImplementedError("this method is not supported")

    def take(
        self,
        indices,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ):
        raise NotImplementedError("this method is not supported")

    def to_batches(
        self,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> Iterator[pa.RecordBatch]:
        return self.scanner(
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            cache_metadata,
            memory_pool,
        ).to_batches()

    def to_table(
        self,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> pa.Table:
        return self.scanner(
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            cache_metadata,
            memory_pool,
        ).to_table()

    @override
    def __reduce__(self):
        """custom serialize"""
        return self.__class__, (
            self._lakesoul_table_name,
            self._batch_size,
            self._thread_count,
            self._rank,
            self._world_size,
            self._partitions,
            self._retain_partition_columns,
            self._namespace,
        )

    def thread_count(self) -> int:
        return self._thread_count

    def file_urls(self) -> list[list[str]]:
        return self._file_urls

    def primary_keys(self) -> list[list[str]]:
        return self._pks

    def partitions(self) -> dict[str, str]:
        return self._partitions

    def oss_conf(self) -> dict[str, str]:
        return self._oss_conf

    def partition_schema(self) -> pa.Schema:
        return self._partition_schema

    def _check_rank_and_world_size(
        self, rank: int | None, world_size: int | None
    ) -> tuple[int | None, int | None]:
        if rank is None and world_size is None:
            return self._try_get_rank_and_world_size()
        elif rank is not None and world_size is not None:
            if rank < 0:
                raise ValueError(f"rank must be non-negative int; {rank} is invalid")
            if world_size <= 0:
                raise ValueError(
                    f"world_size must be positive int; {world_size} is invalid"
                )
            if rank >= world_size:
                raise ValueError(
                    f"rank {rank} is out of range; world_size = {world_size}"
                )
            return rank, world_size
        else:
            raise ValueError("rank and world_size must be both set or both unset")

    def _try_get_rank_and_world_size(self) -> tuple[int | None, int | None]:
        try:
            import torch.distributed as dist  # type: ignore
        except ImportError:
            return None, None
        try:
            rank = dist.get_rank()
            world_size = dist.get_world_size()
            return rank, world_size
        except Exception as e:
            print("An exception occurred to obtain PyTorch distributed rank: ", e)
            print(
                "If you are not using PyTorch's distributed runtime, just ignore this error"
            )
            return None, None

    def _filter_scan_partitions(
        self,
        scan_partitions: list[LakeSoulScanPlanPartition],
        rank: int | None,
        world_size: int | None,
    ) -> list[LakeSoulScanPlanPartition]:
        import warnings

        if rank is None or world_size is None:
            return scan_partitions
        if len(scan_partitions) < world_size and rank == 0:
            warnings.warn(
                f"LakeSoul table {self._lakesoul_table_name} "
                f"in namespace {self._namespace} contains too few partitions to scan; "
                f"world_size = {world_size}, "
                f"#partition_num = {len(scan_partitions)}"
            )
        if len(scan_partitions) % world_size != 0 and rank == 0:
            warnings.warn(
                f"LakeSoul table {self._lakesoul_table_name}"
                f"in namespace {self._namespace} "
                f"contains {len(scan_partitions)} num of scan partitions, "
                "which is not a multiple of "
                f"world_size {world_size}"
            )
        filtered_scan_partitions = [
            scan_part
            for i, scan_part in enumerate(scan_partitions)
            if i % world_size == rank
        ]
        return filtered_scan_partitions


def check_parameters(
    _func: Callable[..., Any] | None = None,
    *,
    check_columns: bool = True,
    check_filter: bool = True,
    check_batch_readahead: bool = True,
    check_fragment_readahead: bool = True,
    check_fragment_scan_options: bool = True,
    check_cache_metadata: bool = True,
    check_memory_pool: bool = True,
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if (
                check_columns
                and kwargs.get("columns") is not None
                and (
                    not isinstance(kwargs["columns"], list)
                    or not all(isinstance(item, str) for item in kwargs["columns"])
                )
            ):
                raise NotImplementedError("columns with expression is not supported")
            if (
                check_filter
                and kwargs["filter"] is not None
                and not isinstance(kwargs["filter"], ds.Expression)
            ):
                raise NotImplementedError(
                    f"filter type {type(kwargs['filter'])} is not supported"
                )
            if check_batch_readahead and kwargs["batch_readahead"] is not None:
                raise NotImplementedError("batch_readahead is not supported")
            if check_fragment_readahead and kwargs["fragment_readahead"] is not None:
                raise NotImplementedError("fragment_readahead is not supported")
            if (
                check_fragment_scan_options
                and kwargs["fragment_scan_options"] is not None
            ):
                raise NotImplementedError("fragment scan optoins is not supported")
            if check_cache_metadata and kwargs["cache_metadata"] is not None:
                raise NotImplementedError("cache metadata is not supported")
            if check_memory_pool and kwargs["memory_pool"] is not None:
                raise NotImplementedError("memory pool is not supported")
            return func(*args, **kwargs)

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


@final
class Fragment(ds.Fragment):
    def __init__(  # pyright: ignore[reportMissingSuperCall]
        self,
        batch_size: int,
        thread_count: int,
        schema: pa.Schema,
        file_urls: list[str],
        pks: list[str],
        partitions: list[tuple[str, str]],
        oss_conf: list[tuple[str, str]],
        partition_schema: pa.Schema | None,
        filter: ds.Expression | None,
    ):
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._schema = pa.schema(schema)  # copy
        self._file_urls = file_urls[:]
        self._pks = pks[:]
        self._partitions = partitions[:]
        self._oss_conf = oss_conf[:]
        self._partition_schema = (
            pa.schema(partition_schema) if partition_schema is not None else None
        )  # copy
        self._filter = filter

    @property
    def partition_expression(self):
        raise NotImplementedError

    @property
    def physical_schema(self) -> pa.Schema:
        return self._schema

    def count_rows(
        self,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> int:
        return self.scanner(
            None,
            None,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            cache_metadata,
            memory_pool,
        ).count_rows()

    def head(
        self,
        num_rows: int,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> pa.Table:
        return self.scanner(
            schema,
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            cache_metadata,
            memory_pool,
        ).head(num_rows)

    def scanner(
        self,
        schema: pa.Schema | None = None,
        columns: list[str] | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads: bool = True,
        cache_metadata=None,
        memory_pool=None,
    ) -> Scanner:
        return Scanner.from_fragment(
            self,
            schema=schema,  # pyright: ignore[reportCallIssue]
            columns=columns,  # pyright: ignore[reportCallIssue]
            filter=filter if filter is not None else self._filter,  # pyright: ignore[reportCallIssue]
            batch_size=batch_size,  # pyright: ignore[reportCallIssue]
            batch_readahead=batch_readahead,  # pyright: ignore[reportCallIssue]
            fragment_readahead=fragment_readahead,  # pyright: ignore[reportCallIssue]
            fragment_scan_options=fragment_scan_options,  # pyright: ignore[reportCallIssue]
            use_threads=use_threads,  # pyright: ignore[reportCallIssue]
            cache_metadata=cache_metadata,  # pyright: ignore[reportCallIssue]
            memory_pool=memory_pool,  # pyright: ignore[reportCallIssue]
        )

    def take(
        self,
        indices,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ):
        raise NotImplementedError("this method is not supported")

    def to_batches(
        self,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> Iterator[pa.RecordBatch]:
        return self.scanner(
            schema=schema,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            batch_readahead=batch_readahead,
            fragment_readahead=fragment_readahead,
            fragment_scan_options=fragment_scan_options,
            use_threads=use_threads,
            cache_metadata=cache_metadata,
            memory_pool=memory_pool,
        ).to_batches()

    def to_table(
        self,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=None,
        memory_pool=None,
    ) -> pa.Table:
        return self.scanner(
            schema=schema,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            batch_readahead=batch_readahead,
            fragment_readahead=fragment_readahead,
            fragment_scan_options=fragment_scan_options,
            use_threads=use_threads,
            cache_metadata=cache_metadata,
            memory_pool=memory_pool,
        ).to_batches()

    def batch_size(self) -> int:
        return self._batch_size

    def thread_count(self) -> int:
        return self._thread_count

    def file_urls(self) -> list[str]:
        return self._file_urls

    def primary_keys(self) -> list[str]:
        return self._pks

    def partitions(self) -> list[tuple[str, str]]:
        return self._partitions

    def oss_conf(self) -> list[tuple[str, str]]:
        return self._oss_conf

    def partition_schema(self) -> pa.Schema:
        return self._partition_schema


def schema_projection(origin: pa.Schema, projections: list[str]) -> pa.Schema:
    # O(n + m)
    origin_fields = {field.name for field in origin}
    redundant_fields = [col for col in projections if col not in origin_fields]
    if redundant_fields:
        raise ValueError(f"columns are not in origin schema : {redundant_fields}")

    fields = []

    for field in projections:
        if field in origin_fields:
            fields.append(origin.field(field))
        else:
            raise ValueError(f"column {field} is not in origin schema")
    return pa.schema(fields)


@final
class Scanner(ds.Scanner):
    def __init__(  # pyright: ignore[reportMissingSuperCall]
        self,
        batch_size: int,
        thread_count: int,
        target_schema: pa.Schema,
        file_urls: list[list[str]],
        pks: list[list[str]],
        partitions: list[tuple[str, str]],
        oss_conf: list[tuple[str, str]],
        partiion_schema: pa.Schema | None,
        filter: bytes | None,
    ):
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._target_schema = pa.schema(target_schema)
        self._partition_schema = pa.schema(partiion_schema) if partiion_schema else None
        self._file_urls = copy.deepcopy(file_urls)
        self._pks = copy.deepcopy(pks)
        self._partition = partitions
        self._oss_conf = oss_conf
        self._filter = filter

    def count_rows(self) -> int:
        return self.to_table().num_rows

    @staticmethod
    def from_batches(
        source,
        *,
        schema=None,
        colums=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ):
        raise NotImplementedError("the method is not supported")

    @staticmethod
    @check_parameters
    def from_dataset(
        dataset: Dataset,
        *,
        columns: list[str] | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads: bool = True,
        cache_metadata=None,
        memory_pool=None,
    ) -> Scanner:
        logging.debug(f"from_dataset filter: {filter}")
        if not use_threads:
            thread_count = 1
        else:
            thread_count = dataset.thread_count()

        if columns:
            target_schema = schema_projection(dataset.schema, columns)
        else:
            target_schema = dataset.schema

        if filter is not None:
            filter = bytes(filter.to_substrait(dataset.schema))  # copy

        return Scanner(
            batch_size,
            thread_count,
            target_schema,
            dataset.file_urls(),
            dataset.primary_keys(),
            list(dataset.partitions().items()),
            list(dataset.oss_conf().items()),
            dataset.partition_schema(),
            filter,
        )

    @staticmethod
    @check_parameters
    def from_fragment(
        fragment: Fragment,
        *,
        schema: pa.Schema | None = None,
        columns: list[str] | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads: bool = True,
        cache_metadata=None,
        memory_pool=None,
    ) -> Scanner:
        logging.debug(f"from_fragement filter: {filter}")
        if not use_threads:
            thread_count = 1
        else:
            thread_count = fragment.thread_count()

        if filter is not None:
            filter = bytes(filter.to_substrait(fragment.physical_schema))  # copy

        schema = schema if schema is not None else fragment.physical_schema

        if columns is not None:
            schema = schema_projection(schema, columns)

        return Scanner(
            batch_size,
            thread_count,
            schema,
            [fragment.file_urls()],
            [fragment.primary_keys()],
            fragment.partitions(),
            fragment.oss_conf(),
            fragment.partition_schema(),
            filter,
        )

    def head(self, num_rows: int) -> pa.Table:
        reader = self.to_reader()
        batches = []
        total_rows = 0

        for batch in reader:
            if total_rows >= num_rows:
                break
            remaining = num_rows - total_rows
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
            batches.append(batch)
            total_rows += batch.num_rows
        return pa.Table.from_batches(batches)

    def scan_batches(self):
        raise NotImplementedError("this method is not supported")

    def take(self, indices):
        raise NotImplementedError("this method is not supported")

    def to_batches(self) -> Iterator[pa.RecordBatch]:
        reader = self.to_reader()
        for rb in reader:
            yield rb

    def to_reader(self) -> pa.RecordBatchReader:
        return one_reader(
            self._batch_size,
            self._thread_count,
            self._target_schema,
            self._file_urls,
            self._pks,
            self._partition,
            self._oss_conf,
            self._partition_schema,
            self._filter,
        )

    def to_table(self) -> pa.Table:
        return self.to_reader().read_all()


def lakesoul_dataset(
    table_name: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    thread_count: int = 1,
    rank: int | None = None,
    world_size: int | None = None,
    partitions: dict[str, str] | None = None,
    retain_partition_columns: bool = False,
    namespace: str = "default",
    object_store_configs: dict[str, str] | None = None,
) -> Dataset:
    dataset = Dataset(
        table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        rank=rank,
        world_size=world_size,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
        object_store_configs=object_store_configs,
    )
    return dataset
