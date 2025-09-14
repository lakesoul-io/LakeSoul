# SPDX-FileCopyrightText: 2023,2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Iterator
from typing_extensions import TYPE_CHECKING

import pyarrow
from ..metadata.meta_ops import (
    get_scan_plan_partitions,
    get_schemas_by_table_name,
    LakeSoulScanPlanPartition,
)

from lakesoul._lib._dataset import sync_reader

if TYPE_CHECKING:
    DatasetBase = pyarrow._dataset.Dataset  # type: ignore
    ScannerBase = pyarrow._dataset.Scanner  # type: ignore
    FragmentBase = pyarrow._dataset.Fragment  # type: ignore
else:
    DatasetBase = object
    ScannerBase = object
    FragmentBase = object


DEFAULT_BATCH_SIZE: int = 2**10


class Dataset(DatasetBase):
    def __init__(
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

        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError(
                f"batch_size must be non-negative int; {batch_size} is invalid"
            )
        self._batch_size = batch_size

        if not isinstance(thread_count, int) or thread_count < 0:
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
        self._target_schema = target_schema
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

    # TODO
    def count_rows(
        self,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ):
        pass

    # TODO
    def filter(self, expression):
        raise NotImplementedError

    # TODO
    def get_fragments(self, filter):
        raise NotImplementedError

    # TODO
    def head(
        self,
        num_rows: int,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ):
        raise NotImplementedError

    def join(
        self,
        right_dataset: pyarrow.dataset.Dataset, # type: ignore
        keys,
        right_keys,
        join_type,
        left_suffix,
        right_suffix,
        coalesce_keys,
        use_threads,
    ):
        raise NotImplementedError

    def join_asof(
        self,
        right_dataset: pyarrow.dataset.Dataset, # type: ignore
        on: str,
        by,
        tolerance: int,
        right_on=None,
        right_by=None,
    ):
        raise NotImplementedError

    @property
    def partition_expression(self):
        raise NotImplementedError

    def replace_schema(self,schema:pyarrow.Schema):
        raise NotImplementedError

    # TODO
    def scanner(self,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,

                ):
        raise NotImplementedError
    
    @property
    def schema(self):
        return self._schema

    def sort_by(self,sorting,**kwargs):
        raise NotImplementedError

    def take(self,indices,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
             ):
        raise NotImplementedError

    # TODO
    def to_batches(self,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
                   ) -> Iterator[pyarrow.RecordBatch]:
        readers = self._sync_readers()
        for reader in readers:
            for rb in reader:
                yield rb
    
    # TODO
    def to_table(self,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
                 ) -> pyarrow.Table:
        raise NotImplementedError


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




    def _sync_readers(self) -> list[pyarrow.RecordBatchReader]:
        readers = []
        for urls, keys in zip(self._file_urls, self._pks):
            readers.append(
                sync_reader(
                    self._batch_size,
                    self._thread_count,
                    self._target_schema,
                    urls,
                    keys,
                    list(self._partitions.items()),
                    list(self._oss_conf.items()),
                    self._partition_schema,
                )
            )
        return readers



    def _check_rank_and_world_size(
        self, rank: int | None, world_size: int | None
    ) -> tuple[int | None, int | None]:
        if rank is None and world_size is None:
            return self._try_get_rank_and_world_size()
        elif rank is not None and world_size is not None:
            if not isinstance(rank, int) or rank < 0:
                raise ValueError(f"rank must be non-negative int; {rank} is invalid")
            if not isinstance(world_size, int) or world_size <= 0:
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


class Fragment(FragmentBase):
    # TODO
    def __init__(self):
        pass

    @property
    def partition_expression(self):
        raise NotImplementedError

    @property
    def physical_schema(self):
        raise NotImplementedError

    # TODO
    def count_rows(
        self,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ): ...

    # TODO
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
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ): ...

    # TODO
    def scanner(
        self,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ): ...

    def take(self): ...

    # TODO
    def to_batches(
        self,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ): ...

    # TODO
    def to_table(
        self,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ): ...


class Scanner(ScannerBase):
    # TODO
    def __init__(self, schema, projected_schema):
        self._dataset_schema = schema
        self._projected_schema = projected_schema

    # TODO
    def count_rows(self) -> int: ...

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

    # TODO
    @staticmethod
    def from_dataset(
        dataset,
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
        pass

    # TODO
    @staticmethod
    def from_fragment(
        fragment,
        *,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=None,
        cache_metadata=None,
        memory_pool=None,
    ): ...

    # TODO
    def head(self, num_rows: int):
        pass

    def scan_batches(self):
        raise NotImplementedError("the method is not supported")

    def take(self, indices):
        raise NotImplementedError("the method is not supported")

    # TODO
    def to_batches(self): ...

    # TODO
    def to_reader(self): ...

    # TODO
    def to_table(self): ...


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
):
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
