# SPDX-FileCopyrightText: 2023,2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing_extensions import final, TYPE_CHECKING

import pyarrow
from ..metadata.meta_ops import (
    get_scan_plan_partitions,
    get_arrow_schema_by_table_name,
    LakeSoulScanPlanPartition,
)

if TYPE_CHECKING:
    DatasetBase = pyarrow._dataset.Dataset  # type: ignore
    FragmentBase = pyarrow._dataset.Fragment  # type: ignore
    ScannerBase = pyarrow._dataset.Scanner  # type: ignore
    FragmentScanOptionsBase = pyarrow._dataset.FragmentScanOptions  # type: ignore
else:
    DatasetBase = object
    FragmentBase = object
    ScannerBase = object
    FragmentScanOptionsBase = object

DEFAULT_BATCH_SIZE: int = 2**17


# class Dataset(pyarrow._dataset.Dataset):  # type: ignore
#     def __init__(
#         self,
#         lakesoul_table_name: str,
#         batch_size: int = 16,
#         thread_count: int = 1,
#         rank: int | None = None,
#         world_size: int | None = None,
#         partitions: dict[str, str] | None = None,
#         retain_partition_columns: bool = False,
#         namespace: str = "default",
#         object_store_configs: dict[str, str] | None = None,
#     ):
#         from ._path_utils import _configure_pyarrow_path

#         _configure_pyarrow_path()
#         from ._lakesoul_dataset import LakeSoulDataset

#         self._lakesoul_table_name = lakesoul_table_name
#         self._batch_size = batch_size
#         self._thread_count = thread_count
#         self._rank = rank
#         self._world_size = world_size
#         self._partitions = partitions
#         self._namespace = namespace
#         rank, world_size = self._check_rank_and_world_size(rank, world_size)
#         partitions = partitions or {}
#         scan_partitions = get_scan_plan_partitions(
#             table_name=lakesoul_table_name,
#             partitions=partitions,
#             namespace=namespace,
#         )
#         object_store_configs = object_store_configs or {}
#         # arrow_schema = get_arrow_schema_by_table_name(
#         #     table_name=lakesoul_table_name, namespace=namespace
#         # )
#         target_schema = get_arrow_schema_by_table_name(
#             table_name=lakesoul_table_name,
#             namespace=namespace,
#             exclude_partition=not retain_partition_columns,
#         )
#         dataset = LakeSoulDataset(target_schema)
#         filtered_scan_partitions = self._filter_scan_partitions(
#             scan_partitions, rank, world_size
#         )
#         for scan_part in filtered_scan_partitions:
#             dataset._add_file_urls(scan_part.files)
#             dataset._add_primary_keys(scan_part.primary_keys)
#         for key, value in partitions.items():
#             dataset._add_partition_key_value(key, value)
#         if not isinstance(batch_size, int) or batch_size <= 0:
#             message = "batch_size must be positive int; "
#             message += "%r is invalid" % (batch_size,)
#             raise ValueError(message)
#         dataset._set_batch_size(batch_size)
#         if not isinstance(thread_count, int) or thread_count < 0:
#             message = "thread_count must be non-negative int; "
#             message += "%r is invalid" % (thread_count,)
#             raise ValueError(message)
#         if thread_count == 0:
#             import multiprocessing

#             dataset._set_thread_num(multiprocessing.cpu_count())
#         else:
#             dataset._set_thread_num(thread_count)
#         if retain_partition_columns:
#             dataset._set_retain_partition_columns()
#         dataset._set_object_store_configs(object_store_configs)
#         self._dataset = dataset

#     def __reduce__(self):
#         return self.__class__, (
#             self._lakesoul_table_name,
#             self._batch_size,
#             self._thread_count,
#             self._rank,
#             self._world_size,
#             self._partitions,
#             self._retain_partition_columns,
#             self._namespace,
#         )

#     def _get_fragments(self, filter):
#         return self._dataset._get_fragments(filter)

#     def scanner(self, *args, **kwargs):
#         return self._dataset.scanner(*args, **kwargs)

#     @property
#     def schema(self):
#         return self._dataset.schema

#     def _check_rank_and_world_size(
#         self, rank: int | None, world_size: int | None
#     ) -> tuple[int | None, int | None]:
#         if rank is None and world_size is None:
#             return self._try_get_rank_and_world_size()
#         elif rank is not None and world_size is not None:
#             if not isinstance(rank, int) or rank < 0:
#                 raise ValueError(f"rank must be non-negative int; {rank} is invalid")
#             if not isinstance(world_size, int) or world_size <= 0:
#                 raise ValueError(
#                     f"world_size must be positive int; {world_size} is invalid"
#                 )
#             if rank >= world_size:
#                 raise ValueError(
#                     f"rank {rank} is out of range; world_size = {world_size}"
#                 )
#             return rank, world_size
#         else:
#             raise ValueError("rank and world_size must be both set or both unset")

#     def _try_get_rank_and_world_size(self) -> tuple[int | None, int | None]:
#         try:
#             import torch.distributed as dist  # type: ignore
#         except ImportError:
#             return None, None
#         try:
#             rank = dist.get_rank()
#             world_size = dist.get_world_size()
#             return rank, world_size
#         except Exception as e:
#             print("An exception occurred to obtain PyTorch distributed rank: ", e)
#             print(
#                 "If you are not using PyTorch's distributed runtime, just ignore this error"
#             )
#             return None, None

#     def _filter_scan_partitions(
#         self,
#         scan_partitions: list[LakeSoulScanPlanPartition],
#         rank: int | None,
#         world_size: int | None,
#     ) -> list[LakeSoulScanPlanPartition]:
#         import warnings

#         if rank is None or world_size is None:
#             return scan_partitions
#         if len(scan_partitions) < world_size and rank == 0:
#             warnings.warn(
#                 f"LakeSoul table {self._lakesoul_table_name} "
#                 f"in namespace {self._namespace} contains too few partitions to scan; "
#                 f"world_size = {world_size}, "
#                 f"#partition_num = {len(scan_partitions)}"
#             )
#         if len(scan_partitions) % world_size != 0 and rank == 0:
#             warnings.warn(
#                 f"LakeSoul table {self._lakesoul_table_name}"
#                 f"in namespace {self._namespace} "
#                 f"contains {len(scan_partitions)} num of scan partitions, "
#                 "which is not a multiple of "
#                 f"world_size {world_size}"
#             )
#         filtered_scan_partitions = [
#             scan_part
#             for i, scan_part in enumerate(scan_partitions)
#             if i % world_size == rank
#         ]
#         return filtered_scan_partitions


# def lakesoul_dataset(
#     table_name,
#     batch_size=16,
#     thread_count=1,
#     rank=None,
#     world_size=None,
#     partitions=None,
#     retain_partition_columns=False,
#     namespace="default",
#     object_store_configs={},
# ):
#     dataset = Dataset(
#         table_name,
#         batch_size=batch_size,
#         thread_count=thread_count,
#         rank=rank,
#         world_size=world_size,
#         partitions=partitions,
#         retain_partition_columns=retain_partition_columns,
#         namespace=namespace,
#         object_store_configs=object_store_configs,
#     )
#     return dataset


# TODO
class LakeSoulFragmentScanOptions(FragmentScanOptionsBase):
    pass


class LakeSoulScanner(ScannerBase):
    @staticmethod
    def from_dataset(
        dataset,
        *,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=True,
        memory_pool=None,
    ) -> LakeSoulScanner:
        raise NotImplementedError()

    @staticmethod
    def from_batches(
        source,
        *,
        schema=None,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=True,
        memory_pool=None,
    ):
        raise NotImplementedError

    @property
    def projected_schema(self): ...

    def to_table(self) -> pyarrow.Table:
        """
        Convert a Scanner into a Table.

        Use this convenience utility with care. This will serially materialize
        the Scan result in memory before creating the Table.

        Returns
        -------
        Table
        """
        raise NotImplementedError

    def to_reader(self) -> pyarrow.RecordBatchReader:
        """Consume this scanner as a RecordBatchReader.

        Returns
        -------
        RecordBatchReader
        """
        raise NotImplementedError


def lakesoul_dataset() -> LakeSoulDataset:
    """
    open a `LakeSoulTable` as `Arrow Dataset`
    """
    raise NotImplementedError


@final
class LakeSoulDataset(DatasetBase):
    def __init__(
        self,
        lakesoul_table_name: str,
        batch_size: int = 16,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        partitions: dict[str, str] | None = None,
        retain_partition_columns: bool = False,
        namespace: str = "default",
        object_store_configs: dict[str, str] | None = None,
    ):
        raise NotImplementedError

    # def count_rows(
    #     self,
    #     filter: pyarrow._dataset.Expression | None = None,  # type: ignore
    #     batch_size: int | None = None,
    #     batch_readahead: int | None = None,
    #     fragment_readahead: int | None = None,
    #     fragment_scan_options: pyarrow._dataset.FragmentScanOptions | None = None,  # type: ignore
    #     use_threads: bool | None = None,
    #     cache_metadata: bool | None = None,
    #     memory_pool: pyarrow.MemoryPool | None = None,
    #     row_range: tuple[int, int] | None = None,
    # ): ...

    # def filter(self): ...

    # def head(self): ...

    # def join(
    #     self,
    #     right_dataset,
    #     keys,
    #     right_keys,
    #     join_type,
    #     left_suffix,
    #     right_suffix,
    #     coalesce_keys,
    #     use_threads,
    # ): ...

    # def join_asof(self, right_dataset, on, by, tolerance): ...

    # def replace_schema(self, schema): ...

    def scanner(
        self,
        columns=None,
        filter=None,
        batch_size=DEFAULT_BATCH_SIZE,
        batch_readahead=None,
        fragment_readahead=None,
        fragment_scan_options=None,
        use_threads=True,
        cache_metadata=True,
        memory_pool=None,
    ):
        """
        Build a scan operation against the dataset.

        Data is not loaded immediately. Instead, this produces a Scanner,
        which exposes further operations (e.g. loading all data as a
        table, counting rows).

        See the :meth:`Scanner.from_dataset` method for further information.

        Parameters
        ----------
        columns : list of str, default None
            The columns to project. This can be a list of column names to
            include (order and duplicates will be preserved), or a dictionary
            with {new_column_name: expression} values for more advanced
            projections.

            The list of columns or expressions may use the special fields
            `__batch_index` (the index of the batch within the fragment),
            `__fragment_index` (the index of the fragment within the dataset),
            `__last_in_fragment` (whether the batch is last in fragment), and
            `__filename` (the name of the source file or a description of the
            source fragment).

            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        batch_size : int, default 131_072
            The maximum row count for scanned record batches. If scanned
            record batches are overflowing memory then this method can be
            called to reduce their size.
        batch_readahead : int, default 16
            The number of batches to read ahead in a file. This might not work
            for all file formats. Increasing this number will increase
            RAM usage but could also improve IO utilization.
        fragment_readahead : int, default 4
            The number of files to read ahead. Increasing this number will increase
            RAM usage but could also improve IO utilization.
        fragment_scan_options : FragmentScanOptions, default None
            Options specific to a particular scan and fragment type, which
            can change between different scans of the same dataset.
        use_threads : bool, default True
            If enabled, then maximum parallelism will be used determined by
            the number of available CPU cores.
        cache_metadata : bool, default True
            If enabled, metadata may be cached when scanning to speed up
            repeated scans.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        scanner : Scanner

        Examples
        --------
        >>> import pyarrow as pa
        >>> table = pa.table({'year': [2020, 2022, 2021, 2022, 2019, 2021],
        ...                   'n_legs': [2, 2, 4, 4, 5, 100],
        ...                   'animal': ["Flamingo", "Parrot", "Dog", "Horse",
        ...                              "Brittle stars", "Centipede"]})
        >>>
        >>> import pyarrow.parquet as pq
        >>> pq.write_table(table, "dataset_scanner.parquet")

        >>> import pyarrow.dataset as ds
        >>> dataset = ds.dataset("dataset_scanner.parquet")

        Selecting a subset of the columns:

        >>> dataset.scanner(columns=["year", "n_legs"]).to_table()
        pyarrow.Table
        year: int64
        n_legs: int64
        ----
        year: [[2020,2022,2021,2022,2019,2021]]
        n_legs: [[2,2,4,4,5,100]]

        Projecting selected columns using an expression:

        >>> dataset.scanner(columns={
        ...     "n_legs_uint": ds.field("n_legs").cast("uint8"),
        ... }).to_table()
        pyarrow.Table
        n_legs_uint: uint8
        ----
        n_legs_uint: [[2,2,4,4,5,100]]

        Filtering rows while scanning:

        >>> dataset.scanner(filter=ds.field("year") > 2020).to_table()
        pyarrow.Table
        year: int64
        n_legs: int64
        animal: string
        ----
        year: [[2022,2021,2022,2021]]
        n_legs: [[2,4,4,100]]
        animal: [["Parrot","Dog","Horse","Centipede"]]
        """
        return LakeSoulScanner.from_dataset(
            self,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            batch_readahead=batch_readahead,
            fragment_readahead=fragment_readahead,
            fragment_scan_options=fragment_scan_options,
            use_threads=use_threads,
            cache_metadata=cache_metadata,
            memory_pool=memory_pool,
        )

    # def sort_by(self, sorting, **kwargs): ...

    # def take(self, indices): ...

    # def to_batches(self): ...

    def to_table(
        self,
        columns=None,
        filter=None,
        batch_size=2**17,
        batch_readahead=16,
        fragment_readahead=4,
        fragment_scan_options=None,
        use_threads: bool = True,
        cache_metadata: bool = True,
        emory_pool=None,
    ):
        """
        Read the dataset to an Arrow table.

        Note that this method reads all the selected data from the dataset
        into memory.

        Parameters
        ----------
        columns : list of str, default None
            The columns to project. This can be a list of column names to
            include (order and duplicates will be preserved), or a dictionary
            with {new_column_name: expression} values for more advanced
            projections.

            The list of columns or expressions may use the special fields
            `__batch_index` (the index of the batch within the fragment),
            `__fragment_index` (the index of the fragment within the dataset),
            `__last_in_fragment` (whether the batch is last in fragment), and
            `__filename` (the name of the source file or a description of the
            source fragment).

            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the data
            source, e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        batch_size : int, default 131_072
            The maximum row count for scanned record batches. If scanned
            record batches are overflowing memory then this method can be
            called to reduce their size.
        batch_readahead : int, default 16
            The number of batches to read ahead in a file. This might not work
            for all file formats. Increasing this number will increase
            RAM usage but could also improve IO utilization.
        fragment_readahead : int, default 4
            The number of files to read ahead. Increasing this number will increase
            RAM usage but could also improve IO utilization.
        fragment_scan_options : FragmentScanOptions, default None
            Options specific to a particular scan and fragment type, which
            can change between different scans of the same dataset.
        use_threads : bool, default True
            If enabled, then maximum parallelism will be used determined by
            the number of available CPU cores.
        cache_metadata : bool, default True
            If enabled, metadata may be cached when scanning to speed up
            repeated scans.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        table : Table
        """
        # return self.scanner(
        #     columns=columns,
        #     filter=filter,
        #     batch_size=batch_size,
        #     batch_readahead=batch_readahead,
        #     fragment_readahead=fragment_readahead,
        #     fragment_scan_options=fragment_scan_options,
        #     use_threads=use_threads,
        #     cache_metadata=cache_metadata,
        #     memory_pool=None,
        # ).to_table()
        raise NotImplementedError


# @final
# class LakeSoulFragment(pyarrow._dataset.Fragment):
#     def __init__(self): ...

#     def count_rows(self): ...

#     def head(self): ...

#     def scanner(self): ...

#     def take(self): ...

#     def to_batches(self): ...

#     def to_table(self): ...


# @final
# class LakeSoulScanner(pyarrow._dataset.Scanner):
#     def __init__(self): ...
#     def count_rows(self): ...

#     @staticmethod
#     def from_batches(): ...

#     @staticmethod
#     def from_dataset(): ...

#     @staticmethod
#     def from_fragment(): ...

#     def head(self): ...

#     def scan_batches(self): ...

#     def take(self): ...

#     def to_batches(self): ...

#     def to_reader(self): ...

#     def to_table(self): ...
