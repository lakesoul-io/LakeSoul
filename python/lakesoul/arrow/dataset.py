# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pyarrow._dataset

class Dataset(pa._dataset.Dataset):
    def __init__(self,
                 lakesoul_table_name,
                 batch_size=16,
                 thread_count=1,
                 rank=None,
                 world_size=None,
                 partitions=None,
                 retain_partition_columns=False,
                 namespace='default'):
        from ._path_utils import _configure_pyarrow_path
        _configure_pyarrow_path()
        from ._lakesoul_dataset import LakeSoulDataset
        from ..metadata.db_manager import DBManager
        self._lakesoul_table_name = lakesoul_table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._rank = rank
        self._world_size = world_size
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace
        rank, world_size = self._check_rank_and_world_size(rank, world_size)
        db_manager = DBManager()
        partitions = partitions or {}
        data_files = db_manager.get_data_files_by_table_name(
            table_name=lakesoul_table_name,
            partitions=partitions,
            namespace=namespace,
        )
        arrow_schema = db_manager.get_arrow_schema_by_table_name(
            table_name=lakesoul_table_name,
            namespace=namespace,
            exclude_partition=not retain_partition_columns,
        )
        dataset = LakeSoulDataset(arrow_schema)
        filtered_data_files = self._filter_data_files(data_files, rank, world_size)
        for data_file in filtered_data_files:
            dataset._add_file_url(data_file)
        if retain_partition_columns:
            for key, value in partitions.items():
                dataset._add_partition_key_value(key, value)
        if not isinstance(batch_size, int) or batch_size <= 0:
            message = "batch_size must be positive int; "
            message += "%r is invalid" % (batch_size,)
            raise ValueError(message)
        dataset._set_batch_size(batch_size)
        if not isinstance(thread_count, int) or thread_count < 0:
            message = "thread_count must be non-negative int; "
            message += "%r is invalid" % (thread_count,)
            raise ValueError(message)
        if thread_count == 0:
            import multiprocessing
            dataset._set_thread_num(multiprocessing.cpu_count())
        else:
            dataset._set_thread_num(thread_count)
        self._dataset = dataset

    def __reduce__(self):
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

    def _get_fragments(self, filter):
        return self._dataset._get_fragments(filter)

    def scanner(self, *args, **kwargs):
        return self._dataset.scanner(*args, **kwargs)

    @property
    def schema(self):
        return self._dataset.schema

    def _check_rank_and_world_size(self, rank, world_size):
        if rank is None and world_size is None:
            return self._try_get_rank_and_world_size()
        elif rank is not None and world_size is not None:
            if not isinstance(rank, int) or rank < 0:
                message = "rank must be non-negative int; "
                message += "%r is invalid" % (rank,)
                raise ValueError(message)
            if not isinstance(world_size, int) or world_size <= 0:
                message = "world_size must be positive int; "
                message += "%r is invalid" % (world_size,)
                raise ValueError(message)
            if rank >= world_size:
                message = "rank %d is out of range; " % rank
                message += "world_size = %d" % world_size
                raise ValueError(message)
            return rank, world_size
        else:
            message = "rank and world_size must be "
            message += "both set or both not set"
            raise ValueError(message)

    def _try_get_rank_and_world_size(self):
        try:
            import torch.distributed as dist
        except ImportError:
            return None, None
        try:
            rank = dist.get_rank()
            world_size = dist.get_world_size()
            return rank, world_size
        except RuntimeError:
            return None, None

    def _filter_data_files(self, data_files, rank, world_size):
        import warnings
        if rank is None:
            return data_files
        if len(data_files) < world_size and rank == 0:
            message = "LakeSoul table %r " % self._lakesoul_table_name
            message += "in namespace %r " % self._namespace
            message += "contains too few data files; "
            message += "world_size = %d, " % world_size
            message += "#data_files = %d" % len(data_files)
            warnings.warn(message)
        if len(data_files) % world_size != 0 and rank == 0:
            message = "LakeSoul table %r " % self._lakesoul_table_name
            message += "in namespace %r " % self._namespace
            message += "contains %d data files, " % len(data_files)
            message += "which is not a multiple of "
            message += "world_size %d" % world_size
            warnings.warn(message)
        filtered_data_files = [
            data_file
            for i, data_file in enumerate(data_files)
            if i % world_size == rank
        ]
        return filtered_data_files

def lakesoul_dataset(table_name,
                     batch_size=16,
                     thread_count=1,
                     rank=None,
                     world_size=None,
                     partitions=None,
                     retain_partition_columns=False,
                     namespace='default'):
    dataset = Dataset(
        table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        rank=rank,
        world_size=world_size,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
    )
    return dataset
