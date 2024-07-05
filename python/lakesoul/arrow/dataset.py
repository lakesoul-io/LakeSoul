# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pyarrow._dataset

from ..metadata.meta_ops import *


class Dataset(pa._dataset.Dataset):
    def __init__(self,
                 lakesoul_table_name,
                 batch_size=16,
                 thread_count=1,
                 rank=None,
                 world_size=None,
                 partitions=None,
                 retain_partition_columns=False,
                 namespace='default',
                 object_store_configs={}):
        from ._path_utils import _configure_pyarrow_path
        _configure_pyarrow_path()
        from ._lakesoul_dataset import LakeSoulDataset
        self._lakesoul_table_name = lakesoul_table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._rank = rank
        self._world_size = world_size
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace
        rank, world_size = self._check_rank_and_world_size(rank, world_size)
        partitions = partitions or {}
        scan_partitions = get_scan_plan_partitions(
            table_name=lakesoul_table_name,
            partitions=partitions,
            namespace=namespace,
        )
        arrow_schema = get_arrow_schema_by_table_name(
            table_name=lakesoul_table_name,
            namespace=namespace
        )
        target_schema = get_arrow_schema_by_table_name(
            table_name=lakesoul_table_name,
            namespace=namespace,
            exclude_partition=not retain_partition_columns
        )
        dataset = LakeSoulDataset(target_schema)
        filtered_scan_partitions = self._filter_scan_partitions(scan_partitions, rank, world_size)
        for scan_part in filtered_scan_partitions:
            dataset._add_file_urls(scan_part.files)
            dataset._add_primary_keys(scan_part.primary_keys)
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
        if retain_partition_columns:
            dataset._set_retain_partition_columns()
        dataset._set_object_store_configs(object_store_configs)
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
        except Exception as e:
            print("An exception occurred to obtain PyTorch distributed rank: ", e)
            print("If you are not using PyTorch's distributed runtime, just ignore this error")
            return None, None

    def _filter_scan_partitions(self, scan_partitions: LakeSoulScanPlanPartition, rank, world_size):
        import warnings
        if rank is None:
            return scan_partitions
        if len(scan_partitions) < world_size and rank == 0:
            message = "LakeSoul table %r " % self._lakesoul_table_name
            message += "in namespace %r " % self._namespace
            message += "contains too few partitions to scan; "
            message += "world_size = %d, " % world_size
            message += "#partition_num = %d" % len(scan_partitions)
            warnings.warn(message)
        if len(scan_partitions) % world_size != 0 and rank == 0:
            message = "LakeSoul table %r " % self._lakesoul_table_name
            message += "in namespace %r " % self._namespace
            message += "contains %d num of scan partitions, " % len(scan_partitions)
            message += "which is not a multiple of "
            message += "world_size %d" % world_size
            warnings.warn(message)
        filtered_scan_partitions = [
            scan_part
            for i, scan_part in enumerate(scan_partitions)
            if i % world_size == rank
        ]
        return filtered_scan_partitions


def lakesoul_dataset(table_name,
                     batch_size=16,
                     thread_count=1,
                     rank=None,
                     world_size=None,
                     partitions=None,
                     retain_partition_columns=False,
                     namespace='default',
                     object_store_configs={}):
    dataset = Dataset(
        table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        rank=rank,
        world_size=world_size,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
        object_store_configs=object_store_configs
    )
    return dataset
