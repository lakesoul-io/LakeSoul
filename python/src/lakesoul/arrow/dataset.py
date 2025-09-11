# SPDX-FileCopyrightText: 2023,2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pyarrow
from ..metadata.meta_ops import (
    get_scan_plan_partitions,
    get_arrow_schema_by_table_name,
    LakeSoulScanPlanPartition,
)

class Dataset(pyarrow._dataset.Dataset):  # type: ignore
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

        self._lakesoul_table_name = lakesoul_table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._rank = rank
        self._world_size = world_size
        self._partitions = partitions
        self._namespace = namespace
        rank, world_size = self._check_rank_and_world_size(rank, world_size)
        partitions = partitions or {}
        scan_partitions = get_scan_plan_partitions(
            table_name=lakesoul_table_name,
            partitions=partitions,
            namespace=namespace,
        )
        object_store_configs = object_store_configs or {}
        # arrow_schema = get_arrow_schema_by_table_name(
        #     table_name=lakesoul_table_name, namespace=namespace
        # )
        target_schema = get_arrow_schema_by_table_name(
            table_name=lakesoul_table_name,
            namespace=namespace,
            exclude_partition=not retain_partition_columns,
        )
        # dataset = LakeSoulDataset(target_schema)
        filtered_scan_partitions = self._filter_scan_partitions(
            scan_partitions, rank, world_size
        )
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
        """custom serialize
        """
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

    def get_fragments(self, filter):
        return self._dataset._get_fragments(filter)

    def scanner(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def schema(self):
        return self._dataset.schema

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


def lakesoul_dataset(
    table_name,
    batch_size=16,
    thread_count=1,
    rank=None,
    world_size=None,
    partitions=None,
    retain_partition_columns=False,
    namespace="default",
    object_store_configs={},
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


