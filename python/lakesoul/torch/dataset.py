# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import torch

class Dataset(torch.utils.data.IterableDataset):
    def __init__(self,
                 lakesoul_table_name,
                 batch_size=16,
                 thread_count=1,
                 rank=None,
                 world_size=None,
                 partitions=None,
                 retain_partition_columns=False,
                 namespace='default'):
        self._lakesoul_table_name = lakesoul_table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._rank = rank
        self._world_size = world_size
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace

    def __iter__(self):
        from ..arrow import lakesoul_dataset
        dataset = lakesoul_dataset(
            self._lakesoul_table_name,
            batch_size=self._batch_size,
            thread_count=self._thread_count,
            rank=self._rank,
            world_size=self._world_size,
            partitions=self._partitions,
            retain_partition_columns=self._retain_partition_columns,
            namespace=self._namespace,
        )
        yield from dataset.to_batches()
