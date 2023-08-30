# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import torch

class Dataset(torch.utils.data.IterableDataset):
    def __init__(self,
                 lakesoul_table_name,
                 partitions=None,
                 namespace='default',
                 retain_partition_columns=False):
        self._lakesoul_table_name = lakesoul_table_name
        self._partitions = partitions
        self._namespace = namespace
        self._retain_partition_columns = retain_partition_columns

    def __iter__(self):
        from ..arrow import lakesoul_dataset
        dataset = lakesoul_dataset(
            self._lakesoul_table_name,
            partitions=self._partitions,
            namespace=self._namespace,
            retain_partition_columns=self._retain_partition_columns,
        )
        yield from dataset.to_batches()
