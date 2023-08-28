# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import torch

class Dataset(torch.utils.data.IterableDataset):
    def __init__(self, lakesoul_table_name):
        self._lakesoul_table_name = lakesoul_table_name

    def __iter__(self):
        from ..arrow import lakesoul_dataset
        dataset = lakesoul_dataset(self._lakesoul_table_name)
        yield from dataset.to_batches()
