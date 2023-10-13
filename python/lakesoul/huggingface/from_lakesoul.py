# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import datasets

def from_lakesoul(table_name,
                  batch_size=16,
                  thread_count=1,
                  rank=None,
                  world_size=None,
                  partitions=None,
                  retain_partition_columns=False,
                  namespace='default'):
    from ..arrow import lakesoul_dataset
    arrow_dataset = lakesoul_dataset(
        table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        rank=rank,
        world_size=world_size,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
    )
    def _generate_tables_from_lakesoul_table():
        import pyarrow as pa
        for batch_idx, batch in enumerate(arrow_dataset.to_batches()):
            yield batch_idx, pa.Table.from_batches([batch])
    ex_gen = _generate_tables_from_lakesoul_table
    ex_iterable = datasets.iterable_dataset.ArrowExamplesIterable(ex_gen, kwargs={})
    inferred_features = datasets.Features.from_arrow_schema(arrow_dataset.schema)
    info = datasets.DatasetInfo(features=inferred_features)
    dataset = datasets.IterableDataset(ex_iterable=ex_iterable, info=info)
    return dataset

datasets.IterableDataset.from_lakesoul = from_lakesoul
