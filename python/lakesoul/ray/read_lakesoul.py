# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import ray

from typing import List

from ray.data.datasource.datasource import Reader
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.datasource import Datasource
from ray.data.block import BlockMetadata

def _read_lakesoul_data_file(table_name,
                             batch_size=16,
                             thread_count=1,
                             rank=None,
                             world_size=None,
                             partitions=None,
                             retain_partition_columns=False,
                             namespace='default'):
    import pyarrow as pa
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
    for batch in arrow_dataset.to_batches():
        yield pa.Table.from_batches([batch])

class _LakeSoulDatasourceReader(Reader):
    def __init__(self,
                 table_name,
                 batch_size=16,
                 thread_count=1,
                 partitions=None,
                 retain_partition_columns=False,
                 namespace='default'):
        self._table_name = table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace

    def estimate_inmemory_data_size(self):
        return None

    def _get_table_metadata(self):
        from ..metadata.db_manager import DBManager
        db_manager = DBManager()
        data_files = db_manager.get_data_files_by_table_name(
            table_name=self._table_name,
            partitions=self._partitions or {},
            namespace=self._namespace,
        )
        arrow_schema = db_manager.get_arrow_schema_by_table_name(
            table_name=self._table_name,
            namespace=self._namespace,
            exclude_partition=not self._retain_partition_columns,
        )
        table_metadata = data_files, arrow_schema
        return table_metadata

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        read_tasks = []
        table_metadata = self._get_table_metadata()
        data_files, arrow_schema = table_metadata
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=arrow_schema,
            input_files=None,
            exec_stats=None,
        )
        for index, _data_file in enumerate(data_files):
            read_task = ReadTask(
                lambda table_name=self._table_name,
                       batch_size=self._batch_size,
                       thread_count=self._thread_count,
                       rank=index,
                       world_size=len(data_files),
                       partitions=self._partitions,
                       retain_partition_columns=self._retain_partition_columns,
                       namespace=self._namespace:
                    _read_lakesoul_data_file(
                        table_name=table_name,
                        batch_size=batch_size,
                        thread_count=thread_count,
                        rank=rank,
                        world_size=world_size,
                        partitions=partitions,
                        retain_partition_columns=retain_partition_columns,
                        namespace=namespace),
                metadata)
            read_tasks.append(read_task)
        return read_tasks

class LakeSoulDatasource(Datasource):
    def create_reader(self,
                      table_name,
                      batch_size=16,
                      thread_count=1,
                      partitions=None,
                      retain_partition_columns=False,
                      namespace='default') -> Reader:
        return _LakeSoulDatasourceReader(
            table_name=table_name,
            batch_size=batch_size,
            thread_count=thread_count,
            partitions=partitions,
            retain_partition_columns=retain_partition_columns,
            namespace=namespace,
        )

    def do_write(self, *args, **kwargs):
        message = 'write to LakeSoul is not implemented yet'
        raise NotImplementedError(message)

def read_lakesoul(table_name,
                  batch_size=16,
                  thread_count=1,
                  partitions=None,
                  retain_partition_columns=False,
                  namespace='default'):
    ds = ray.data.read_datasource(
        LakeSoulDatasource(),
        table_name=table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
    )
    return ds

ray.data.read_lakesoul = read_lakesoul
