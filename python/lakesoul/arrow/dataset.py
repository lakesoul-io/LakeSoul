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
                 partitions=None,
                 retain_partition_columns=False,
                 namespace='default'):
        from ._path_utils import _configure_pyarrow_path
        _configure_pyarrow_path()
        from ._lakesoul_dataset import LakeSoulDataset
        from ..metadata.db_manager import DBManager
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
        for data_file in data_files:
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
        self._lakesoul_table_name = lakesoul_table_name
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._partitions = partitions
        self._retain_partition_columns = retain_partition_columns
        self._namespace = namespace
        self._dataset = dataset

    def __reduce__(self):
        return self.__class__, (
            self._lakesoul_table_name,
            self._batch_size,
            self._thread_count,
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

def lakesoul_dataset(table_name,
                     batch_size=16,
                     thread_count=1,
                     partitions=None,
                     retain_partition_columns=False,
                     namespace='default'):
    dataset = Dataset(
        table_name,
        batch_size=batch_size,
        thread_count=thread_count,
        partitions=partitions,
        retain_partition_columns=retain_partition_columns,
        namespace=namespace,
    )
    return dataset
