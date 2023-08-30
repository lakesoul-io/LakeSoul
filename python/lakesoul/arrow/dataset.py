# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pyarrow._dataset

class Dataset(pa._dataset.Dataset):
    def __init__(self,
                 lakesoul_table_name,
                 partitions=None,
                 namespace='default',
                 retain_partition_columns=False):
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
        )
        if not retain_partition_columns:
            arrow_schema = pa.schema([
                f for f in arrow_schema
                if f.name not in partitions
            ])
        dataset = LakeSoulDataset(arrow_schema)
        for data_file in data_files:
            dataset._add_file_url(data_file)
        if retain_partition_columns:
            for key, value in partitions.items():
                dataset._add_partition_key_value(key, value)
        self._lakesoul_table_name = lakesoul_table_name
        self._partitions = partitions
        self._namespace = namespace
        self._retain_partition_columns = retain_partition_columns
        self._dataset = dataset

    def __reduce__(self):
        return self.__class__, (
            self._lakesoul_table_name,
            self._partitions,
            self._namespace,
            self._retain_partition_columns,
        )

    def _get_fragments(self, filter):
        return self._dataset._get_fragments(filter)

    def scanner(self, *args, **kwargs):
        return self._dataset.scanner(*args, **kwargs)

def lakesoul_dataset(table_name,
                     partitions=None,
                     namespace='default',
                     retain_partition_columns=False):
    dataset = Dataset(
        table_name,
        partitions=partitions,
        namespace=namespace,
        retain_partition_columns=retain_partition_columns,
    )
    return dataset
