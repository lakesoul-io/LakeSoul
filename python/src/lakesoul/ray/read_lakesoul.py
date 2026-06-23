# SPDX-FileCopyrightText: 2023,2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.dataset as ds
import ray
from ray.data import Dataset
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

from lakesoul.arrow import LakeSoulScanConfig, lakesoul_dataset
from lakesoul.arrow.dataset import schema_projection

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan


def _cache_dataset_schema(dataset: Dataset, arrow_schema: pa.Schema) -> None:
    plan = getattr(dataset, "_plan", None)
    if plan is None:
        return

    cache_schema = getattr(plan, "cache_schema", None)
    if cache_schema is not None:
        cache_schema(arrow_schema)
        return

    cache = getattr(plan, "_cache", None)
    logical_plan = getattr(dataset, "_logical_plan", None)
    dag = getattr(logical_plan, "dag", None)
    set_schema = getattr(cache, "set_schema", None)
    if set_schema is not None and dag is not None:
        set_schema(dag, arrow_schema)


def _read_lakesoul_scan(
    scan_config: LakeSoulScanConfig,
    columns: tuple[str, ...] | None,
    filter: ds.Expression | None,
) -> Iterator[pa.Table]:
    arrow_dataset = lakesoul_dataset(scan_config)
    for batch in arrow_dataset.to_batches(
        columns=list(columns) if columns is not None else None,
        filter=filter,
    ):
        yield pa.Table.from_batches([batch])


def _read_empty_lakesoul_table(arrow_schema: pa.Schema) -> Iterator[pa.Table]:
    yield pa.Table.from_batches([], schema=arrow_schema)


class LakeSoulDatasource(Datasource):
    def __init__(
        self,
        scan_config: LakeSoulScanConfig,
        *,
        columns: tuple[str, ...] | None = None,
        filter: ds.Expression | None = None,
    ) -> None:
        self._scan_config = scan_config
        self._columns = columns
        self._filter = filter
        self._schema = (
            schema_projection(scan_config.schema, list(columns))
            if columns is not None
            else scan_config.schema
        )

    def estimate_inmemory_data_size(self) -> int | None:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: Any | None = None,
    ) -> list[ReadTask]:
        del parallelism, data_context

        scan_partitions = [
            scan_partition
            for scan_partition in self._scan_config.scan_partitions
            if scan_partition.files
        ]
        if not scan_partitions:
            metadata = BlockMetadata(
                num_rows=0,
                size_bytes=0,
                input_files=None,
                exec_stats=None,
            )
            return [
                ReadTask(
                    lambda schema=self._schema: _read_empty_lakesoul_table(schema),
                    metadata,
                    schema=self._schema,
                    per_task_row_limit=per_task_row_limit,
                )
            ]

        read_tasks: list[ReadTask] = []
        for scan_partition in scan_partitions:
            task_config = replace(
                self._scan_config,
                scan_partitions=(scan_partition,),
                rank=None,
                world_size=None,
            )
            metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=list(scan_partition.files),
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda config=task_config,
                    columns=self._columns,
                    filter=self._filter: _read_lakesoul_scan(
                        config,
                        columns,
                        filter,
                    ),
                    metadata,
                    schema=self._schema,
                    per_task_row_limit=per_task_row_limit,
                )
            )
        return read_tasks


def read_lakesoul(scan: LakeSoulScan) -> Dataset:
    scan_config = scan.to_scan_config()
    datasource = LakeSoulDatasource(
        scan_config,
        columns=scan.columns,
        filter=scan.expression,
    )
    dataset = ray.data.read_datasource(datasource)
    _cache_dataset_schema(dataset, datasource._schema)
    return dataset


ray.data.read_lakesoul = read_lakesoul  # type: ignore[attr-defined]
