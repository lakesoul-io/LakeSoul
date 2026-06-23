# SPDX-FileCopyrightText: 2023,2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import datasets
import pyarrow as pa

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan


def from_lakesoul(scan: LakeSoulScan) -> datasets.IterableDataset:
    def _generate_tables_from_lakesoul_table(
        *args, **kwargs
    ) -> Iterator[tuple[int, pa.Table]]:
        del args, kwargs
        for batch_idx, batch in enumerate(scan.to_batches()):
            yield batch_idx, pa.Table.from_batches([batch])

    ex_iterable = datasets.iterable_dataset.ArrowExamplesIterable(
        _generate_tables_from_lakesoul_table,
        kwargs={},
    )
    inferred_features = datasets.Features.from_arrow_schema(scan.schema)
    info = datasets.DatasetInfo(features=inferred_features)
    return datasets.IterableDataset(ex_iterable=ex_iterable, info=info)


datasets.IterableDataset.from_lakesoul = from_lakesoul  # type: ignore[attr-defined]
