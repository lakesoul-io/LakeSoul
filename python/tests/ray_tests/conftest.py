# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from typing import List

import pyarrow as pa
import pytest

TABLE_NAME_PART = "part"
TABLE_NAME_TEST_LFS = "test_lfs"


@pytest.fixture(scope="session")
def ray_session():
    import ray

    ray.init("local")
    yield
    ray.shutdown()


@pytest.fixture(scope="session")
def part_schema(ray_session) -> pa.Schema:
    from lakesoul.arrow import lakesoul_dataset

    return lakesoul_dataset(TABLE_NAME_PART).schema


@pytest.fixture(scope="session")
def part_arrow_table(ray_session) -> pa.Table:
    from lakesoul.arrow import lakesoul_dataset

    return lakesoul_dataset(TABLE_NAME_PART).to_table()


@pytest.fixture(scope="session")
def part_data_files(ray_session) -> List[str]:
    from lakesoul.metadata.meta_ops import get_data_files_and_pks_by_table_name

    data_files, _ = get_data_files_and_pks_by_table_name(
        table_name=TABLE_NAME_PART,
        partitions={},
        namespace="default",
    )
    return data_files
