# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pytest

TABLE_NAME_PART = "part"
TABLE_NAME_TEST_LFS = "test_lfs"


@pytest.fixture(scope="session")
def ray_session():
    import os

    import ray

    os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)

    ray.init(
        address=None,
        runtime_env={},
        include_dashboard=False,
    )
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
