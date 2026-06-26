# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pytest

TABLE_NAME_PART = "part"
TABLE_NAME_TEST_LFS = "test_lfs"

_CATALOG = None


def lakesoul_catalog():
    global _CATALOG
    if _CATALOG is None:
        from lakesoul import LakeSoulCatalog

        _CATALOG = LakeSoulCatalog.from_env()
    return _CATALOG


def lakesoul_scan(table_name: str, **kwargs):
    return lakesoul_catalog().scan(table_name, **kwargs)


def lakesoul_arrow_dataset(table_name: str, **kwargs):
    return lakesoul_scan(table_name, **kwargs).to_arrow_dataset()


def lakesoul_ray_dataset(table_name: str, **kwargs):
    return lakesoul_scan(table_name, **kwargs).to_ray()


@pytest.fixture(scope="session")
def ray_session():
    import os

    import ray

    os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)

    ray.init(
        address=None,
        runtime_env=None,
        include_dashboard=False,
    )
    yield
    ray.shutdown()


@pytest.fixture(scope="session")
def part_schema(ray_session) -> pa.Schema:
    return lakesoul_arrow_dataset(TABLE_NAME_PART).schema


@pytest.fixture(scope="session")
def part_arrow_table(ray_session) -> pa.Table:
    return lakesoul_arrow_dataset(TABLE_NAME_PART).to_table()
