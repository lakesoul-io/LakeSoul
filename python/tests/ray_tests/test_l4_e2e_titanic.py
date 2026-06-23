# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from .conftest import lakesoul_ray_dataset, lakesoul_scan

TITANIC_TABLE = "titanic_raw"


def _table_exists(table_name):
    try:
        lakesoul_scan(table_name).schema
        return True
    except Exception:
        return False


requires_titanic = pytest.mark.skipif(
    not _table_exists(TITANIC_TABLE),
    reason="titanic_raw table does not exist — run examples/import_titanic.py first",
)


@requires_titanic
def test_titanic_table_readable(ray_session):
    """Verify titanic_raw can be read."""
    ds = lakesoul_ray_dataset(TITANIC_TABLE)
    assert ds.count() > 0
    assert "Survived" in ds.schema().names


@requires_titanic
def test_titanic_partition_filter(ray_session):
    """Verify partition filter works on split column."""
    ds = lakesoul_ray_dataset(TITANIC_TABLE, partitions={"split": "train"})
    count = ds.count()
    assert count > 0
    # Check that all rows have split="train" (if retain_partition_columns=True)
    ds2 = lakesoul_ray_dataset(
        TITANIC_TABLE,
        partitions={"split": "train"},
        retain_partition_columns=True,
    )
    for row in ds2.iter_rows():
        assert row.get("split") in (None, "train")


@requires_titanic
def test_basic_feature_engineering(ray_session):
    """Run a subset of the feature engineering pipeline."""
    ds = lakesoul_ray_dataset(
        TITANIC_TABLE,
        partitions={"split": "train"},
        batch_size=4096,
    )

    # Test extract_title
    def extract_title(batch):
        import re
        titles = []
        for name in batch["Name"]:
            match = re.search(r",\s*(\w+)\.", str(name))
            title = match.group(1) if match else "Unknown"
            if title in ["Lady", "Countess", "Capt", "Col", "Don",
                         "Dr", "Major", "Rev", "Sir", "Jonkheer",
                         "Dona", "Ms", "Mme", "Mlle"]:
                title = "Rare"
            titles.append(title)
        batch["Title"] = titles
        return batch

    ds = ds.map_batches(extract_title)
    rows = ds.take_all()
    assert len(rows) > 0
    assert all("Title" in row for row in rows)
    titles = {row["Title"] for row in rows}
    for t in titles:
        assert t in ("Mr", "Mrs", "Miss", "Master", "Rare", "Unknown")


@requires_titanic
def test_pipeline_consistency(ray_session):
    """Running pipeline twice with same parameters should produce same output."""
    ds1 = lakesoul_ray_dataset(TITANIC_TABLE, partitions={"split": "train"})
    ds2 = lakesoul_ray_dataset(TITANIC_TABLE, partitions={"split": "train"})

    assert ds1.count() == ds2.count()
    assert ds1.schema() == ds2.schema()


@requires_titanic
def test_full_pipeline_runs(ray_session):
    """Run the complete feature pipeline end-to-end."""
    import numpy as np
    import re
    from collections import Counter

    from ray.data import DataContext

    ds = lakesoul_ray_dataset(
        TITANIC_TABLE,
        partitions={"split": "train"},
        batch_size=4096,
    )

    def extract_title(batch):
        titles = []
        for name in batch["Name"]:
            match = re.search(r",\s*(\w+)\.", str(name))
            title = match.group(1) if match else "Unknown"
            if title in ["Lady", "Countess", "Capt", "Col", "Don",
                         "Dr", "Major", "Rev", "Sir", "Jonkheer",
                         "Dona", "Ms", "Mme", "Mlle"]:
                title = "Rare"
            titles.append(title)
        batch["Title"] = titles
        return batch

    def categorize_family(batch):
        def _family(sibsp, parch):
            size = int(sibsp) + int(parch) + 1
            if size < 2:
                return "Single"
            elif size == 2:
                return "Couple"
            elif size <= 4:
                return "InterM"
            else:
                return "Large"
        batch["FamilySize"] = [
            _family(s, p) for s, p in zip(batch["SibSp"], batch["Parch"])
        ]
        return batch

    def impute_missing(batch):
        ages = np.array([a for a in batch["Age"] if a is not None], dtype=float)
        median_age = np.median(ages) if len(ages) > 0 else 28.0
        batch["Age"] = [float(a) if a is not None else median_age for a in batch["Age"]]
        embarked_vals = [e for e in batch["Embarked"] if e is not None]
        mode_emb = Counter(embarked_vals).most_common(1)[0][0] if embarked_vals else "S"
        batch["Embarked"] = [
            e if e is not None else mode_emb for e in batch["Embarked"]
        ]
        batch["Fare"] = [float(f) if f is not None else 0.0 for f in batch["Fare"]]
        return batch

    def one_hot_and_assemble(batch):
        categorical_maps = {
            "Pclass": ["1", "2", "3"],
            "Sex": ["male", "female"],
            "Embarked": ["C", "Q", "S"],
            "Title": ["Master", "Miss", "Mr", "Mrs", "Rare"],
            "FamilySize": ["Single", "Couple", "InterM", "Large"],
        }
        feature_vecs = []
        n = len(batch["Pclass"])
        for i in range(n):
            vec = []
            for col, categories in categorical_maps.items():
                val = str(batch[col][i])
                oh = [1.0 if val == cat else 0.0 for cat in categories]
                vec.extend(oh)
            vec.append(float(batch["Age"][i]))
            vec.append(float(batch["Fare"][i]))
            feature_vecs.append(vec)
        batch["features"] = [np.asarray(v, dtype=np.float32) for v in feature_vecs]
        batch["label"] = np.array([int(s) for s in batch["Survived"]], dtype=np.int64)
        return batch

    ds = ds.map_batches(extract_title)
    ds = ds.map_batches(categorize_family)
    ds = ds.map_batches(impute_missing)
    ds = ds.map_batches(one_hot_and_assemble)
    ds = ds.select_columns(["features", "label"])

    # Ray 2.10 tensor-extension casting still uses NumPy 1.x internals.
    data_context = DataContext.get_current()
    old_tensor_extension_casting = data_context.enable_tensor_extension_casting
    data_context.enable_tensor_extension_casting = False
    try:
        rows = ds.take_all()
    finally:
        data_context.enable_tensor_extension_casting = old_tensor_extension_casting

    assert len(rows) > 0
    for row in rows:
        # 17 one-hot (3+2+3+5+4) + 2 numeric (Age, Fare) = 19
        assert len(row["features"]) == 19
        assert row["label"] in (0, 1)
