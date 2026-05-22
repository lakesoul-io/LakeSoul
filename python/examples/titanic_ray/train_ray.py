# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""
Kaggle Titanic ML Pipeline — Ray Data + Ray Train (No Spark).

This script demonstrates an end-to-end ML pipeline:
  1. Read raw data from LakeSoul table via `read_lakesoul`
  2. Feature engineering via Ray Data `map_batches` (pure Python)
  3. DNN training via Ray Train `TorchTrainer`

Usage:
    python examples/titanic_ray/train_ray.py

Prerequisites:
    - titanic_raw LakeSoul table created by examples/import_titanic.py
    - pip install lakesoul[ray,torch]
"""

import ray
from ray.data import Dataset
from ray.train.torch import TorchTrainer

from lakesoul.ray import read_lakesoul


# ── Step 1: Load data from LakeSoul ──────────────────────────

def load_data() -> Dataset:
    ds = read_lakesoul(
        "titanic_raw",
        batch_size=4096,
        partitions={"split": "train"},
    )
    return ds


# ── Step 2: Feature engineering (Ray Data) ───────────────────

def extract_title(batch: dict) -> dict:
    """Extract Title from Name column (Mr/Mrs/Miss/Master/Rare)."""
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


def categorize_family(batch: dict) -> dict:
    """Generate FamilySize category from SibSp + Parch."""
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
        _family(s, p)
        for s, p in zip(batch["SibSp"], batch["Parch"])
    ]
    return batch


def impute_missing(batch: dict) -> dict:
    """Fill Age median, Embarked mode, Fare → float."""
    import numpy as np
    from collections import Counter

    ages = np.array([a for a in batch["Age"] if a is not None], dtype=float)
    median_age = np.median(ages) if len(ages) > 0 else 28.0
    batch["Age"] = [float(a) if a is not None else median_age for a in batch["Age"]]

    embarked_vals = [e for e in batch["Embarked"] if e is not None]
    mode_emb = Counter(embarked_vals).most_common(1)[0][0] if embarked_vals else "S"
    batch["Embarked"] = [e if e is not None else mode_emb for e in batch["Embarked"]]

    batch["Fare"] = [float(f) if f is not None else 0.0 for f in batch["Fare"]]
    return batch


def one_hot_and_assemble(batch: dict) -> dict:
    """One-hot encode categorical columns + assemble feature vectors."""
    import numpy as np

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

    batch["features"] = feature_vecs
    batch["label"] = np.array([int(s) for s in batch["Survived"]], dtype=np.int64)
    return batch


# ── Step 3: Build pipeline ───────────────────────────────────

def build_pipeline() -> Dataset:
    ds = load_data()
    ds = ds.map_batches(extract_title)
    ds = ds.map_batches(categorize_family)
    ds = ds.map_batches(impute_missing)
    ds = ds.map_batches(one_hot_and_assemble)
    ds = ds.select_columns(["features", "label"])
    return ds


# ── Step 4: Model and training function ──────────────────────

# 17 one-hot features + 2 numeric = 19
FEATURE_DIM = 19


def train_func(config: dict):
    """Ray Train worker function."""
    import torch
    import torch.nn as nn
    from ray.train.torch import get_device

    feature_dim = int(config["feature_dim"])

    class TitanicDNN(nn.Module):
        def __init__(self, input_dim: int):
            super().__init__()
            self.norm = nn.LayerNorm(input_dim)
            self.fc1 = nn.Linear(input_dim, 256)
            self.fc2 = nn.Linear(256, 2)

        def forward(self, x):
            x = self.norm(x)
            x = torch.relu(self.fc1(x))
            x = self.fc2(x)
            return x

    model = TitanicDNN(feature_dim)
    device = get_device()
    model.to(device)

    train_loader = ray.train.get_dataset_shard("train")
    val_loader = ray.train.get_dataset_shard("val")

    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-2, weight_decay=5e-3)
    criterion = nn.CrossEntropyLoss()

    for epoch in range(config["num_epochs"]):
        model.train()
        for batch in train_loader.iter_torch_batches(batch_size=50):
            x = batch["features"].to(device).float()
            y = batch["label"].to(device)
            optimizer.zero_grad()
            loss = criterion(model(x), y)
            loss.backward()
            optimizer.step()

        model.eval()
        correct, total = 0, 0
        with torch.no_grad():
            for batch in val_loader.iter_torch_batches(batch_size=50):
                x = batch["features"].to(device).float()
                y = batch["label"].to(device)
                pred = model(x).argmax(dim=1)
                correct += (pred == y).sum().item()
                total += y.size(0)
        ray.train.report({"accuracy": correct / total, "epoch": epoch})


# ── Step 5: Main ─────────────────────────────────────────────

def main():
    ray.init("local")

    ds = build_pipeline()
    train_ds, val_ds = ds.train_test_split(
        test_size=0.1,
        shuffle=True,
        seed=42,
    )

    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 50, "feature_dim": FEATURE_DIM},
        datasets={"train": train_ds, "val": val_ds},
        scaling_config=ray.train.ScalingConfig(
            num_workers=1,
            use_gpu=False,
        ),
    )
    result = trainer.fit()
    print(f"Best accuracy: {result.metrics['accuracy']:.4f}")

    ray.shutdown()


if __name__ == "__main__":
    main()
