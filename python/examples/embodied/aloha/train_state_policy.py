"""CPU smoke-training example: historical state -> recorded action window.

Run beside prepare_dataset.py with the same PG/S3 environment variables:
  python train_state_policy.py --samples-table aloha_velcro_samples_v1

Requires numpy and torch plus the existing LakeSoul/Daft dependencies.
Reads only state/action, respects existing train/val splits, and fits scaling
on selected training windows only (overlapping frames count repeatedly).
This verifies offline training, not robot task success or deployment safety.
"""

import argparse
import copy
import json
from pathlib import Path
import random

import numpy as np

from common import DEFAULT_STATE_POLICY_OUTPUT


def choose_indices(rows, split, limit, seed):
    """Reproducibly sample within an existing split; never reassign episodes."""
    selected = sorted(
        (r for r in rows if r["split"] == split), key=lambda r: r["sample_id"]
    )
    random.Random(seed).shuffle(selected)
    selected = selected[:limit]
    if not selected:
        raise ValueError(
            f"No {split} samples; prepare an episode-level train/val split first"
        )
    return selected


def load_vectors(catalog, indices):
    from prepare_dataset import read_training_sample

    states, actions = [], []
    for i, index in enumerate(indices):
        # Override the projection on a copy: do not fetch JPEG columns.
        sample = read_training_sample(
            catalog, {**index, "observation_fields": ["observation_state"]}
        )
        states.append(sample["observation"]["state"])
        actions.append(sample["action"])
        if (i + 1) % 32 == 0 or i + 1 == len(indices):
            print(f"Read {index['split']}: {i + 1}/{len(indices)}", flush=True)
    x, y = np.asarray(states, dtype=np.float32), np.asarray(actions, dtype=np.float32)
    if (
        x.ndim != 3
        or y.ndim != 3
        or not np.isfinite(x).all()
        or not np.isfinite(y).all()
    ):
        raise ValueError(
            "Expected finite state/action arrays shaped [samples, steps, dimensions]"
        )
    return x, y


def vector_stats(values):
    mean = values.mean(axis=(0, 1))
    std = values.std(axis=(0, 1))
    return mean, np.where(std < 1e-6, 1.0, std)


def make_model(observation_shape, action_shape):
    import torch.nn as nn

    return nn.Sequential(
        nn.Flatten(),
        nn.Linear(int(np.prod(observation_shape)), 128),
        nn.ReLU(),
        nn.Linear(128, 128),
        nn.ReLU(),
        nn.Linear(128, int(np.prod(action_shape))),
        nn.Unflatten(1, tuple(action_shape)),
    )


def train_model(x_train, y_train, x_val, y_val, args):
    import torch
    from torch.utils.data import DataLoader, TensorDataset

    torch.manual_seed(args.seed)
    torch.set_num_threads(args.threads)
    x_mean, x_scale = vector_stats(x_train)
    y_mean, y_scale = vector_stats(y_train)
    tx = torch.from_numpy((x_train - x_mean) / x_scale)
    ty = torch.from_numpy((y_train - y_mean) / y_scale)
    vx = torch.from_numpy((x_val - x_mean) / x_scale)
    vy = torch.from_numpy((y_val - y_mean) / y_scale)
    loader = DataLoader(
        TensorDataset(tx, ty), batch_size=args.batch_size, shuffle=True, num_workers=0
    )
    model = make_model(x_train.shape[1:], y_train.shape[1:]).cpu()
    optimizer = torch.optim.Adam(model.parameters(), lr=args.learning_rate)
    loss_fn = torch.nn.MSELoss()
    best_loss, best_epoch, best_weights = float("inf"), 0, None
    history = []
    for epoch in range(1, args.epochs + 1):
        model.train()
        total = 0.0
        for x, y in loader:
            optimizer.zero_grad()
            loss = loss_fn(model(x), y)
            loss.backward()
            optimizer.step()
            total += loss.item() * len(x)
        model.eval()
        with torch.no_grad():
            val_loss = loss_fn(model(vx), vy).item()
        train_loss = total / len(tx)
        if not np.isfinite(train_loss) or not np.isfinite(val_loss):
            raise ValueError("Training produced a non-finite loss")
        history.append(
            {
                "epoch": epoch,
                "train_mse_normalized": train_loss,
                "val_mse_normalized": val_loss,
            }
        )
        print(
            f"Epoch {epoch:03d}: train MSE={train_loss:.6f}, val MSE={val_loss:.6f}",
            flush=True,
        )
        if val_loss < best_loss:
            best_loss, best_epoch = val_loss, epoch
            best_weights = copy.deepcopy(model.state_dict())

    model.load_state_dict(best_weights)
    with torch.no_grad():
        predictions = model(vx).numpy() * y_scale + y_mean
    # A useful reference: always predict the training-set mean action.
    baseline_mse = float(np.mean(((y_val - y_mean) / y_scale) ** 2))
    metrics = {
        "best_epoch": best_epoch,
        "best_val_mse_normalized": best_loss,
        "mean_action_baseline_val_mse_normalized": baseline_mse,
        "beats_mean_action_baseline": best_loss < baseline_mse,
        "val_mae_per_action_dimension": np.abs(predictions - y_val)
        .mean(axis=(0, 1))
        .tolist(),
        "history": history,
    }
    checkpoint = {
        "model_state_dict": best_weights,
        "observation_shape": list(x_train.shape[1:]),
        "action_shape": list(y_train.shape[1:]),
        "state_mean": torch.from_numpy(x_mean),
        "state_scale": torch.from_numpy(x_scale),
        "action_mean": torch.from_numpy(y_mean),
        "action_scale": torch.from_numpy(y_scale),
        "best_epoch": best_epoch,
    }
    return checkpoint, predictions, metrics


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--samples-table", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_STATE_POLICY_OUTPUT,
        help="Output directory (default: state_policy_output beside common.py)",
    )
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--train-samples", type=int, default=256)
    parser.add_argument("--val-samples", type=int, default=64)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    if (
        min(
            args.epochs,
            args.batch_size,
            args.train_samples,
            args.val_samples,
            args.threads,
        )
        < 1
        or args.learning_rate <= 0
    ):
        parser.error("Counts and learning-rate must be positive")
    return args


def main():
    args = parse_args()
    print(f"Output directory: {args.output.resolve()}", flush=True)

    import torch
    from daft import col
    from common import catalog_from_environment

    catalog = catalog_from_environment(args.namespace)
    rows = (
        catalog.table(args.samples_table)
        .scan()
        .to_daft()
        .where((col("split") == "train") | (col("split") == "val"))
        .to_arrow()
        .to_pylist()
    )
    episode_sets = {
        split: {
            (r["prepared_namespace"], r["prepared_table"], r["episode_id"])
            for r in rows
            if r["split"] == split
        }
        for split in ("train", "val")
    }
    if episode_sets["train"] & episode_sets["val"]:
        raise ValueError("An episode appears in both train and val")
    train_indices = choose_indices(rows, "train", args.train_samples, args.seed)
    val_indices = choose_indices(rows, "val", args.val_samples, args.seed)
    x_train, y_train = load_vectors(catalog, train_indices)
    x_val, y_val = load_vectors(catalog, val_indices)
    if x_train.shape[1:] != x_val.shape[1:] or y_train.shape[1:] != y_val.shape[1:]:
        raise ValueError("Train and val window shapes differ")
    checkpoint, predictions, metrics = train_model(x_train, y_train, x_val, y_val, args)
    args.output.mkdir(parents=True, exist_ok=True)
    checkpoint["samples_table"] = f"{args.namespace}.{args.samples_table}"
    torch.save(checkpoint, args.output / "model.pt")
    # Verify the saved weights reproduce the same predictions after reloading.
    saved = torch.load(args.output / "model.pt", map_location="cpu", weights_only=True)
    restored = make_model(saved["observation_shape"], saved["action_shape"])
    restored.load_state_dict(saved["model_state_dict"])
    restored.eval()
    with torch.no_grad():
        check = restored(
            (torch.from_numpy(x_val) - saved["state_mean"]) / saved["state_scale"]
        )
        check = (check * saved["action_scale"] + saved["action_mean"]).numpy()
    np.testing.assert_allclose(check, predictions, rtol=1e-5, atol=1e-6)
    metrics["checkpoint_reload_verified"] = True
    metrics.update(train_samples=len(train_indices), val_samples=len(val_indices))
    np.savez_compressed(
        args.output / "predictions.npz",
        state=x_val,
        predicted_action=predictions,
        target_action=y_val,
    )
    (args.output / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    provenance = {
        "config": {**vars(args), "output": str(args.output)},
        "train": train_indices,
        "val": val_indices,
    }
    (args.output / "samples.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(f"Best validation MSE: {metrics['best_val_mse_normalized']:.6f}")
    print(
        f"Mean-action baseline MSE: {metrics['mean_action_baseline_val_mse_normalized']:.6f}"
    )
    print(
        f"Saved model, metrics, predictions and sample references to {args.output.resolve()}"
    )


if __name__ == "__main__":
    main()
