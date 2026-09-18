# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Tiny training loop over an embodied LakeSoul table.

It windows ``state`` and ``action`` around anchor ticks, wraps the samples in
the PyTorch adapter (shuffle + prefetch), and trains a linear model for a few
steps so the whole "select -> window -> train" path is exercised.

Example:
    python python/examples/embodied/train.py --table embodied_demo --epochs 2
"""

from __future__ import annotations

import argparse
import time

import torch
from torch.utils.data import DataLoader

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset
from lakesoul.embodied.torch import Dataset as EmbodiedTorchDataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="embodied_demo")
    parser.add_argument("--episodes", default=None, help="comma-separated episode ids")
    parser.add_argument("--state-window", type=int, default=4)
    parser.add_argument("--action-window", type=int, default=4)
    parser.add_argument("--stride", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--steps", type=int, default=20, help="steps per epoch")
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--num-workers", type=int, default=0)
    parser.add_argument("--prefetch", type=int, default=4)
    parser.add_argument("--shuffle-buffer", type=int, default=64)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--device", default="cpu")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    table = LakeSoulCatalog.from_env().table(args.table)

    state_dim = table.schema.field("state").type.list_size
    action_dim = table.schema.field("action").type.list_size
    episodes = args.episodes.split(",") if args.episodes else None

    dataset = EmbodiedDataset(
        table.scan(),
        window={
            "state": (-args.state_window, 0),
            "action": (0, args.action_window),
        },
        stride=args.stride,
        episodes=episodes,
        seed=args.seed,
    )
    torch_dataset = EmbodiedTorchDataset(
        dataset,
        shuffle_buffer=args.shuffle_buffer,
        prefetch=args.prefetch,
    )
    loader = DataLoader(
        torch_dataset,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
    )

    model = torch.nn.Linear(
        args.state_window * state_dim,
        args.action_window * action_dim,
    ).to(args.device)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    for epoch in range(args.epochs):
        torch_dataset.set_epoch(epoch)
        epoch_start = time.perf_counter()
        samples = 0
        last_loss = 0.0
        for step, batch in enumerate(loader):
            if step >= args.steps:
                break
            states = batch["state"].float().flatten(1).to(args.device)
            actions = batch["action"].float().flatten(1).to(args.device)
            loss = torch.nn.functional.mse_loss(model(states), actions)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            samples += states.shape[0]
            last_loss = float(loss.detach())
        elapsed = time.perf_counter() - epoch_start
        print(
            f"epoch {epoch}: {samples} samples in {elapsed:.2f}s "
            f"({samples / elapsed:.1f} samples/s), loss={last_loss:.4f}"
        )


if __name__ == "__main__":
    main()
