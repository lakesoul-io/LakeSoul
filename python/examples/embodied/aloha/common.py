"""Shared ALOHA field names, CLI parsing and LakeSoul connection setup."""

import argparse
import os

STATE_COLUMN = "observation_state"
ACTION_COLUMN = "action"
EPISODE_COLUMN = "episode_id"
FRAME_COLUMN = "frame_index"
TIMESTAMP_COLUMN = "timestamp"
DEFAULT_CAMERAS = ("cam_high", "cam_left_wrist")


def parse_csv(value: str) -> list[str]:
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    if not parsed:
        raise argparse.ArgumentTypeError("at least one value is required")
    return parsed


def parse_episodes(value: str) -> list[int]:
    try:
        episodes = [int(item) for item in parse_csv(value)]
    except ValueError as error:
        raise argparse.ArgumentTypeError("episodes must be integer indices") from error
    if min(episodes) < 0 or len(set(episodes)) != len(episodes):
        raise argparse.ArgumentTypeError("episodes must be unique non-negative indices")
    return episodes


def image_column(camera: str) -> str:
    # LakeSoul's LeRobot importer uses the last component of each camera key.
    return camera.rsplit(".", 1)[-1]


def catalog_from_environment(namespace: str):
    from lakesoul.catalog import LakeSoulCatalog

    return LakeSoulCatalog.from_env(
        namespace=namespace,
        object_store_options={
            "fs.s3a.endpoint": os.environ["AWS_ENDPOINT"],
            "fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
            "fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "fs.s3a.path.style.access": "true",
        },
    )
