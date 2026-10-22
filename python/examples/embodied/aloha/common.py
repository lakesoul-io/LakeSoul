"""Shared ALOHA field names, CLI parsing and LakeSoul connection setup."""

import argparse
import os
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

# 训练和预测查看器共用；与启动命令时的工作目录无关。
DEFAULT_STATE_POLICY_OUTPUT = Path(__file__).resolve().parent / "state_policy_output"

STATE_COLUMN = "observation_state"
ACTION_COLUMN = "action"
EPISODE_COLUMN = "episode_id"
FRAME_COLUMN = "frame_index"
TIMESTAMP_COLUMN = "timestamp"
DEFAULT_CAMERAS = ("cam_high", "cam_low", "cam_left_wrist", "cam_right_wrist")


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


def metadata_root(raw_path):
    """Shared by import, notebook publication and preparation."""
    return str(raw_path).rstrip("/") + "_meta"


def uri_filesystem(uri):
    import pyarrow.fs as fs

    uri = str(uri)
    parsed = urlparse(uri)
    if parsed.scheme in ("s3", "s3a"):
        options = dict(
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            session_token=os.getenv("AWS_SESSION_TOKEN"),
            region=os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1",
        )
        endpoint = os.getenv("AWS_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            options.update(
                endpoint_override=endpoint,
                scheme="http" if endpoint.startswith("http://") else "https",
            )
        return fs.S3FileSystem(**options), parsed.netloc + parsed.path
    if not parsed.scheme:
        return fs.LocalFileSystem(), str(Path(uri).resolve())
    return fs.FileSystem.from_uri(uri)


def read_uri(uri, *, optional=False):
    import pyarrow.fs as fs

    filesystem, path = uri_filesystem(uri)
    if filesystem.get_file_info(path).type == fs.FileType.NotFound:
        if optional:
            return None
        raise FileNotFoundError(f"Required metadata is missing: {uri}")
    with filesystem.open_input_file(path) as stream:
        return stream.read()


def write_uri(uri, payload):
    import pyarrow.fs as fs

    filesystem, path = uri_filesystem(uri)
    if isinstance(filesystem, fs.LocalFileSystem):
        filesystem.create_dir(str(Path(path).parent), recursive=True)
    with filesystem.open_output_stream(path) as stream:
        stream.write(payload)


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def read_import_manifest(raw_path, namespace, raw_table):
    uri = metadata_root(raw_path) + "/manifest.json"
    metadata = json.loads(read_uri(uri))
    if (
        metadata.get("status") != "imported"
        or not metadata.get("import_id")
        or metadata.get("namespace") != namespace
        or metadata.get("raw_table") != raw_table
        or str(metadata.get("raw_path")).rstrip("/") != str(raw_path).rstrip("/")
    ):
        raise ValueError(
            f"Manifest does not match a completed import of this Raw table: {uri}"
        )
    return metadata


def read_annotation_version(raw_path, manifest):
    """Resolve the pointer once and verify the immutable version files."""
    root = metadata_root(raw_path) + "/annotations/"
    latest_uri = root + "latest.json"
    payload = read_uri(latest_uri, optional=True)
    if payload is None:
        return (
            [],
            [],
            {"latest_uri": latest_uri, "published": False, "excluded_behaviors": []},
        )
    pointer = json.loads(payload)
    if pointer.get("import_id") != manifest["import_id"]:
        raise ValueError("Annotation version belongs to another Raw import")
    documents = []
    for name in ("annotations", "review"):
        uri = pointer[name + "_uri"]
        if not uri.startswith(root) or "/../" in uri:
            raise ValueError(
                "Annotation version must reside in this Raw metadata directory"
            )
        data = read_uri(uri)
        if sha256(data) != pointer[name + "_sha256"]:
            raise ValueError(f"Annotation checksum mismatch: {uri}")
        documents.append(json.loads(data))
    annotations, review = documents
    if (
        not isinstance(annotations, list)
        or review.get("import_id") != manifest["import_id"]
    ):
        raise ValueError("Invalid annotation version")
    return (
        annotations,
        review["segments"],
        dict(pointer, latest_uri=latest_uri, published=True),
    )


def publish_annotations(raw_path, manifest, episode_id, segments):
    """Publish one episode's review, preserving other episodes.

    Single-reviewer workflow: concurrent publishers are last-writer-wins.
    Upload both version files before updating the fixed pointer.
    """
    _, previous, _ = read_annotation_version(raw_path, manifest)
    if not segments or any(
        s["episode_id"] != episode_id or not s.get("confirmed") for s in segments
    ):
        raise ValueError("Confirm all segments of this episode before publishing")
    combined = [s for s in previous if s["episode_id"] != episode_id] + list(segments)
    policies = {}
    for segment in combined:
        label, selected = segment["behavior"], segment["selected"]
        if label in policies and policies[label] != selected:
            raise ValueError(
                f"Conflicting keep/exclude policy across episodes: {label}"
            )
        policies[label] = selected
    combined.sort(key=lambda s: (s["episode_id"], s["start_frame"]))
    annotations = [
        {
            k: s[k]
            for k in (
                "episode_id",
                "start_frame",
                "end_frame_exclusive",
                "behavior",
                "reason",
            )
        }
        for s in combined
    ]
    revision = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ") + "_" + uuid4().hex[:8]
    )
    root = metadata_root(raw_path) + "/annotations/"
    pointer = dict(
        revision=revision,
        import_id=manifest["import_id"],
        excluded_behaviors=sorted(k for k, v in policies.items() if not v),
    )
    review = dict(
        import_id=manifest["import_id"],
        revision=revision,
        segments=combined,
        namespace=manifest["namespace"],
        raw_table=manifest["raw_table"],
    )
    for name, document in (("annotations", annotations), ("review", review)):
        uri = root + f"aloha_{name}_{revision}.json"
        payload = json_bytes(document)
        write_uri(uri, payload)
        pointer[name + "_uri"] = uri
        pointer[name + "_sha256"] = sha256(payload)
    write_uri(root + "latest.json", json_bytes(pointer))
    return pointer
