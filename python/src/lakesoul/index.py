# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""LakeSoul secondary index orchestration (kind-agnostic).

Every index kind (vector, text, ...) shares the same orchestration:

1. the kind declares a :class:`IndexKindSpec` (table property key, config
   parser/validator, per-shard builder) and registers it here,
2. configs are read from the ``{kind}_index_columns`` table property,
3. files are grouped by ``(partition_desc, hash_bucket_id)`` shard,
4. each shard is built by the kind's native builder,
5. the same drivers serve explicit builds (``build_partition_index`` /
   ``build_table_index``) and the post-write auto-build
   (``incremental_build_index``).

Kind-specific modules (``lakesoul.vector_index``, ``lakesoul.text_index``)
stay thin: they only map configs onto their native builder.
"""

from __future__ import annotations

import collections
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from .metadata.native_client import NativeMetadataClient

#: Suffix of the table property declaring a kind's configurations.
INDEX_PROPERTY_SUFFIX = "_index_columns"


@dataclass
class ShardInfo:
    """A set of files belonging to one (partition, bucket) shard."""

    partition_desc: str
    bucket_id: int
    file_paths: list[str]
    primary_keys: list[str]


@dataclass(frozen=True)
class IndexKindSpec:
    """Everything the generic drivers need to know about one index kind."""

    #: Kind name, e.g. ``"vector"``; used in option keys and messages.
    name: str
    #: Table property key, e.g. ``"vector_index_columns"``.
    property_key: str
    #: Parse the property value into configuration dicts (native parser).
    parse_configs: Callable[[str], list[dict[str, Any]]]
    #: Validate configurations against a table (schema + primary keys).
    validate: Callable[
        [Sequence[Mapping[str, Any]], pa.Schema, Sequence[str]],
        None,
    ]
    #: Build one shard: ``(store_config, file_paths, pk_column, config,
    #: rebuild) -> "ok"``; raises on failure.
    build_shard: Callable[
        [Mapping[str, str], list[str], str, Mapping[str, Any], bool],
        str,
    ]
    #: Parameters every entry of the property must provide (besides
    #: ``column``), checked before any metadata is created.
    required_params: tuple[str, ...] = ()


_KIND_SPECS: dict[str, IndexKindSpec] = {}


def register_index_kind(spec: IndexKindSpec) -> None:
    """Register the spec of one index kind (called by kind modules)."""
    _KIND_SPECS[spec.name] = spec


def _load_kind(name: str) -> None:
    """Import the module that registers ``name``, if known."""
    if name == "vector":
        import lakesoul.vector_index  # noqa: F401  (registers itself)
    elif name == "text":
        import lakesoul.text_index  # noqa: F401  (registers itself)


def index_kind_spec(name: str) -> IndexKindSpec:
    """Return the spec of ``name``; raises ``ValueError`` when unknown."""
    spec = _KIND_SPECS.get(name)
    if spec is None:
        _load_kind(name)
        spec = _KIND_SPECS.get(name)
    if spec is None:
        raise ValueError(f"unknown index kind: {name!r}")
    return spec


def find_index_kind_spec(name: str) -> IndexKindSpec | None:
    """Like :func:`index_kind_spec`, but ``None`` for unknown kinds."""
    try:
        return index_kind_spec(name)
    except ValueError:
        return None


def configured_index_kinds(properties: Mapping[str, Any]) -> list[str]:
    """Index kinds declared by a table's properties."""
    kinds = []
    for key in properties:
        if key.endswith(INDEX_PROPERTY_SUFFIX):
            kinds.append(key[: -len(INDEX_PROPERTY_SUFFIX)])
    return kinds


def extract_bucket_id(file_path: str) -> int:
    """Extract hash bucket id from a parquet file path.

    File names follow the pattern ``part-{random}_{bucket_id:0>4}.parquet``.
    """
    match = re.search(r".*_(\d+)(?:\..*)?$", file_path)
    if not match:
        raise ValueError(f"Cannot determine bucket id from file name {file_path}")
    return int(match.group(1))


def group_files_by_shard(
    client: NativeMetadataClient,
    table_id: str,
    partition_desc: str,
    pk_cols: list[str],
) -> list[ShardInfo]:
    """Query PG for a partition's current data files and group by bucket."""
    partition_infos = client.get_partition_info_by_table_id_and_desc(
        table_id, partition_desc
    )
    if not partition_infos:
        return []

    latest = max(partition_infos, key=lambda p: p.version)
    bucket_files: dict[int, list[str]] = collections.defaultdict(list)
    data_commits = client.list_data_commit_info(
        latest.table_id, latest.partition_desc, latest.snapshot
    )
    for commit in data_commits:
        for file_op in commit.file_ops:
            if file_op.file_op == 0:  # FileOp.add
                bid = extract_bucket_id(file_op.path)
                bucket_files[bid].append(file_op.path)

    return [
        ShardInfo(
            partition_desc=partition_desc,
            bucket_id=bid,
            file_paths=paths,
            primary_keys=pk_cols,
        )
        for bid, paths in sorted(bucket_files.items())
    ]


def group_file_infos_by_shard(
    file_infos: Sequence[Any],
) -> dict[tuple[str, int], list[str]]:
    """Group freshly written files by ``(partition_desc, bucket_id)``.

    Files from different range partitions never share a shard: the native
    builder derives the index location from the files' partition directory.
    """
    shards: dict[tuple[str, int], list[str]] = collections.defaultdict(list)
    for file_info in file_infos:
        shards[(file_info.partition, extract_bucket_id(file_info.path))].append(
            file_info.path
        )
    return shards


def parse_index_configs(kind: str, raw: str) -> list[dict[str, Any]]:
    """Parse a ``{kind}_index_columns`` property value via the native parser."""
    return list(index_kind_spec(kind).parse_configs(raw))


def normalize_index_configs(kind: str, value: Any) -> list[dict[str, Any]]:
    """Normalize a user-supplied ``index_configs`` entry into config dicts.

    ``column`` and the kind's required parameters are checked here so a
    mistake is reported before any metadata is created.
    """
    spec = index_kind_spec(kind)
    if isinstance(value, dict):
        items = [value]
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        raise TypeError(f"{kind}_index must be a dict or a list of dicts")
    required = ("column", *spec.required_params)
    for item in items:
        if not isinstance(item, dict):
            raise TypeError(f"each {kind}_index entry must be a dict")
        missing = [key for key in required if key not in item]
        if missing:
            names = ", ".join(repr(key) for key in required)
            raise ValueError(f"each {kind}_index entry requires {names}")
    return items


def validate_index_configs(
    kind: str,
    configs: Sequence[Mapping[str, Any]],
    schema: pa.Schema,
    primary_keys: Sequence[str],
) -> None:
    """Validate a table can support the configured index (native rules)."""
    if not configs:
        return
    index_kind_spec(kind).validate(configs, schema, primary_keys)


def table_index_configs(table: Any, kind: str) -> list[dict[str, Any]]:
    """Parse a table's ``{kind}_index_columns`` property into config dicts."""
    spec = index_kind_spec(kind)
    raw = dict(table.properties).get(spec.property_key, "")
    if not raw:
        return []
    return list(spec.parse_configs(raw))


def config_for_column(
    configs: Sequence[Mapping[str, Any]],
    column: str | None,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Pick one config entry and apply explicit overrides.

    ``column=None`` selects the first configured entry.  Explicit overrides
    (``dim``/``nlist``/...) take precedence over the property values; a
    column may be built even when it has no property entry as long as the
    overrides carry the required parameters.
    """
    selected: Mapping[str, Any] | None = None
    if column is not None:
        selected = next(
            (c for c in configs if c.get("column") == column), None
        )
    elif configs:
        selected = configs[0]
    resolved: dict[str, Any] = dict(selected) if selected is not None else {}
    if column is not None:
        resolved["column"] = column
    for key, value in (overrides or {}).items():
        if value is not None:
            resolved[key] = value
    if "column" not in resolved or resolved["column"] is None:
        raise ValueError(
            "index column not specified and not found in table properties"
        )
    return resolved


def default_object_store_config(catalog: Any, table: Any) -> dict:
    """Build the native ``store_config`` dict for a table's index builder."""
    path = table.path
    if path.startswith("file://"):
        return {"type": "local"}
    opts = dict(catalog.object_store_options)
    config: dict = {"type": "s3"}
    # Pass through all fs.s3a.* keys so both the index store (create_s3_store)
    # and the reader (reader_config_builder) get them.
    for key, value in opts.items():
        if key.startswith("fs.s3a."):
            config[key] = value
    # Bucket: prefer explicit config, fall back to path
    if "fs.s3a.bucket" not in config:
        if path.startswith("s3://") or path.startswith("s3a://"):
            rest = path.split("://", 1)[1]
            config["fs.s3a.bucket"] = rest.split("/", 1)[0]
    return config


def _s3_clean_table_path(table_path: str) -> str:
    return (
        table_path.replace("file://", "")
        .replace("s3://", "")
        .replace("s3a://", "")
    )


def build_shard(
    kind: str,
    store_config: Mapping[str, str],
    file_paths: Sequence[str],
    pk_column: str,
    config: Mapping[str, Any],
    *,
    rebuild: bool = False,
) -> str:
    """Build one index shard with the kind's native builder."""
    return index_kind_spec(kind).build_shard(
        store_config, list(file_paths), pk_column, config, rebuild
    )


def build_partition_index(
    kind: str,
    *,
    table_name: str,
    namespace: str,
    partition_desc: str,
    config: Mapping[str, Any],
    store_config: Mapping[str, str] | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Build (or update, or rebuild) one partition's index shards.

    Args:
        kind: Index kind name, e.g. ``"vector"``.
        table_name: LakeSoul table name.
        namespace: LakeSoul namespace.
        partition_desc: Partition descriptor, e.g. ``"range=2024-01-01"``.
        config: Fully resolved configuration of the indexed column.
        store_config: Object-store credentials; ``None`` means local storage.
        rebuild: Rebuild every shard from scratch instead of appending a
            delta update.

    Returns:
        Summary dict with ``status``, ``shards_total``, ``shards_succeeded``,
        ``table_path``, ``column`` and ``partition_desc``.
    """
    store_config = dict(store_config) if store_config else {"type": "local"}
    client = NativeMetadataClient.from_env()
    table_info = client.get_table_info_by_name(table_name, namespace)
    _, pk_cols = client.get_partition_and_pk_cols(table_info)
    if not pk_cols:
        raise ValueError(
            f"Table '{table_name}' has no primary key columns defined. "
            f"A {kind} index requires a u64 primary key."
        )
    pk_column = pk_cols[0]
    table_path = _s3_clean_table_path(table_info.table_path)

    shards = group_files_by_shard(
        client, table_info.table_id, partition_desc, pk_cols
    )
    if not shards:
        return {
            "status": "ok",
            "shards_total": 0,
            "shards_succeeded": 0,
            "table_path": table_path,
            "column": config["column"],
            "partition_desc": partition_desc,
            "message": "no data files found",
        }

    succeeded = 0
    failed = 0
    for shard in shards:
        try:
            result = build_shard(
                kind,
                store_config,
                shard.file_paths,
                pk_column,
                config,
                rebuild=rebuild,
            )
            if result == "ok":
                succeeded += 1
            else:
                failed += 1
        except Exception as error:  # noqa: BLE001 - report per-shard status
            print(
                f"ERROR building {kind} index for partition "
                f"{partition_desc}: {error}"
            )
            failed += 1

    if failed > 0:
        raise RuntimeError(
            f"{kind} index build failed for {failed}/{len(shards)} shards "
            f"of partition '{partition_desc}'"
        )

    return {
        "status": "ok",
        "shards_total": len(shards),
        "shards_succeeded": succeeded,
        "table_path": table_path,
        "column": config["column"],
        "partition_desc": partition_desc,
    }


def build_table_index(
    kind: str,
    *,
    table_name: str,
    namespace: str,
    configs: Sequence[Mapping[str, Any]],
    store_config: Mapping[str, str] | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Build (or rebuild) indexes for all partitions of a table.

    ``configs`` is the resolved configuration list (one entry per indexed
    column).
    """
    store_config = dict(store_config) if store_config else {"type": "local"}
    client = NativeMetadataClient.from_env()
    table_info = client.get_table_info_by_name(table_name, namespace)
    partition_infos = client.get_all_partition_info(table_info.table_id)

    results = []
    for config in configs:
        for pinfo in partition_infos:
            results.append(
                build_partition_index(
                    kind,
                    table_name=table_name,
                    namespace=namespace,
                    partition_desc=pinfo.partition_desc,
                    config=config,
                    store_config=store_config,
                    rebuild=rebuild,
                )
            )

    return {
        "status": "ok",
        "table_name": table_name,
        "columns": [config["column"] for config in configs],
        "partitions_total": len(partition_infos),
        "partitions_processed": sum(
            1 for result in results if result["status"] == "ok"
        ),
        "details": results,
    }


def incremental_build_index(
    kind: str,
    *,
    table: Any,
    file_infos: Sequence[Any],
    column: str | None = None,
) -> int:
    """Build/update configured index shards from freshly written files.

    Files are grouped by ``(partition_desc, hash_bucket_id)`` and the native
    builder performs a delta update when a commit already exists.  Returns
    the number of shards built; raises if any shard fails.
    """
    configs = table_index_configs(table, kind)
    if column is not None:
        configs = [c for c in configs if c.get("column") == column]
    if not configs or not file_infos:
        return 0
    primary_keys = list(table.primary_keys)
    if not primary_keys:
        raise ValueError(
            f"a {kind} index requires an id column: the table has "
            f"{index_kind_spec(kind).property_key} but no primary key"
        )
    pk_column = primary_keys[0]
    store_config = default_object_store_config(catalog=table.catalog, table=table)
    shards = group_file_infos_by_shard(file_infos)

    succeeded = 0
    failed = 0
    for config in configs:
        for (partition_desc, bucket_id), files in sorted(shards.items()):
            try:
                result = build_shard(
                    kind, store_config, sorted(files), pk_column, config
                )
                if result == "ok":
                    succeeded += 1
                else:
                    failed += 1
                    print(
                        f"ERROR building {kind} index for column "
                        f"'{config['column']}' shard (partition="
                        f"'{partition_desc}', bucket={bucket_id}): {result}"
                    )
            except Exception as error:  # noqa: BLE001 - report per-shard status
                failed += 1
                print(
                    f"ERROR building {kind} index for column "
                    f"'{config['column']}' shard (partition="
                    f"'{partition_desc}', bucket={bucket_id}): {error}"
                )

    if failed > 0:
        total = sum(len(shards) for _ in configs)
        raise RuntimeError(
            f"{kind} index build failed for {failed}/{total} shard(s)"
        )
    return succeeded


def rename_summary_key(result: dict[str, Any], key: str) -> dict[str, Any]:
    """Rename the generic ``column``/``columns`` summary keys for one kind.

    Vector callers have always seen ``vector_column``; keep that surface
    while the generic drivers return ``column``.
    """
    if "column" in result:
        result[key] = result.pop("column")
    elif "columns" in result:
        result[key] = result.pop("columns")[0]
    for detail in result.get("details", []):
        if "column" in detail:
            detail[key] = detail.pop("column")
    return result


def shard_build_rows(
    kind: str,
    configs: Sequence[Mapping[str, Any]],
    file_infos: Sequence[Any],
    store_config_json: str,
    pk_column: str,
) -> tuple[dict[str, list[Any]], int]:
    """Materialize per-shard build tasks as dataframe column dicts (Daft).

    One row per ``(partition_desc, bucket, column)`` shard; the distributed
    actor calls :func:`build_shard` with the row's config JSON.
    """
    import json

    shards = group_file_infos_by_shard(file_infos)
    rows: dict[str, list[Any]] = {
        "file_paths": [],
        "store_config_json": [],
        "pk_column": [],
        "kind": [],
        "config_json": [],
    }
    for (partition_desc, bucket_id) in sorted(shards):
        files = sorted(shards[(partition_desc, bucket_id)])
        for config in configs:
            rows["file_paths"].append(files)
            rows["store_config_json"].append(store_config_json)
            rows["pk_column"].append(pk_column)
            rows["kind"].append(kind)
            rows["config_json"].append(json.dumps(dict(config)))
    return rows, len(rows["file_paths"])


__all__ = [
    "IndexKindSpec",
    "ShardInfo",
    "build_partition_index",
    "build_shard",
    "build_table_index",
    "config_for_column",
    "configured_index_kinds",
    "default_object_store_config",
    "extract_bucket_id",
    "find_index_kind_spec",
    "group_file_infos_by_shard",
    "group_files_by_shard",
    "incremental_build_index",
    "index_kind_spec",
    "normalize_index_configs",
    "parse_index_configs",
    "register_index_kind",
    "rename_summary_key",
    "shard_build_rows",
    "table_index_configs",
    "validate_index_configs",
]
