# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pyarrow.dataset as ds

from lakesoul._lib._utils import _schema_from_metadata_str
from lakesoul.io import IOConfig, Writer, WriteResult, merge_blob_option
from lakesoul.metadata import (
    NativeMetadataClient,
    PostgresMetadataConfig,
    TableInfo,
    TableNotFoundError,
)

from . import index as _index

if TYPE_CHECKING:
    from lakesoul._lib.vector import VectorIndexConfig

DEFAULT_SCAN_BATCH_SIZE: int = 2**10
#: Reserved output column carrying the per-row BM25 score when the scan
#: requests ``text_search_scores=true``.
TEXT_SEARCH_SCORE_COLUMN = "__lakesoul_text_score"
PhysicalFormat = Literal["parquet", "vortex", "vortex-compact"]
_ASCII_LOWER_TRANS = str.maketrans(
    {chr(code): chr(code + 32) for code in range(ord("A"), ord("Z") + 1)}
)


@dataclass(frozen=True, slots=True)
class TableWriteConfig:
    table_name: str
    namespace: str
    path: str
    schema: pa.Schema
    primary_keys: tuple[str, ...]
    partition_by: tuple[str, ...]
    hash_bucket_num: int
    format: PhysicalFormat
    vector_columns: tuple[str, ...] = ()
    blob_columns: str | None = None


class LakeSoulCatalog:
    """Main Python entry point for LakeSoul metadata and table IO."""

    def __init__(
        self,
        metadata_config: PostgresMetadataConfig | None = None,
        *,
        pg_url: str | None = None,
        pg_username: str | None = None,
        pg_password: str | None = None,
        pg_secondary_url: str | None = None,
        max_retry: int = 3,
        namespace: str = "default",
        object_store_options: Mapping[str, str] | None = None,
        _client: Any | None = None,
    ) -> None:
        if not namespace:
            raise ValueError("namespace must not be empty")

        if _client is not None:
            client = _client
        elif metadata_config is None:
            if pg_url is None or pg_username is None or pg_password is None:
                client = NativeMetadataClient.from_env()
            else:
                metadata_config = PostgresMetadataConfig(
                    url=pg_url,
                    username=pg_username,
                    password=pg_password,
                    secondary_url=pg_secondary_url,
                    max_retry=max_retry,
                )
                client = NativeMetadataClient(
                    metadata_config.primary_config(),
                    metadata_config.secondary_config(),
                    metadata_config.max_retry,
                )
        else:
            client = NativeMetadataClient(
                metadata_config.primary_config(),
                metadata_config.secondary_config(),
                metadata_config.max_retry,
            )

        self._client = client
        self._namespace = namespace
        self._object_store_options = MappingProxyType(
            _validate_string_mapping("object_store_options", object_store_options or {})
        )

    @classmethod
    def from_env(
        cls,
        *,
        namespace: str = "default",
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulCatalog:
        return cls(
            namespace=namespace,
            object_store_options=object_store_options,
            _client=NativeMetadataClient.from_env(),
        )

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def object_store_options(self) -> Mapping[str, str]:
        return self._object_store_options

    def list_namespaces(self) -> tuple[str, ...]:
        return self._client.list_namespaces()

    def list_tables(self, namespace: str | None = None) -> tuple[str, ...]:
        return self._client.list_tables(self._resolve_namespace(namespace))

    def table(self, name: str, namespace: str | None = None) -> LakeSoulTable:
        """Load a table.

        Raises:
            TableNotFoundError: If the table does not exist.
            MetadataError: If the metadata operation fails.
        """
        namespace = self._resolve_namespace(namespace)
        table_info = self._client.get_table_info_by_name(
            _case_fold_identifier(name),
            namespace,
        )
        return LakeSoulTable(self, table_info)

    def scan(
        self,
        name: str,
        *,
        namespace: str | None = None,
        partitions: Mapping[str, str] | None = None,
        columns: Sequence[str] | str | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        retain_partition_columns: bool = False,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        _validate_scan_runtime_options(
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
        )
        return self.table(name, namespace).scan(
            partitions=partitions,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
            retain_partition_columns=retain_partition_columns,
            object_store_options=object_store_options,
        )

    def dataset(
        self,
        name: str,
        *,
        namespace: str | None = None,
        partitions: Mapping[str, str] | None = None,
        batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        retain_partition_columns: bool = False,
        object_store_options: Mapping[str, str] | None = None,
    ) -> ds.Dataset:
        return self.scan(
            name,
            namespace=namespace,
            partitions=partitions,
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
            retain_partition_columns=retain_partition_columns,
            object_store_options=object_store_options,
        ).to_arrow_dataset()

    def create_table(
        self,
        name: str,
        *,
        path: str | Path,
        schema: pa.Schema,
        namespace: str | None = None,
        partition_by: Sequence[str] = (),
        primary_keys: Sequence[str] = (),
        hash_bucket_num: int | None = None,
        properties: Mapping[str, str] | None = None,
        vector_index: Any | None = None,
        text_index: Any | None = None,
        index_configs: Mapping[str, Any] | None = None,
        domain: str = "public",
    ) -> LakeSoulTable:
        """Create and load a table.

        ``index_configs`` (optional) maps an index kind (``"vector"``,
        ``"text"``, ...) to one or more column configurations; each kind is
        stored in its ``{kind}_index_columns`` table property as JSON.
        ``vector_index`` and ``text_index`` are the kind short-hands for
        ``index_configs={"vector": ...}`` and ``index_configs={"text": ...}``.
        Each vector entry must have ``column`` and ``dim``; ``nlist``/
        ``total_bits``/``metric``/``rotator_type``/``seed``/
        ``use_faster_config`` default if omitted.  Each text entry must have
        ``column``; ``tokenizer``/``with_positions``/``stored`` default if
        omitted, and ``rebuild_mode``/``max_delta_ratio`` control the
        automatic compaction of the text index (defaults ``"auto"``/``1.0``).
        When a property is present, ``write_arrow`` automatically
        builds/updates the index.  An index requires an Int64/UInt64
        ``primary_keys`` column; vector columns must be Float32 with a
        matching dimension, text columns must be Utf8.  The configuration is
        validated here, before any metadata is created.

        Raises:
            AlreadyExistsError: If the table already exists.
            NamespaceNotFoundError: If the namespace does not exist.
            MetadataError: If the metadata operation fails.
        """
        if not isinstance(schema, pa.Schema):
            raise TypeError("schema must be a pyarrow.Schema")
        table_name = _case_fold_identifier(name)
        normalized_schema = _case_fold_schema(schema)
        normalized_partition_by = _case_fold_columns("partition_by", partition_by)
        normalized_primary_keys = _case_fold_columns("primary_keys", primary_keys)
        _validate_columns("partition_by", normalized_partition_by, normalized_schema)
        _validate_columns("primary_keys", normalized_primary_keys, normalized_schema)
        normalized_schema = _make_primary_keys_non_nullable(
            normalized_schema,
            normalized_primary_keys,
        )
        if hash_bucket_num is not None:
            _validate_hash_bucket_num(hash_bucket_num)

        props = dict(properties or {})
        if "blob_columns" in props:
            _validate_blob_columns_property(props["blob_columns"], normalized_schema)
        requested: dict[str, Any] = dict(index_configs or {})
        if vector_index is not None:
            requested["vector"] = vector_index
        if text_index is not None:
            requested["text"] = text_index
        for kind, value in requested.items():
            spec = _index.index_kind_spec(kind)
            props[spec.property_key] = json.dumps(
                _index.normalize_index_configs(kind, value)
            )
        if hash_bucket_num is not None:
            props["hashBucketNum"] = str(hash_bucket_num)
        elif normalized_primary_keys and "hashBucketNum" not in props:
            props["hashBucketNum"] = "4"

        # Fail before creating any metadata: every configured index is
        # validated against the schema and the primary keys.
        for kind in _index.configured_index_kinds(props):
            spec = _index.find_index_kind_spec(kind)
            if spec is None:
                if kind in requested:
                    raise ValueError(f"unknown index kind: {kind!r}")
                continue
            raw = props.get(spec.property_key)
            if raw:
                _index.validate_index_configs(
                    kind,
                    _index.parse_index_configs(kind, raw),
                    normalized_schema,
                    normalized_primary_keys,
                )

        namespace = self._resolve_namespace(namespace)
        self._client.create_table(
            table_name,
            namespace=namespace,
            table_path=path,
            table_schema=normalized_schema,
            properties=props,
            partitions={
                "range": list(normalized_partition_by),
                "hash": list(normalized_primary_keys),
            },
            domain=domain,
        )
        return self.table(table_name, namespace)

    def drop_table(
        self,
        name: str,
        namespace: str | None = None,
        *,
        if_exists: bool = False,
    ) -> None:
        """Drop a table.

        ``if_exists`` suppresses only :class:`TableNotFoundError`.

        Raises:
            TableNotFoundError: If the table does not exist and ``if_exists`` is false.
            MetadataError: If the metadata operation fails.
        """
        namespace = self._resolve_namespace(namespace)
        name = _case_fold_identifier(name)
        try:
            self._client.drop_table(name, namespace)
        except TableNotFoundError:
            if if_exists:
                return
            raise

    def _resolve_namespace(self, namespace: str | None) -> str:
        resolved = self._namespace if namespace is None else namespace
        if not resolved:
            raise ValueError("namespace must not be empty")
        return resolved

    def _merge_object_store_options(
        self,
        object_store_options: Mapping[str, str] | None,
    ) -> dict[str, str]:
        merged = dict(self._object_store_options)
        merged.update(
            _validate_string_mapping("object_store_options", object_store_options or {})
        )
        return merged

    def _commit_write_result(self, table: LakeSoulTable, result: WriteResult) -> None:
        files = [
            (
                file_info.partition,
                file_info.path,
                file_info.size,
                list(file_info.existing_columns),
            )
            for file_info in result.files
        ]
        if files:
            self._client.commit_data_files(table.name, table.namespace, files)


class LakeSoulTable:
    """A loaded LakeSoul table."""

    def __init__(self, catalog: LakeSoulCatalog, table_info: TableInfo) -> None:
        self._catalog = catalog
        self._table_info = table_info

    @property
    def catalog(self) -> LakeSoulCatalog:
        return self._catalog

    @property
    def name(self) -> str:
        return self._table_info.table_name

    @property
    def namespace(self) -> str:
        return self._table_info.table_namespace

    @property
    def id(self) -> str:
        return self._table_info.table_id

    @property
    def path(self) -> str:
        return self._table_info.table_path

    @property
    def schema(self) -> pa.Schema:
        return _schema_from_metadata_str(self._table_info.table_schema, None)

    @property
    def properties(self) -> Mapping[str, Any]:
        return MappingProxyType(_parse_table_properties(self._table_info.properties))

    @property
    def partition_by(self) -> tuple[str, ...]:
        partition_by, _ = self._catalog._client.get_partition_and_pk_cols(
            self._table_info
        )
        return tuple(partition_by)

    @property
    def primary_keys(self) -> tuple[str, ...]:
        _, primary_keys = self._catalog._client.get_partition_and_pk_cols(
            self._table_info
        )
        return tuple(primary_keys)

    @property
    def hash_bucket_num(self) -> int:
        raw_value = self.properties.get("hashBucketNum", "1")
        try:
            value = int(raw_value)
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"invalid hashBucketNum table property: {raw_value!r}"
            ) from error
        if value <= 0:
            raise ValueError(f"invalid hashBucketNum table property: {raw_value!r}")
        return value

    @property
    def blob_columns(self) -> tuple[str, ...]:
        """Columns externalized to side pack files via the ``blob_columns`` property."""
        raw = self._blob_columns_option()
        if raw is None:
            return ()
        parsed = json.loads(raw)
        return tuple(str(column) for column in parsed)

    def _blob_columns_option(self) -> str | None:
        raw = dict(self.properties).get("blob_columns")
        if raw is None:
            return None
        return raw if isinstance(raw, str) else json.dumps(raw)

    @property
    def vector_index_columns(self) -> tuple[str, ...]:
        """Columns declared in the ``vector_index_columns`` table property.

        Writers give these columns small row blocks so index builds and row
        lookups fetch less data per random access.
        """
        if "vector_index_columns" not in self.properties:
            return ()
        return tuple(str(config["column"]) for config in self._vector_configs())

    def scan(
        self,
        *,
        partitions: Mapping[str, str] | None = None,
        columns: Sequence[str] | str | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        retain_partition_columns: bool = True,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        """
        when `filter` is not None and references the range column,
        retain_partition_columns` must be True.
        """
        return LakeSoulScan(
            table=self,
            partitions=_normalize_string_mapping("partitions", partitions or {}),
            columns=_normalize_optional_columns(columns),
            filter=_validate_filter(filter),
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
            retain_partition_columns=retain_partition_columns,
            object_store_options=self._catalog._merge_object_store_options(
                object_store_options
            ),
        )

    def scan_plan(
        self,
        partitions: Mapping[str, str] | None = None,
    ) -> tuple[Any, ...]:
        return tuple(
            self._catalog._client.get_scan_plan_partitions(
                self.name,
                partitions=dict(partitions or {}),
                namespace=self.namespace,
            )
        )

    def write_config(
        self,
        *,
        format: PhysicalFormat = "vortex-compact",
    ) -> TableWriteConfig:
        if format not in {"parquet", "vortex", "vortex-compact"}:
            raise ValueError("format must be 'parquet', 'vortex', or 'vortex-compact'")
        return TableWriteConfig(
            table_name=self.name,
            namespace=self.namespace,
            path=self.path,
            schema=self.schema,
            primary_keys=self.primary_keys,
            partition_by=self.partition_by,
            hash_bucket_num=self.hash_bucket_num,
            format=format,
            vector_columns=self.vector_index_columns,
            blob_columns=self._blob_columns_option(),
        )

    def write_arrow(
        self,
        data: pa.RecordBatch | pa.Table | pa.RecordBatchReader,
        *,
        format: PhysicalFormat = "vortex-compact",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
        auto_build_vector_index: bool = True,
        auto_build_index: bool | None = None,
    ) -> WriteResult:
        # ``auto_build_index`` is the generic spelling; the vector-named
        # parameter is kept working for existing callers.
        auto_build = (
            auto_build_vector_index
            if auto_build_index is None
            else bool(auto_build_index)
        )
        # Fail before writing/committing if an auto-build cannot be satisfied.
        if auto_build:
            self._require_index_writable()
        write_config = self.write_config(format=format)
        writer_config = IOConfig(
            path=write_config.path,
            schema=write_config.schema,
            format=write_config.format,
            primary_keys=write_config.primary_keys,
            partition_by=write_config.partition_by,
            vector_columns=write_config.vector_columns,
            hash_bucket_num=write_config.hash_bucket_num,
            batch_size=batch_size,
            thread_num=thread_num,
            max_file_size=max_file_size,
            max_row_group_size=max_row_group_size,
            object_store_options=self._catalog._merge_object_store_options(
                object_store_options
            ),
            options=merge_blob_option(dict(options or {}), write_config.blob_columns),
        )
        with Writer(writer_config) as writer:
            writer.write(data)
        result = writer.result
        if result is None:
            raise RuntimeError("writer finished without a result")
        self._catalog._commit_write_result(self, result)

        if auto_build:
            self._auto_build_after_write(result)

        return result

    def _auto_build_after_write(self, result: WriteResult) -> None:
        """Build/update configured indexes for a freshly written ``result``.

        Iterates every index kind declared by a ``*_index_columns`` table
        property and builds/updates its shard indexes using only the newly
        written files (the native builder performs an incremental delta
        update).  Raises on any shard failure.
        """
        file_infos = list(result.files)
        if not file_infos:
            return
        for kind in _index.configured_index_kinds(self.properties):
            if _index.find_index_kind_spec(kind) is None:
                continue
            if not _index.table_index_configs(self, kind):
                continue
            _index.incremental_build_index(kind, table=self, file_infos=file_infos)

    def write_ray(
        self,
        dataset: Any,
        *,
        format: PhysicalFormat = "vortex-compact",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
        ray_remote_args: Mapping[str, Any] | None = None,
        concurrency: int | None = None,
    ) -> None:
        from lakesoul.ray.write_lakesoul import write_lakesoul

        write_lakesoul(
            dataset,
            self,
            format=format,
            batch_size=batch_size,
            thread_num=thread_num,
            max_file_size=max_file_size,
            max_row_group_size=max_row_group_size,
            object_store_options=object_store_options,
            options=options,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )

    def write_daft(
        self,
        dataframe: Any,
        *,
        format: PhysicalFormat = "vortex-compact",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
        results_buffer_size: int | Literal["num_cpus"] = "num_cpus",
        auto_build_vector_index: bool = True,
        vector_index_cpus: float = 1,
    ) -> WriteResult:
        """Write a Daft DataFrame (distributed).

        If the table declares ``*_index_columns`` properties, the configured
        indexes are built/updated automatically after the write commits via a
        distributed ``@daft.cls`` actor-pool UDF over the new files, grouped
        by (partition, hash bucket); drifted shards are compacted afterwards.
        Pass ``auto_build_vector_index=False`` to skip.
        """
        from lakesoul.daft import write_lakesoul

        return write_lakesoul(
            dataframe,
            self,
            format=format,
            batch_size=batch_size,
            thread_num=thread_num,
            max_file_size=max_file_size,
            max_row_group_size=max_row_group_size,
            object_store_options=object_store_options,
            options=options,
            results_buffer_size=results_buffer_size,
            auto_build_vector_index=auto_build_vector_index,
            vector_index_cpus=vector_index_cpus,
        )

    def build_vector_index(
        self,
        *,
        column: str | None = None,
        dim: int | None = None,
        nlist: int | None = None,
        total_bits: int | None = None,
        metric: str | None = None,
        rotator_type: str | None = None,
        seed: int | None = None,
        use_faster_config: bool | None = None,
        partition_desc: str | None = None,
        partitions: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Build or update the IVF+RaBitQ vector index for this table.

        The vector column and all index params are auto-detected from the
        ``vector_index_columns`` table property; explicit args override them.

        Args:
            column: Vector column name (auto-detected if omitted).
            dim: Vector dimension (auto-detected if omitted).
            nlist: Number of IVF clusters.
            total_bits: RaBitQ total bits.
            metric: Distance metric, ``"L2"`` or ``"IP"``.
            rotator_type: Rotation type, ``"FhtKac"`` or ``"Matrix"``.
            seed: Random seed.
            use_faster_config: Enable fast quantization.
            partition_desc: Build index for a single partition, e.g.
                ``"range=2024-01-01"``.  When omitted, builds for all
                partitions.
            partitions: Shorthand for partition_desc — a mapping of
                partition column names to values.  Only one of
                *partition_desc* or *partitions* may be specified.

        Returns:
            Dict with summary: ``{"status": "ok", "partitions": 2, ...}``.
        """
        if partition_desc is not None and partitions is not None:
            raise ValueError("partition_desc and partitions are mutually exclusive")

        # Auto-detect column + params from table properties; explicit args win.
        config = _index.config_for_column(
            self._index_configs("vector"),
            column,
            overrides={
                "dim": dim,
                "nlist": nlist,
                "total_bits": total_bits,
                "metric": metric,
                "rotator_type": rotator_type,
                "seed": seed,
                "use_faster_config": use_faster_config,
            },
        )
        self._require_vector_dim(config)
        store_config = _index.default_object_store_config(
            catalog=self._catalog, table=self
        )

        if partition_desc is not None:
            return _index.rename_summary_key(
                _index.build_partition_index(
                    "vector",
                    table_name=self.name,
                    namespace=self.namespace,
                    partition_desc=partition_desc,
                    config=config,
                    store_config=store_config,
                ),
                "vector_column",
            )

        if partitions is not None:
            return _index.rename_summary_key(
                _index.build_partition_index(
                    "vector",
                    table_name=self.name,
                    namespace=self.namespace,
                    partition_desc=self._partition_desc(partitions),
                    config=config,
                    store_config=store_config,
                ),
                "vector_column",
            )

        # Build for all partitions
        return _index.rename_summary_key(
            _index.build_table_index(
                "vector",
                table_name=self.name,
                namespace=self.namespace,
                configs=[config],
                store_config=store_config,
            ),
            "vector_column",
        )

    def rebuild_vector_index(
        self,
        *,
        column: str | None = None,
        partition_desc: str | None = None,
        partitions: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Rebuild the IVF+RaBitQ vector index from scratch for this table.

        Unlike incremental writes — which append vectors to the centroids
        trained at the original build — a rebuild re-reads every active data
        file of each shard, re-trains the IVF centroids on the full dataset,
        and publishes a new index generation.  Use this after the data
        distribution has drifted (or call it proactively as maintenance).

        Args:
            column: Vector column to rebuild.  When omitted, rebuilds all
                configured ``vector_index_columns``.
            partition_desc: Rebuild a single partition, e.g.
                ``"range=2024-01-01"``.  When omitted, rebuilds all
                partitions.
            partitions: Shorthand for partition_desc — a mapping of
                partition column names to values.

        Returns:
            Dict with summary: ``{"status": "ok", "partitions": N, ...}``.
        """
        if partition_desc is not None and partitions is not None:
            raise ValueError("partition_desc and partitions are mutually exclusive")

        configs = self._index_configs("vector")
        if not configs:
            raise ValueError(
                "table has no vector_index_columns configured; nothing to rebuild"
            )
        columns = [column] if column is not None else [c["column"] for c in configs]
        unknown = [c for c in columns if self._vector_config_for(c) is None]
        if unknown:
            raise ValueError(
                f"column(s) {unknown} are not configured for vector indexing"
            )
        selected = (
            [c for c in configs if c["column"] == column]
            if column is not None
            else configs
        )

        store_config = _index.default_object_store_config(
            catalog=self._catalog, table=self
        )

        results = []
        for config in selected:
            if partition_desc is not None:
                result = _index.build_partition_index(
                    "vector",
                    table_name=self.name,
                    namespace=self.namespace,
                    partition_desc=partition_desc,
                    config=config,
                    store_config=store_config,
                    rebuild=True,
                )
            elif partitions is not None:
                result = _index.build_partition_index(
                    "vector",
                    table_name=self.name,
                    namespace=self.namespace,
                    partition_desc=self._partition_desc(partitions),
                    config=config,
                    store_config=store_config,
                    rebuild=True,
                )
            else:
                result = _index.build_table_index(
                    "vector",
                    table_name=self.name,
                    namespace=self.namespace,
                    configs=[config],
                    store_config=store_config,
                    rebuild=True,
                )
            results.append(_index.rename_summary_key(result, "vector_column"))

        return {
            "status": "ok",
            "table_name": self.name,
            "columns": columns,
            "results": results,
        }

    def build_text_index(
        self,
        *,
        column: str | None = None,
        tokenizer: str | None = None,
        with_positions: bool | None = None,
        stored: bool | None = None,
        partition_desc: str | None = None,
        partitions: Mapping[str, str] | None = None,
        rebuild: bool = False,
    ) -> dict[str, Any]:
        """Build or update the Tantivy text index for this table.

        The text column and its tokenizer options are auto-detected from the
        ``text_index_columns`` table property; explicit args override them.

        Args:
            column: Text column name (auto-detected if omitted).
            tokenizer: Tokenizer name (auto-detected if omitted).
            with_positions: Index term positions.
            stored: Store the original text in the index.
            partition_desc: Build index for a single partition, e.g.
                ``"range=2024-01-01"``.
            partitions: Shorthand for partition_desc — a mapping of
                partition column names to values.
            rebuild: When true, rebuild every shard from all of its active
                data files instead of appending a delta split.

        Returns:
            Dict with summary: ``{"status": "ok", "partitions": 2, ...}``.
        """
        if partition_desc is not None and partitions is not None:
            raise ValueError("partition_desc and partitions are mutually exclusive")

        config = _index.config_for_column(
            self._index_configs("text"),
            column,
            overrides={
                "tokenizer": tokenizer,
                "with_positions": with_positions,
                "stored": stored,
            },
        )
        store_config = _index.default_object_store_config(
            catalog=self._catalog, table=self
        )

        if partition_desc is not None:
            return _index.rename_summary_key(
                _index.build_partition_index(
                    "text",
                    table_name=self.name,
                    namespace=self.namespace,
                    partition_desc=partition_desc,
                    config=config,
                    store_config=store_config,
                    rebuild=rebuild,
                ),
                "text_column",
            )

        if partitions is not None:
            return _index.rename_summary_key(
                _index.build_partition_index(
                    "text",
                    table_name=self.name,
                    namespace=self.namespace,
                    partition_desc=self._partition_desc(partitions),
                    config=config,
                    store_config=store_config,
                    rebuild=rebuild,
                ),
                "text_column",
            )

        return _index.rename_summary_key(
            _index.build_table_index(
                "text",
                table_name=self.name,
                namespace=self.namespace,
                configs=[config],
                store_config=store_config,
                rebuild=rebuild,
            ),
            "text_column",
        )

    def _index_configs(self, kind: str) -> list[dict[str, Any]]:
        """Parse a ``{kind}_index_columns`` property into config dicts.

        Uses the kind's native parser as the single source of truth, so
        Python and Rust never disagree on the schema.
        """
        return _index.table_index_configs(self, kind)

    def _vector_configs(self) -> list[VectorIndexConfig]:
        """Parse the ``vector_index_columns`` property into config dicts."""
        return self._index_configs("vector")

    def _text_configs(self) -> list[dict[str, Any]]:
        """Parse the ``text_index_columns`` property into config dicts."""
        return self._index_configs("text")

    def _require_index_writable(self) -> None:
        """Validate the configured indexes against the table before writing.

        ``create_table`` rejects invalid configurations up front; this guards
        pre-existing tables (e.g. created before validation existed, or via
        other engines) so an auto-build failure surfaces *before* data is
        written and committed, instead of post-commit.
        """
        for kind in _index.configured_index_kinds(self.properties):
            if _index.find_index_kind_spec(kind) is None:
                continue
            configs = self._index_configs(kind)
            if configs:
                _index.validate_index_configs(
                    kind, configs, self.schema, self.primary_keys
                )

    # Kept for callers written before the generic entry point.
    _require_vector_index_writable = _require_index_writable

    def _partition_desc(self, partitions: Mapping[str, str]) -> str:
        """Construct the partition descriptor in the table's partition order."""
        part_cols = self.partition_by
        missing = [c for c in part_cols if c not in partitions]
        if missing:
            raise ValueError(f"missing partition columns: {missing}")
        return ",".join(f"{c}={partitions[c]}" for c in part_cols)

    @staticmethod
    def _require_vector_dim(config: Mapping[str, Any]) -> None:
        dim = config.get("dim", 0)
        if dim is None or dim <= 0:
            raise ValueError(f"invalid vector dimension: {dim}")

    def _vector_config_for(self, column: str) -> VectorIndexConfig | None:
        """Return the config dict for ``column``, or ``None`` if not indexed."""
        for cfg in self._vector_configs():
            if cfg["column"] == column:
                return cfg
        return None

    def drop(self, *, if_exists: bool = False) -> None:
        self._catalog.drop_table(self.name, self.namespace, if_exists=if_exists)


class LakeSoulScan:
    """A configured, lazily resolved scan of a LakeSoul table."""

    def __init__(
        self,
        *,
        table: LakeSoulTable,
        partitions: Mapping[str, str],
        columns: tuple[str, ...] | None,
        filter: ds.Expression | None,
        batch_size: int,
        thread_count: int,
        rank: int | None,
        world_size: int | None,
        retain_partition_columns: bool,
        object_store_options: Mapping[str, str],
        _reader_options: Mapping[str, str] | None = None,
        _timestamp: Any = None,
        _time_zone: str | None = None,
    ) -> None:
        _validate_scan_runtime_options(
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
        )
        self._table = table
        self._partitions = dict(partitions)
        self._columns = columns
        self._filter = filter
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._rank = rank
        self._world_size = world_size
        self._retain_partition_columns = retain_partition_columns
        self._object_store_options = dict(object_store_options)
        self._reader_options = dict(_reader_options or {})
        self._timestamp = _timestamp
        self._time_zone = _time_zone

    @property
    def table(self) -> LakeSoulTable:
        return self._table

    @property
    def partitions(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._partitions))

    @property
    def columns(self) -> tuple[str, ...] | None:
        return self._columns

    @property
    def expression(self) -> ds.Expression | None:
        return self._filter

    @property
    def schema(self) -> pa.Schema:
        scan_config = self.to_scan_config()
        if self._columns is None:
            return scan_config.schema
        from lakesoul.arrow.dataset import schema_projection

        return schema_projection(scan_config.schema, list(self._columns))

    def select(self, *columns: str | Sequence[str]) -> LakeSoulScan:
        return self._replace(columns=_normalize_variadic_columns(columns))

    def filter(self, expression: ds.Expression | None) -> LakeSoulScan:
        return self._replace(filter=_validate_filter(expression))

    def with_partitions(
        self,
        partitions: Mapping[str, str] | None = None,
        **overrides: str,
    ) -> LakeSoulScan:
        merged = dict(self._partitions)
        merged.update(_normalize_string_mapping("partitions", partitions or {}))
        merged.update(_normalize_string_mapping("partitions", overrides))
        return self._replace(partitions=merged)

    def shard(self, rank: int, world_size: int) -> LakeSoulScan:
        return self._replace(rank=rank, world_size=world_size)

    def options(
        self,
        *,
        batch_size: int | None = None,
        thread_count: int | None = None,
        retain_partition_columns: bool | None = None,
        object_store_options: Mapping[str, str] | None = None,
        reader_options: Mapping[str, str] | None = None,
        timestamp: Any = None,
        time_zone: str | None = None,
    ) -> LakeSoulScan:
        updates: dict[str, Any] = {}
        if batch_size is not None:
            updates["batch_size"] = batch_size
        if thread_count is not None:
            updates["thread_count"] = thread_count
        if retain_partition_columns is not None:
            updates["retain_partition_columns"] = retain_partition_columns
        if object_store_options is not None:
            updates["object_store_options"] = (
                self._table.catalog._merge_object_store_options(object_store_options)
            )
        if reader_options is not None:
            updates["_reader_options"] = dict(reader_options)
        if timestamp is not None:
            updates["_timestamp"] = timestamp
        if time_zone is not None:
            updates["_time_zone"] = time_zone
        return self._replace(**updates)

    def scan_plan(self) -> tuple[Any, ...]:
        return tuple(
            self._table.catalog._client.get_scan_plan_partitions(
                self._table.name,
                partitions=self._partitions,
                namespace=self._table.namespace,
                as_of_ms=self._as_of_ms(),
            )
        )

    def _as_of_ms(self) -> int | None:
        if self._timestamp is None:
            if self._time_zone is not None:
                raise ValueError("time_zone requires a timestamp")
            return None
        from lakesoul.time_utils import resolve_timestamp_ms

        return resolve_timestamp_ms(self._timestamp, self._time_zone)

    def to_arrow_dataset(self) -> ds.Dataset:
        from lakesoul.arrow import lakesoul_dataset

        return lakesoul_dataset(self.to_scan_config())

    def to_scan_config(self) -> Any:
        return self._scan_config()

    def to_reader(self) -> pa.RecordBatchReader:
        return (
            self.to_arrow_dataset()
            .scanner(
                columns=list(self._columns) if self._columns is not None else None,
                filter=self._filter,
            )
            .to_reader()
        )

    def to_batches(self) -> Any:
        return self.to_arrow_dataset().to_batches(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_arrow_table(self) -> pa.Table:
        return self.to_arrow_dataset().to_table(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_ray(self) -> Any:
        from lakesoul.ray.read_lakesoul import read_lakesoul

        return read_lakesoul(self)

    def to_daft(self) -> Any:
        from lakesoul.daft import read_lakesoul

        return read_lakesoul(self)

    def to_torch(self) -> Any:
        from lakesoul.torch import Dataset

        return Dataset(self)

    def to_huggingface(self) -> Any:
        from lakesoul.huggingface import from_lakesoul

        return from_lakesoul(self)

    def _resolved_reader_options(self) -> dict[str, str]:
        """Merge table index params into reader options at scan time.

        When a vector search is requested (``vector_search_query`` present),
        this auto-fills the search column (from the single indexed column),
        the metric, and the top_k/nprobe defaults from the table's
        ``vector_index_columns`` property, so the Rust reader uses the
        table's metric instead of a hardcoded L2 and the caller doesn't have
        to repeat the column.

        A ``text_search_query`` is handled the same way against
        ``text_index_columns``: the column and the top_k default are filled
        in from the table property.
        """
        reader_options = dict(self._reader_options)
        # CDC tables: the native reader drops delete tombstones after the
        # merge-on-read merge, so forward the configured change column.
        cdc_column = dict(self._table.properties).get("cdc_change_column")
        if cdc_column:
            reader_options.setdefault("cdc_column", str(cdc_column))
        # Blob columns are materialized by the native reader; forward the
        # table's property unless the caller overrides it.
        blob_columns = self._table._blob_columns_option()
        if blob_columns is not None:
            reader_options.setdefault("blob_columns", blob_columns)
        if "vector_search_query" in reader_options:
            self._resolve_vector_search_options(reader_options)
        if "text_search_query" in reader_options:
            self._resolve_text_search_options(reader_options)
        return reader_options

    def _resolve_vector_search_options(self, reader_options: dict[str, str]) -> None:
        configs = self._table._vector_configs()
        if not configs:
            return
        column = reader_options.get("vector_search_column")
        if column is None:
            if len(configs) == 1:
                column = configs[0]["column"]
            else:
                raise ValueError(
                    "multiple vector columns are indexed; "
                    "set 'vector_search_column' explicitly"
                )
            reader_options["vector_search_column"] = column
        cfg = next((c for c in configs if c["column"] == column), None)
        if cfg is not None:
            reader_options.setdefault("vector_search_metric", cfg.get("metric", "L2"))
        reader_options.setdefault("vector_search_top_k", "10")
        reader_options.setdefault("vector_search_nprobe", "64")

    def _resolve_text_search_options(self, reader_options: dict[str, str]) -> None:
        configs = self._table._text_configs()
        if not configs:
            raise ValueError(
                "text search requires a text_index_columns property; "
                "create the table with text_index=[...] to enable it"
            )
        column = reader_options.get("text_search_column")
        if column is None:
            if len(configs) == 1:
                column = configs[0]["column"]
            else:
                raise ValueError(
                    "multiple text columns are indexed; "
                    "set 'text_search_column' explicitly"
                )
            reader_options["text_search_column"] = column
        reader_options.setdefault("text_search_top_k", "10")

    def _scan_config(self) -> Any:
        from lakesoul.arrow import LakeSoulScanConfig

        client = self._table.catalog._client
        schema, partition_schema = client.get_schemas_by_table_name(
            self._table.name,
            namespace=self._table.namespace,
            retain_partition_columns=self._retain_partition_columns,
        )
        reader_options = self._resolved_reader_options()
        if reader_options.get("text_search_scores") == "true":
            schema = _with_text_score_column(schema)
        return LakeSoulScanConfig(
            table_name=self._table.name,
            namespace=self._table.namespace,
            schema=schema,
            partition_schema=partition_schema,
            scan_partitions=self.scan_plan(),
            partitions=dict(self._partitions),
            filter=self._filter,
            object_store_options=dict(self._object_store_options),
            batch_size=self._batch_size,
            thread_count=self._thread_count,
            rank=self._rank,
            world_size=self._world_size,
            reader_options=reader_options,
        )

    def _replace(self, **updates: Any) -> LakeSoulScan:
        values = {
            "table": self._table,
            "partitions": self._partitions,
            "columns": self._columns,
            "filter": self._filter,
            "batch_size": self._batch_size,
            "thread_count": self._thread_count,
            "rank": self._rank,
            "world_size": self._world_size,
            "retain_partition_columns": self._retain_partition_columns,
            "object_store_options": self._object_store_options,
            "_reader_options": self._reader_options,
            "_timestamp": self._timestamp,
            "_time_zone": self._time_zone,
        }
        values.update(updates)
        # A score-requesting scan must read the reserved score column; add it
        # to an explicit projection so it survives schema projection.
        options = values.get("_reader_options") or {}
        columns = values.get("columns")
        if (
            options.get("text_search_scores") == "true"
            and columns is not None
            and TEXT_SEARCH_SCORE_COLUMN not in columns
        ):
            values["columns"] = (*columns, TEXT_SEARCH_SCORE_COLUMN)
        return LakeSoulScan(**values)


def _with_text_score_column(schema: pa.Schema) -> pa.Schema:
    """Append the reserved BM25 score column to a scan schema."""
    if TEXT_SEARCH_SCORE_COLUMN in schema.names:
        return schema
    return schema.append(pa.field(TEXT_SEARCH_SCORE_COLUMN, pa.float32(), True))


def _validate_string_mapping(name: str, value: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    result = dict(value)
    if any(not isinstance(k, str) or not isinstance(v, str) for k, v in result.items()):
        raise TypeError(f"{name} must contain only string keys and values")
    return result


def _normalize_string_mapping(name: str, value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    result = {}
    for k, v in value.items():
        if not isinstance(k, str):
            raise TypeError(f"{name} keys must be strings")
        result[k] = str(v)
    return result


def _normalize_optional_columns(
    columns: Sequence[str] | str | None,
) -> tuple[str, ...] | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        values = (columns,)
    elif isinstance(columns, Sequence):
        values = tuple(columns)
    else:
        raise TypeError("columns must be a string or a sequence of strings")
    if any(not isinstance(column, str) or not column for column in values):
        raise TypeError("columns must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError("columns must not contain duplicate values")
    return values


def _normalize_variadic_columns(
    columns: tuple[str | Sequence[str], ...],
) -> tuple[str, ...] | None:
    if len(columns) == 1 and not isinstance(columns[0], str):
        return _normalize_optional_columns(columns[0])
    return _normalize_optional_columns(columns)  # ty: ignore


def _validate_filter(expression: ds.Expression | None) -> ds.Expression | None:
    if expression is not None and not isinstance(expression, ds.Expression):
        raise TypeError("filter must be a pyarrow.dataset.Expression")
    return expression


def _validate_scan_runtime_options(
    *,
    batch_size: int,
    thread_count: int,
    rank: int | None,
    world_size: int | None,
) -> None:
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size <= 0
    ):
        raise ValueError(
            f"batch_size must be a positive integer; {batch_size} is invalid"
        )
    if (
        isinstance(thread_count, bool)
        or not isinstance(thread_count, int)
        or thread_count < 0
    ):
        raise ValueError(
            f"thread_count must be a non-negative integer; {thread_count} is invalid"
        )
    if rank is None and world_size is None:
        return
    if rank is None or world_size is None:
        raise ValueError("rank and world_size must be both set or both unset")
    if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
        raise ValueError(f"rank must be a non-negative integer; {rank} is invalid")
    if (
        isinstance(world_size, bool)
        or not isinstance(world_size, int)
        or world_size <= 0
    ):
        raise ValueError(
            f"world_size must be a positive integer; {world_size} is invalid"
        )
    if rank >= world_size:
        raise ValueError(f"rank {rank} is out of range; world_size = {world_size}")


def _case_fold_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError("identifier must be a non-empty string")
    return value.translate(_ASCII_LOWER_TRANS)


def _case_fold_columns(name: str, columns: Sequence[str]) -> tuple[str, ...]:
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence):
        raise TypeError(f"{name} must be a sequence of strings")
    values = tuple(_case_fold_identifier(column) for column in columns)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicate columns")
    return values


def _case_fold_schema(schema: pa.Schema) -> pa.Schema:
    fields = []
    names = []
    for field in schema:
        name = _case_fold_identifier(field.name)
        fields.append(
            pa.field(
                name,
                field.type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
        )
        names.append(name)
    if len(names) != len(set(names)):
        raise ValueError("schema must not contain duplicate columns after case folding")
    return pa.schema(fields, metadata=schema.metadata)


def _make_primary_keys_non_nullable(
    schema: pa.Schema,
    primary_keys: Sequence[str],
) -> pa.Schema:
    primary_key_set = set(primary_keys)
    if not primary_key_set:
        return schema
    fields = [
        pa.field(
            field.name,
            field.type,
            nullable=False if field.name in primary_key_set else field.nullable,
            metadata=field.metadata,
        )
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _validate_hash_bucket_num(hash_bucket_num: int) -> None:
    if (
        isinstance(hash_bucket_num, bool)
        or not isinstance(hash_bucket_num, int)
        or hash_bucket_num <= 0
    ):
        raise ValueError("hash_bucket_num must be greater than zero")


def _validate_columns(name: str, columns: Sequence[str], schema: pa.Schema) -> None:
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence):
        raise TypeError(f"{name} must be a sequence of strings")
    schema_names = set(schema.names)
    values = tuple(columns)
    if any(not isinstance(column, str) or not column for column in values):
        raise TypeError(f"{name} must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicate columns")
    for column in values:
        if column not in schema_names:
            raise ValueError(f"{name} column not in schema: {column}")


_BLOB_MODES = {"auto", "inline", "external"}


def _validate_blob_columns_property(raw: Any, schema: pa.Schema) -> None:
    """Validate the ``blob_columns`` table property against the schema."""
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError("blob_columns property is not valid JSON") from error
    else:
        parsed = raw
    if not isinstance(parsed, dict):
        raise TypeError("blob_columns property must be a JSON object")
    schema_names = set(schema.names)
    for column, policy in parsed.items():
        if column not in schema_names:
            raise ValueError(
                f"blob_columns references a column not in the schema: {column!r}"
            )
        if not isinstance(policy, dict):
            raise TypeError(f"blob policy for {column!r} must be a JSON object")
        mode = policy.get("mode", "auto")
        if mode not in _BLOB_MODES:
            raise ValueError(
                f"blob mode for {column!r} must be auto|inline|external, got {mode!r}"
            )
        for key in ("inline_threshold", "pack_target_bytes"):
            value = policy.get(key)
            if value is not None and (not isinstance(value, int) or value < 0):
                raise ValueError(
                    f"blob {key} for {column!r} must be a non-negative integer"
                )


def _parse_table_properties(properties: str) -> dict[str, Any]:
    if not properties:
        return {}
    try:
        value = json.loads(properties)
    except json.JSONDecodeError as error:
        raise ValueError("LakeSoul table properties are not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError("LakeSoul table properties must be a JSON object")
    return value


def _default_object_store_config(
    catalog: LakeSoulCatalog,
    table: LakeSoulTable,
) -> dict:
    """Build store_config dict for an index build from table/catalog info."""
    return _index.default_object_store_config(catalog=catalog, table=table)


def _normalize_vector_index(value: Any) -> list[dict[str, Any]]:
    """Normalize a ``vector_index`` argument into a list of config dicts."""
    return _index.normalize_index_configs("vector", value)


def _parse_vector_index_configs(value: str) -> list[VectorIndexConfig]:
    """Parse a ``vector_index_columns`` property value via the Rust parser."""
    return _index.parse_index_configs("vector", value)


def _validate_vector_index_configs(
    configs: Sequence[Mapping[str, Any]],
    schema: pa.Schema,
    primary_keys: Sequence[str],
) -> None:
    """Validate a table can support the configured vector index.

    Mirrors the native builder's requirements (``extract_vector_batch``): an
    Int64/UInt64 primary key column and Float32 vector columns whose
    ``FixedSizeList`` size matches the configured ``dim``.  Raises
    ``ValueError`` so callers fail *before* creating metadata or committing
    data, rather than in a post-commit auto-build step.
    """
    if not configs:
        return
    if not primary_keys:
        raise ValueError(
            "a vector index requires an id column: pass primary_keys=[...] "
            "when creating a table with vector_index (the index maps "
            "search results to primary key values)"
        )
    pk_column = primary_keys[0]
    pk_index = schema.get_field_index(pk_column)
    if pk_index < 0:
        raise ValueError(
            f"vector index primary key '{pk_column}' not found in table schema "
            f"(columns: {list(schema.names)})"
        )
    pk_type = schema.field(pk_index).type
    if not (pa.types.is_uint64(pk_type) or pa.types.is_int64(pk_type)):
        raise ValueError(
            f"vector index primary key '{pk_column}' must be UInt64 or Int64, "
            f"got {pk_type}"
        )
    for cfg in configs:
        column = cfg["column"]
        index = schema.get_field_index(column)
        if index < 0:
            raise ValueError(
                f"vector index column '{column}' not found in table schema "
                f"(columns: {list(schema.names)})"
            )
        dtype = schema.field(index).type
        if pa.types.is_fixed_size_list(dtype):
            element_type = dtype.value_type
            list_size: int | None = dtype.list_size
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
            element_type = dtype.value_type
            list_size = None
        else:
            raise ValueError(
                f"vector index column '{column}' must be FixedSizeList<Float32> "
                f"or List<Float32>, got {dtype}"
            )
        if not pa.types.is_float32(element_type):
            raise ValueError(
                f"vector index column '{column}' must hold Float32 values, "
                f"got {element_type}"
            )
        if list_size is not None and list_size != cfg["dim"]:
            raise ValueError(
                f"vector index column '{column}': configured dim {cfg['dim']} "
                f"does not match schema FixedSizeList size {list_size}"
            )


__all__ = [
    "LakeSoulCatalog",
    "LakeSoulScan",
    "LakeSoulTable",
    "PostgresMetadataConfig",
    "TableWriteConfig",
]
