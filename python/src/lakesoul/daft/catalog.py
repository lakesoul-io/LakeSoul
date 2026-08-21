# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Daft Catalog adapters backed by the LakeSoul Python catalog."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypedDict, cast

from daft.catalog import (
    Catalog,
    Function,
    Identifier,
    NotFoundError,
    Properties,
    Schema,
    Table,
)

from lakesoul.catalog import LakeSoulCatalog, LakeSoulTable
from lakesoul.exceptions import TableNotFoundError

if TYPE_CHECKING:
    from daft.io.partitioning import PartitionField


class LakeSoulCreateTableProperties(TypedDict, total=False):
    """LakeSoul-specific options accepted by Daft ``create_table``."""

    location: str
    primary_keys: Sequence[str]
    hash_bucket_num: int
    domain: str
    table_properties: Mapping[str, str]


_CREATE_TABLE_PROPERTY_NAMES = frozenset(
    {
        "location",
        "primary_keys",
        "hash_bucket_num",
        "domain",
        "table_properties",
    }
)


class LakeSoulDataTable(Table):
    """Expose one :class:`LakeSoulTable` through Daft's Table interface."""

    def __init__(self, table: LakeSoulTable) -> None:
        self._table = table

    @property
    def lakesoul_table(self) -> LakeSoulTable:
        """Return the underlying LakeSoul table."""
        return self._table

    @property
    def name(self) -> str:
        return self._table.name

    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    def read(self, **options: Any) -> Any:
        """Build a lazy Daft DataFrame using the existing native source."""
        return self._table.scan(**options).to_daft()

    def append(self, df: Any, **options: Any) -> None:
        """Append a Daft DataFrame using the existing native sink."""
        self._table.write_daft(df, **options)

    def overwrite(self, df: Any, **options: Any) -> None:
        del df, options
        raise NotImplementedError("LakeSoul Daft tables do not support overwrite yet")


class LakeSoulDataCatalog(Catalog):
    """Expose :class:`LakeSoulCatalog` through Daft's Catalog interface."""

    def __init__(
        self,
        catalog: LakeSoulCatalog,
        *,
        name: str = "lakesoul",
    ) -> None:
        if not name:
            raise ValueError("catalog name must not be empty")
        self._catalog = catalog
        self._name = name

    @classmethod
    def from_env(
        cls,
        *,
        name: str = "lakesoul",
        namespace: str = "default",
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulDataCatalog:
        """Create a Daft catalog backed by LakeSoul environment settings."""
        return cls(
            LakeSoulCatalog.from_env(
                namespace=namespace,
                object_store_options=object_store_options,
            ),
            name=name,
        )

    @property
    def lakesoul_catalog(self) -> LakeSoulCatalog:
        """Return the underlying LakeSoul catalog."""
        return self._catalog

    @property
    def name(self) -> str:
        return self._name

    def _get_table(self, ident: Identifier) -> LakeSoulDataTable:
        namespace, table_name = self._resolve_table_identifier(ident)
        try:
            table = self._catalog.table(table_name, namespace)
        except TableNotFoundError as error:
            raise NotFoundError(f"LakeSoul table {ident} was not found") from error
        return LakeSoulDataTable(table)

    def _has_table(self, ident: Identifier) -> bool:
        try:
            self._get_table(ident)
        except NotFoundError:
            return False
        return True

    def _list_tables(self, pattern: str | None = None) -> list[Identifier]:
        _reject_pattern(pattern)
        return [
            Identifier(*namespace.split("."), table_name)
            for namespace in self._catalog.list_namespaces()
            for table_name in self._catalog.list_tables(namespace)
        ]

    def _has_namespace(self, ident: Identifier) -> bool:
        namespace = self._namespace_identifier(ident)
        return namespace in self._catalog.list_namespaces()

    def _list_namespaces(self, pattern: str | None = None) -> list[Identifier]:
        _reject_pattern(pattern)
        return [
            Identifier.from_str(namespace)
            for namespace in self._catalog.list_namespaces()
        ]

    def _create_table(
        self,
        ident: Identifier,
        schema: Schema,
        properties: Properties | None = None,
        partition_fields: list[PartitionField] | None = None,
    ) -> LakeSoulDataTable:
        namespace, table_name = self._resolve_table_identifier(ident)
        options = _validate_create_table_properties(properties)
        location = options.get("location")
        if location is None:
            raise ValueError(
                "creating a LakeSoul table requires properties['location']"
            )

        partition_by = _identity_partition_columns(partition_fields)
        table = self._catalog.create_table(
            table_name,
            namespace=namespace,
            path=location,
            schema=schema.to_pyarrow_schema(),
            partition_by=partition_by,
            primary_keys=options.get("primary_keys", ()),
            hash_bucket_num=options.get("hash_bucket_num"),
            properties=options.get("table_properties"),
            domain=options.get("domain", "public"),
        )
        return LakeSoulDataTable(table)

    def _drop_table(self, ident: Identifier) -> None:
        namespace, table_name = self._resolve_table_identifier(ident)
        try:
            self._catalog.drop_table(table_name, namespace)
        except TableNotFoundError as error:
            raise NotFoundError(f"LakeSoul table {ident} was not found") from error

    def _create_namespace(self, ident: Identifier) -> None:
        self._namespace_identifier(ident)
        raise NotImplementedError(
            "LakeSoul's Python metadata client does not expose namespace creation"
        )

    def _drop_namespace(self, ident: Identifier) -> None:
        self._namespace_identifier(ident)
        raise NotImplementedError(
            "LakeSoul's Python metadata client does not expose namespace deletion"
        )

    def _create_function(
        self,
        ident: Identifier,
        function: Function | Callable[..., Any],
    ) -> None:
        del ident, function
        raise NotImplementedError("LakeSoul does not support catalog functions")

    def _get_function(self, ident: Identifier) -> Function:
        raise NotFoundError(f"LakeSoul catalog function {ident} was not found")

    def _resolve_table_identifier(self, ident: Identifier) -> tuple[str, str]:
        """Resolve a catalog-relative Daft identifier to LakeSoul names.

        Daft Session removes the catalog qualifier before calling this catalog.
        Therefore ``ident`` is either ``table`` or ``namespace[.child].table``;
        it never contains this catalog's name.
        """
        if len(ident) == 1:
            return self._catalog.namespace, ident[0]
        return ".".join(ident[:-1]), ident[-1]

    @staticmethod
    def _namespace_identifier(ident: Identifier) -> str:
        return ".".join(ident)


def _reject_pattern(pattern: str | None) -> None:
    if pattern is not None:
        raise NotImplementedError(
            "LakeSoul's metadata list API does not support pattern filtering"
        )


def _validate_create_table_properties(
    properties: Properties | None,
) -> LakeSoulCreateTableProperties:
    options = dict(properties or {})
    unknown = options.keys() - _CREATE_TABLE_PROPERTY_NAMES
    if unknown:
        raise ValueError(
            f"unsupported LakeSoul create-table properties: {sorted(unknown)}"
        )

    location = options.get("location")
    if location is not None and (not isinstance(location, str) or not location.strip()):
        raise TypeError("location must be a non-empty string")

    primary_keys = options.get("primary_keys")
    if primary_keys is not None:
        if isinstance(primary_keys, (str, bytes)) or not isinstance(
            primary_keys, Sequence
        ):
            raise TypeError("primary_keys must be a sequence of strings")
        if not all(isinstance(key, str) and key for key in primary_keys):
            raise TypeError("primary_keys must contain non-empty strings")
        options["primary_keys"] = tuple(primary_keys)

    hash_bucket_num = options.get("hash_bucket_num")
    if hash_bucket_num is not None and (
        isinstance(hash_bucket_num, bool)
        or not isinstance(hash_bucket_num, int)
        or hash_bucket_num <= 0
    ):
        raise ValueError("hash_bucket_num must be a positive integer")

    domain = options.get("domain")
    if domain is not None and (not isinstance(domain, str) or not domain):
        raise TypeError("domain must be a non-empty string")

    table_properties = options.get("table_properties")
    if table_properties is not None:
        if not isinstance(table_properties, Mapping):
            raise TypeError("table_properties must be a mapping of strings")
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in table_properties.items()
        ):
            raise TypeError("table_properties must be a mapping of strings")
        options["table_properties"] = dict(table_properties)

    return cast(LakeSoulCreateTableProperties, options)


def _identity_partition_columns(
    partition_fields: list[PartitionField] | None,
) -> tuple[str, ...]:
    columns = []
    for partition_field in partition_fields or ():
        transform = partition_field.transform
        if transform is not None and not transform.is_identity():
            raise ValueError("LakeSoul only supports identity partition fields")
        source_field = partition_field.source_field or partition_field.field
        column = source_field.name
        if column in columns:
            raise ValueError(f"duplicate partition field: {column}")
        columns.append(column)
    return tuple(columns)
