# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

from collections.abc import Callable, Sequence

import pyarrow as pa
import pyarrow.compute as pc


ArrowData = pa.RecordBatch | pa.Table | pa.RecordBatchReader


def validate_write_schema(
    actual: pa.Schema,
    expected: pa.Schema,
    *,
    primary_keys: Sequence[str] = (),
    partition_by: Sequence[str] = (),
    source: str = "input",
    compatible_type: Callable[[pa.DataType, pa.DataType], bool] | None = None,
) -> pa.Schema:
    """Resolve a physical contribution schema against the stored write schema."""
    prefix = (
        f"{source} schema does not match LakeSoul table schema:\n"
        f"expected: {expected}\nactual: {actual}"
    )
    names = set(actual.names)
    if len(names) != len(actual.names):
        raise ValueError(f"{prefix}\nduplicate input columns are not supported")
    unknown = [name for name in actual.names if name not in expected.names]
    if unknown:
        raise ValueError(f"{prefix}\nunknown columns: {unknown}")

    required = set(primary_keys) | set(partition_by)
    required.update(field.name for field in expected if _has_required_field(field))
    if not primary_keys:
        # Append-only tables still write complete rows, not MOR contributions.
        required.update(expected.names)
    missing = [field.name for field in expected if field.name in required - names]
    if missing:
        raise ValueError(f"{prefix}\nmissing required columns: {missing}")

    fields = []
    for field in expected:
        if field.name not in names:
            continue
        actual_type = actual.field(field.name).type
        contribution_type = _with_input_nullability(actual_type, field.type)
        if actual_type != contribution_type and not (
            compatible_type is not None
            and compatible_type(actual_type, contribution_type)
        ):
            raise ValueError(
                f"{prefix}\nincompatible column {field.name!r}: "
                f"{actual_type} cannot be written as {field.type}"
            )
        fields.append(field.with_type(contribution_type))
    return pa.schema(fields, metadata=expected.metadata)


def _has_required_field(field: pa.Field) -> bool:
    return not field.nullable or (
        pa.types.is_struct(field.type)
        and any(_has_required_field(child) for child in field.type)
    )


def _with_input_nullability(actual: pa.DataType, expected: pa.DataType) -> pa.DataType:
    """Keep nested source nullable flags; constraints are checked separately."""
    if pa.types.is_struct(actual) and pa.types.is_struct(expected):
        if actual.names == expected.names:
            return pa.struct(
                [
                    field.with_nullable(source.nullable).with_type(
                        _with_input_nullability(source.type, field.type)
                    )
                    for source, field in zip(actual, expected)
                ]
            )
    list_types = (pa.types.is_list, pa.types.is_large_list, pa.types.is_fixed_size_list)
    if any(check(actual) for check in list_types) and any(
        check(expected) for check in list_types
    ):
        field = expected.value_field.with_nullable(
            actual.value_field.nullable
        ).with_type(_with_input_nullability(actual.value_type, expected.value_type))
        if pa.types.is_fixed_size_list(expected):
            return pa.list_(field, expected.list_size)
        return (
            pa.large_list(field)
            if pa.types.is_large_list(expected)
            else pa.list_(field)
        )
    if pa.types.is_map(actual) and pa.types.is_map(expected):
        return pa.map_(
            expected.key_field.with_type(
                _with_input_nullability(actual.key_type, expected.key_type)
            ),
            expected.item_field.with_nullable(actual.item_field.nullable).with_type(
                _with_input_nullability(actual.item_type, expected.item_type)
            ),
            keys_sorted=expected.keys_sorted,
        )
    return expected


def validate_non_nullable(
    data: pa.RecordBatch | pa.Table,
    schema: pa.Schema,
    *,
    check_null_ancestors: bool = True,
) -> None:
    """Check stored constraints, or physical nullability for configured IO."""

    def validate(column, field: pa.Field, path: str, parent_valid=None) -> None:
        if not field.nullable and column.null_count:
            visible_null = (
                parent_valid is None
                or pc.any(pc.and_(pc.is_null(column), parent_valid)).as_py()
            )
            if visible_null:
                raise ValueError(
                    "input contains null values for a non-nullable "
                    f"LakeSoul column: {path!r}"
                )
        if pa.types.is_struct(field.type):
            child_parent_valid = (
                pc.is_valid(column)
                if not check_null_ancestors and column.null_count
                else None
            )
            for child in field.type:
                if _has_required_field(child):
                    validate(
                        pc.struct_field(column, child.name),
                        child,
                        f"{path}.{child.name}",
                        child_parent_valid,
                    )

    for field in schema:
        if field.name in data.schema.names and _has_required_field(field):
            validate(data.column(field.name), field, field.name)


def normalize_write_data(
    data: ArrowData,
    schema: pa.Schema,
    *,
    constraints: pa.Schema,
) -> ArrowData:
    """Use stored field metadata without padding omitted nullable columns."""
    if isinstance(data, pa.RecordBatchReader):
        return pa.RecordBatchReader.from_batches(
            schema,
            (
                normalize_write_data(batch, schema, constraints=constraints)
                for batch in data
            ),
        )

    validate_non_nullable(data, constraints)
    arrays = [
        data.column(field.name)
        if data.schema.field(field.name).type == field.type
        else pc.cast(data.column(field.name), field.type, safe=True)
        for field in schema
    ]
    if isinstance(data, pa.Table):
        return pa.Table.from_arrays(arrays, schema=schema)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
