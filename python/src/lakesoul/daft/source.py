# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from daft.datatype import DataType
from daft.expressions import Expression, ExpressionsProjection
from daft.expressions.pyarrow_visitor import _PyArrowExpressionVisitor
from daft.io.partitioning import PartitionField, PartitionTransform
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Field, Schema

from lakesoul.arrow import LakeSoulScanConfig, lakesoul_dataset
from lakesoul.arrow.dataset import schema_projection
from lakesoul.metadata import LakeSoulScanPlanPartition


class LakeSoulDataSource(DataSource):
    """A lazy Daft data source backed by the LakeSoul native reader."""

    def __init__(
        self,
        scan_config: LakeSoulScanConfig,
        *,
        columns: tuple[str, ...] | None,
        partition_columns: Sequence[str],
    ) -> None:
        self._scan_config = scan_config
        self._arrow_schema = _project_schema(scan_config.schema, columns)
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)
        self._partition_arrow_fields = {
            name: self._arrow_schema.field(name)
            for name in partition_columns
            if name in self._arrow_schema.names
        }
        self._partition_fields = [
            _identity_partition_field(field)
            for field in self._partition_arrow_fields.values()
        ]

    @property
    def name(self) -> str:
        return (
            f"LakeSoulDataSource({self._scan_config.namespace}."
            f"{self._scan_config.table_name})"
        )

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        return list(self._partition_fields)

    async def get_tasks(
        self,
        pushdowns: Pushdowns,
    ) -> AsyncIterator[DataSourceTask]:
        columns = (
            tuple(pushdowns.columns)
            if pushdowns.columns is not None
            else tuple(self._arrow_schema.names)
        )
        filter_columns = pushdowns.filter_required_column_names()
        read_columns = tuple(
            name
            for name in self._arrow_schema.names
            if name in columns or name in filter_columns
        )
        task_schema = _project_schema(self._arrow_schema, read_columns)
        daft_filter = None
        try:
            daft_filter = _to_arrow_filter(
                pushdowns.filters,
                schema=self._scan_config.schema,
            )
        except ValueError:
            # Daft keeps its filter above the source. An expression that Arrow
            # cannot represent, such as a Python UDF, must be evaluated there.
            pass
        filter = _combine_filters(
            self._scan_config.filter,
            daft_filter,
        )

        rank = self._scan_config.rank
        world_size = self._scan_config.world_size
        for index, scan_partition in enumerate(self._scan_config.scan_partitions):
            # LakeSoul shards the complete scan plan by scan-partition index.
            # Apply that rule before Daft turns each retained partition into a
            # separate task; the task itself must not shard its one partition
            # for a second time.
            if (
                rank is not None
                and world_size is not None
                and index % world_size != rank
            ):
                continue
            if not scan_partition.files:
                continue
            if not _partition_matches(
                scan_partition,
                self._partition_arrow_fields,
                pushdowns.partition_filters,
            ):
                continue

            partition_config = replace(
                self._scan_config,
                scan_partitions=(scan_partition,),
                # Sharding has already been applied to the complete scan plan.
                # Use an explicit one-rank scan so the Arrow dataset cannot
                # infer torch.distributed rank/world_size and shard this task
                # a second time.
                rank=0,
                world_size=1,
            )
            yield LakeSoulDataSourceTask(
                partition_config,
                arrow_schema=task_schema,
                columns=read_columns,
                filter=filter,
            )


class LakeSoulDataSourceTask(DataSourceTask):
    """Read one LakeSoul partition/bucket, including native PK merge-read."""

    def __init__(
        self,
        scan_config: LakeSoulScanConfig,
        *,
        arrow_schema: pa.Schema,
        columns: tuple[str, ...],
        filter: ds.Expression | None,
    ) -> None:
        self._scan_config = scan_config
        self._arrow_schema = arrow_schema
        self._schema = Schema.from_pyarrow_schema(arrow_schema)
        self._columns = columns
        self._filter = filter

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        arrow_dataset = lakesoul_dataset(self._scan_config)
        for batch in arrow_dataset.to_batches(
            columns=list(self._columns),
            filter=self._filter,
        ):
            if batch.num_rows:
                yield RecordBatch.from_arrow_record_batches(
                    [batch],
                    self._arrow_schema,
                )


def _project_schema(
    schema: pa.Schema,
    columns: Sequence[str] | None,
) -> pa.Schema:
    if columns is None:
        return schema
    return schema_projection(schema, list(columns))


def _identity_partition_field(field: pa.Field) -> PartitionField:
    daft_field = Field.create(field.name, DataType.from_arrow_type(field.type))
    return PartitionField.create(
        field=daft_field,
        source_field=daft_field,
        transform=PartitionTransform.identity(),
    )


def _to_arrow_filter(
    expression: Expression | None,
    *,
    schema: pa.Schema,
) -> ds.Expression | None:
    if expression is None:
        return None
    try:
        return _SchemaAwarePyArrowExpressionVisitor(schema).visit(expression)
    except Exception as error:
        raise ValueError(
            f"Daft filter cannot be pushed down to the LakeSoul reader: {error}"
        ) from error


class _SchemaAwarePyArrowExpressionVisitor(_PyArrowExpressionVisitor):
    """Convert Daft filters while matching numeric literals to Decimal columns."""

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def visit_equal(self, left: Expression, right: Expression) -> ds.Expression:
        return self._visit_comparison(left, right, lambda lhs, rhs: lhs == rhs)

    def visit_not_equal(
        self,
        left: Expression,
        right: Expression,
    ) -> ds.Expression:
        return self._visit_comparison(left, right, lambda lhs, rhs: lhs != rhs)

    def visit_less_than(
        self,
        left: Expression,
        right: Expression,
    ) -> ds.Expression:
        return self._visit_comparison(left, right, lambda lhs, rhs: lhs < rhs)

    def visit_less_than_or_equal(
        self,
        left: Expression,
        right: Expression,
    ) -> ds.Expression:
        return self._visit_comparison(left, right, lambda lhs, rhs: lhs <= rhs)

    def visit_greater_than(
        self,
        left: Expression,
        right: Expression,
    ) -> ds.Expression:
        return self._visit_comparison(left, right, lambda lhs, rhs: lhs > rhs)

    def visit_greater_than_or_equal(
        self,
        left: Expression,
        right: Expression,
    ) -> ds.Expression:
        return self._visit_comparison(left, right, lambda lhs, rhs: lhs >= rhs)

    def visit_between(
        self,
        expression: Expression,
        lower: Expression,
        upper: Expression,
    ) -> ds.Expression:
        field = self._decimal_field_for_column(expression)
        if field is None:
            return super().visit_between(expression, lower, upper)

        arrow_expression = self.visit(expression)
        arrow_lower = self._visit_decimal_operand(field, lower)
        arrow_upper = self._visit_decimal_operand(field, upper)
        return (arrow_lower <= arrow_expression) & (arrow_expression <= arrow_upper)

    def visit_is_in(
        self,
        expression: Expression,
        items: list[Expression],
    ) -> ds.Expression:
        field = self._decimal_field_for_column(expression)
        if field is None:
            return super().visit_is_in(expression, items)

        values: list[Decimal | None] = []
        for item in items:
            if not item.is_literal():
                return super().visit_is_in(expression, items)
            value = item.as_py()
            if value is None:
                values.append(None)
                continue
            decimal_value = self._decimal_value(value)
            if decimal_value is None:
                return super().visit_is_in(expression, items)
            values.append(decimal_value)

        value_set = pa.array(values, type=field.type)
        return self.visit(expression).isin(value_set)

    def _visit_comparison(
        self,
        left: Expression,
        right: Expression,
        operator: Callable[[Any, Any], ds.Expression],
    ) -> ds.Expression:
        operands = self._decimal_comparison_operands(left, right)
        if operands is None:
            operands = self.visit(left), self.visit(right)
        return operator(*operands)

    def _decimal_comparison_operands(
        self,
        left: Expression,
        right: Expression,
    ) -> tuple[ds.Expression, ds.Expression] | None:
        if left.is_column() and right.is_literal():
            literal = self._decimal_literal_for_column(left, right)
            if literal is not None:
                return self.visit(left), literal
        if left.is_literal() and right.is_column():
            literal = self._decimal_literal_for_column(right, left)
            if literal is not None:
                return literal, self.visit(right)
        return None

    def _decimal_literal_for_column(
        self,
        column: Expression,
        literal: Expression,
    ) -> ds.Expression | None:
        field = self._decimal_field_for_column(column)
        if field is None:
            return None

        decimal_value = self._decimal_value(literal.as_py())
        if decimal_value is None:
            return None
        scalar = pa.scalar(decimal_value, type=field.type)
        return pc.scalar(scalar)

    def _decimal_field_for_column(
        self,
        expression: Expression,
    ) -> pa.Field | None:
        if not expression.is_column():
            return None
        column_name = expression.column_name()
        if column_name is None or column_name not in self._schema.names:
            return None
        field = self._schema.field(column_name)
        return field if pa.types.is_decimal(field.type) else None

    def _visit_decimal_operand(
        self,
        field: pa.Field,
        operand: Expression,
    ) -> ds.Expression:
        if operand.is_literal():
            decimal_value = self._decimal_value(operand.as_py())
            if decimal_value is not None:
                return pc.scalar(pa.scalar(decimal_value, type=field.type))
        return self.visit(operand)

    @staticmethod
    def _decimal_value(value: object) -> Decimal | None:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            return None
        return value if isinstance(value, Decimal) else Decimal(str(value))


def _combine_filters(
    left: ds.Expression | None,
    right: ds.Expression | None,
) -> ds.Expression | None:
    if left is None:
        return right
    if right is None:
        return left
    return left & right


def _partition_matches(
    scan_partition: LakeSoulScanPlanPartition,
    partition_fields: dict[str, pa.Field],
    filter: object | None,
) -> bool:
    if filter is None:
        return True

    raw_values = dict(scan_partition.partition_info)
    values = {}
    for name, field in partition_fields.items():
        if name not in raw_values:
            raise ValueError(f"LakeSoul partition metadata is missing column {name!r}")
        array = pa.array([raw_values[name]], type=pa.string())
        values[name] = pc.cast(array, field.type, safe=True)

    partition_batch = RecordBatch.from_arrow_table(pa.table(values))
    filtered = partition_batch.filter(ExpressionsProjection([filter]))
    return len(filtered) > 0
