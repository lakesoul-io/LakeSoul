# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import logging
import random
from e2e.operators import PythonOperator
from dataclasses import dataclass
from typing import List, Optional, Any

from enum import Enum

from faker import Faker
from e2e.utils import random_str
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

fake = Faker()


# type Exp = Var | Ap | Fn | Let


# @dataclass
# class Var:
#     name: str


# @dataclass
# class Ap:
#     target: Exp
#     arg: Exp


# @dataclass
# class Fn:
#     name: str
#     ret: Exp


# @dataclass
# class Binding:
#     name: str
#     exp: Exp


# @dataclass
# class Let:
#     bindings: list[Binding]
#     body: Exp


# def format_exp(exp: Exp) -> str:
#     match exp:
#         case Var(name):
#             return name
#         case Ap(target, arg):
#             return f"({format_exp(target)} {format_exp(arg)})"
#         case Fn(name, ret):
#             return f"(lambda ({name}) {format_exp(ret)})"
#         case Let(bindings, body):
#             bindings_text = " ".join(map(format_binding, bindings))
#             return f"(let ({bindings_text}) {format_exp(body)})"


# def format_binding(binding: Binding) -> str:
#     return f"[{binding.name} {format_exp(binding.exp)}]"


# print(format_exp(Var("x")))
# print(format_exp(Ap(Var("f"), Var("x"))))
# print(format_exp(Fn("x", Ap(Var("f"), Var("x")))))
# print(
#     format_exp(
#         Let(
#             [
#                 Binding("x", Ap(Var("f"), Var("x"))),
#                 Binding("y", Ap(Var("f"), Var("x"))),
#             ],
#             Ap(Var("x"), Var("y")),
#         )
#     )
# )


class DataType(Enum):
    Int32 = 0
    Int64 = 1
    Float32 = 2
    Float64 = 3
    Bool = 4
    String = 5
    Binary = 6
    Date32 = 7
    Date64 = 8
    Timestamp = 9
    Decimal128 = 10
    List = 11
    Struct = 12
    Map = 13

    def to_arrow(self, value_type=None):
        mapping = {
            DataType.Int32: pa.int32(),
            DataType.Int64: pa.int64(),
            DataType.Float32: pa.float32(),
            DataType.Float64: pa.float64(),
            DataType.Bool: pa.bool_(),
            DataType.String: pa.string(),
            DataType.Binary: pa.binary(),
            DataType.Date32: pa.date32(),
            DataType.Date64: pa.date64(),
            DataType.Timestamp: pa.timestamp("ms"),
            DataType.Decimal128: pa.decimal128(18, 6),
        }
        if self in mapping:
            return mapping[self]
        elif self == DataType.List:
            assert value_type is not None
            return pa.list_(
                value_type
                if isinstance(value_type, pa.DataType)
                else value_type.to_arrow()
            )
        elif self == DataType.Struct:
            assert value_type is not None
            return pa.struct([f.to_arrow() for f in value_type])
        elif self == DataType.Map:
            assert value_type is not None and isinstance(value_type, tuple)
            key_type, item_type = value_type
            return pa.map_(key_type.to_arrow(), item_type.to_arrow())
        else:
            raise NotImplementedError(f"不支持的DataType: {self}")

    def as_spark_sql(self, null_prob) -> str:
        mapping = {
            DataType.Int32: "INT",
            DataType.Int64: "LONG",
            DataType.Float32: "FLOAT",
            DataType.Float64: "DOUBLE",
            DataType.Bool: "BOOLEAN",
            DataType.String: "STRING",
            DataType.Binary: "BINARY",
            DataType.Date32: "DATE",
        }
        if self in mapping:
            return f"{mapping[self]} {'' if null_prob else 'NOT NULL'}"
        else:
            raise RuntimeError("Not Supported Data Type")


@dataclass
class Field:
    name: str
    data_type: DataType
    null_prob: Optional[float] = None
    value_type: Optional[Any] = None  # List/Struct/Map 才用

    def to_arrow(self):
        nullable = self.null_prob is not None and self.null_prob > 0
        if self.data_type in [DataType.List, DataType.Struct, DataType.Map]:
            arrow_type = self.data_type.to_arrow(self.value_type)
        else:
            arrow_type = self.data_type.to_arrow()
        return pa.field(self.name, arrow_type, nullable=nullable)

    def as_spark_sql(self) -> str:
        return f"{self.name} {self.data_type.as_spark_sql(self.null_prob)}"


@dataclass
class Schema:
    fields: List[Field]

    def to_arrow(self):
        arrow_fields = [f.to_arrow() for f in self.fields]
        return pa.schema(arrow_fields)

    def as_spark_sql(self) -> str:
        fields_sql = []
        for f in self.fields:
            fields_sql.append(f.as_spark_sql())
        return f"({', '.join(fields_sql)})"


def generate_value(field: Field, num_rows: int):
    dtype = field.data_type
    null_prob = field.null_prob if field.null_prob else 0
    n_null = int(num_rows * null_prob)
    values = []

    if dtype == DataType.Int32 or dtype == DataType.Int64:
        values = [fake.random_int(min=0, max=100) for _ in range(num_rows)]
    elif dtype == DataType.Float32 or dtype == DataType.Float64:
        values = [
            fake.pyfloat(left_digits=2, right_digits=3, positive=True)
            for _ in range(num_rows)
        ]
    elif dtype == DataType.Bool:
        values = [fake.boolean() for _ in range(num_rows)]
    elif dtype == DataType.String:
        values = [fake.name() for _ in range(num_rows)]
    elif dtype == DataType.Binary:
        values = [fake.binary(length=10) for _ in range(num_rows)]
    elif dtype == DataType.Date32 or dtype == DataType.Date64:
        values = [
            fake.date_between(start_date="-10y", end_date="today")
            for _ in range(num_rows)
        ]
    elif dtype == DataType.Timestamp:
        values = [fake.date_time_this_decade() for _ in range(num_rows)]
    elif dtype == DataType.Decimal128:
        values = [
            fake.pydecimal(left_digits=4, right_digits=2, positive=True)
            for _ in range(num_rows)
        ]
    elif dtype == DataType.List:
        raise NotImplementedError()
        # field.value_type 必须已设定
        # item_field = (
        #     Field("item", field.value_type)
        #     if not isinstance(field.value_type, Field)
        #     else field.value_type
        # )
        # values = []
        # for _ in range(num_rows):
        #     n = random.randint(0, 5)  # 每个List元素数量
        #     values.append([generate_value(item_field, 1)[0] for _ in range(n)])
    elif dtype == DataType.Struct:
        raise NotImplementedError()
        # field.value_type 是子Field的列表
        # values = []
        # for _ in range(num_rows):
        #     record = {}
        #     for subfield in field.value_type:
        #         record[subfield.name] = generate_value(subfield, 1)[0]
        #     values.append(record)
    elif dtype == DataType.Map:
        raise NotImplementedError()
        # field.value_type 为 (key_type, item_type)
        # key_type, val_type = field.value_type
        # key_field = Field("key", key_type)
        # val_field = Field("item", val_type)
        # values = []
        # for _ in range(num_rows):
        #     n = random.randint(0, 3)
        #     keys = [generate_value(key_field, 1)[0] for _ in range(n)]
        #     vals = [generate_value(val_field, 1)[0] for _ in range(n)]
        #     values.append(list(zip(keys, vals)))
    else:
        values = [None] * num_rows  # 兜底

    # 按null_prob随机置为None
    if n_null > 0:
        null_idx = random.sample(range(num_rows), n_null)
        for idx in null_idx:
            values[idx] = None  # type: ignore
    return values


def generate_fake_data(schema: Schema, num_rows: int):
    data = {}
    for field in schema.fields:
        data[field.name] = generate_value(field, num_rows)
    return pd.DataFrame(data)


def data_init(
    task_id: str,
    schema: Schema,
    rows: int,
    times: int,
    dest_prefix: str | None,
):
    """
    generate random data and upload to `path`
    """
    import tempfile
    from e2e.utils import oss_upload_files
    from e2e.variables import E2E_PATH_PREFIX
    from pathlib import Path

    if dest_prefix is None:
        dest_prefix = f"{E2E_PATH_PREFIX}/data-init/{task_id}/"

    for _ in range(times):
        df = generate_fake_data(schema, rows)
        logging.debug(f"data:\n{df}")
        arrow_schema = schema.to_arrow()
        table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        local_path = Path(f"{tempfile.gettempdir()}/{task_id}-{random_str()}.parquet")
        pq.write_table(table, local_path)
        oss_upload_files([local_path], dest_prefix=dest_prefix)


class DataInitOperator(PythonOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        schema: Schema,
        rows: int,
        times: int = 1,
        dest_prefix: str | None = None,
        **kwargs,
    ):
        task_id = f"data-init-{random_str()}" if task_id is None else task_id
        op_args = [task_id, schema, rows, times, dest_prefix]
        super().__init__(
            task_id=task_id, python_callable=data_init, op_args=op_args, **kwargs
        )
