# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import json
import pyarrow


def to_arrow_field(spark_field_json):
    spark_type = spark_field_json['type']
    arrow_type = None
    if spark_type == 'long':
        arrow_type = pyarrow.int64()
    elif spark_type == 'integer':
        arrow_type = pyarrow.int32()
    elif spark_type == 'string':
        arrow_type = pyarrow.utf8()
    elif spark_type == 'float':
        arrow_type = pyarrow.float32()
    elif spark_type == 'double':
        arrow_type = pyarrow.float64()
    elif spark_type == "binary":
        arrow_type = pyarrow.binary()
    elif spark_type.startswith("decimal"):
        arrow_type = pyarrow.decimal128(38)
    elif spark_type == 'struct':
        fields = spark_field_json['fields']
        arrow_fields = []
        for field in fields:
            arrow_fields.append(to_arrow_field(field))
        arrow_type = pyarrow.struct(arrow_fields)
    else:
        raise IOError("Not supported spark type " + str(spark_type))
    return pyarrow.field(spark_field_json['name'], arrow_type, spark_field_json['nullable'])


def to_arrow_schema(spark_schema_str, exclude_columns=None):
    exclude_columns = frozenset(exclude_columns or frozenset())
    fields = json.loads(spark_schema_str)['fields']
    arrow_fields = []
    for field in fields:
        if field['name'] in exclude_columns:
            continue
        arrow_fields.append(to_arrow_field(field))
    return pyarrow.schema(arrow_fields)
