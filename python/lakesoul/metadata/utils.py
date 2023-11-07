# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import json
import pyarrow


def deserialize_from_rust_arrow_type(arrow_type_json):
    if isinstance(arrow_type_json, str):
        if arrow_type_json == 'Boolean':
            return pyarrow.bool_()
        elif arrow_type_json == 'Date32':
            return pyarrow.date32()
        elif arrow_type_json == 'Date64':
            return pyarrow.date64()
        elif arrow_type_json == 'Int8':
            return pyarrow.int8()
        elif arrow_type_json == 'Int16':
            return pyarrow.int16()
        elif arrow_type_json == 'Int32':
            return pyarrow.int32()
        elif arrow_type_json == 'Int64':
            return pyarrow.int64()
        elif arrow_type_json == 'UInt8':
            return pyarrow.uint8()
        elif arrow_type_json == 'UInt16':
            return pyarrow.uint16()
        elif arrow_type_json == 'UInt32':
            return pyarrow.uint32()
        elif arrow_type_json == 'UInt64':
            return pyarrow.uint64()
        elif arrow_type_json == 'String':
            return pyarrow.string()
        elif arrow_type_json == 'Utf8':
            return pyarrow.utf8()
        elif arrow_type_json == 'LargeUtf8':
            return pyarrow.large_utf8()
        elif arrow_type_json == 'Float32':
            return pyarrow.float32()
        elif arrow_type_json == 'Float64':
            return pyarrow.float64()
        elif arrow_type_json == "Binary":
            return pyarrow.binary()
        elif arrow_type_json == "LargeBinary":
            return pyarrow.large_binary()
        elif arrow_type_json == "Null":
            return pyarrow.null()
    elif isinstance(arrow_type_json, dict):
        if 'Decimal128' in arrow_type_json:
            return pyarrow.decimal128(arrow_type_json['Decimal128'][0], arrow_type_json['Decimal128'][1])
        elif 'Decimal256' in arrow_type_json:
            return pyarrow.decimal256(arrow_type_json['Decimal256'][0], arrow_type_json['Decimal256'][1])
        elif 'Interval' in arrow_type_json:
            if arrow_type_json['Interval'] == 'DayTime':
                return pyarrow.month_day_nano_interval()
            elif arrow_type_json['Interval'] == 'YearMonth':
                return pyarrow.month_day_nano_interval()
        elif 'List' in arrow_type_json:
            return pyarrow.list_(deserialize_from_rust_arrow_type(arrow_type_json['List']['data_type']))
        elif 'FixedSizeList' in arrow_type_json:
            return pyarrow.list_(deserialize_from_rust_arrow_type(arrow_type_json['FixedSizeList'][0]['data_type']),
                                 arrow_type_json['FixedSizeList'][1])
        elif 'Dictionary' in arrow_type_json:
            return pyarrow.dictionary(arrow_type_json['Dictionary'][0], arrow_type_json['Dictionary'][1])
        elif 'FixedSizeBinary' in arrow_type_json:
            return pyarrow.binary(arrow_type_json['FixedSizeBinary'])
        elif 'Map' in arrow_type_json:
            return pyarrow.map_(
                deserialize_from_rust_arrow_type(arrow_type_json['Map'][0]['data_type']['Struct'][0]['data_type']),
                deserialize_from_rust_arrow_type(arrow_type_json['Map'][0]['data_type']['Struct'][1]['data_type']),
                arrow_type_json['Map'][1])
        elif 'Struct' in arrow_type_json:
            arrow_fields = []
            for field in arrow_type_json['Struct']:
                arrow_fields.append(to_arrow_field(field))
            return pyarrow.struct(arrow_fields)
        elif 'Time32' in arrow_type_json:
            return pyarrow.time32('ms' if arrow_type_json['Time32'] == 'Millisecond' else 's')
        elif 'Time64' in arrow_type_json:
            return pyarrow.time64('us' if arrow_type_json['Time64'] == 'Microsecond' else 'ns')
        elif 'Timestamp' in arrow_type_json:
            unit = arrow_type_json['Timestamp'][0]
            unit = 's' if unit == 'Second' else 'ms' if unit == 'Millisecond' else 'us' if unit == 'Microsecond' else 'ns'
            return pyarrow.timestamp(unit, arrow_type_json['Timestamp'][1])
    raise IOError("Failed at deserialize_from_rust_arrow_type: " + str(arrow_type_json))


def deserialize_from_java_arrow_field(arrow_field_json):
    arrow_type_json = arrow_field_json['type']
    filed_name = arrow_field_json['name']
    nullable = arrow_field_json['nullable']
    arrow_type = None
    if isinstance(arrow_type_json, dict):
        name = arrow_type_json['name']
        if name == 'null':
            arrow_type = pyarrow.null()
        if name == 'struct':
            arrow_fields = []
            for child_field in arrow_field_json['children']:
                arrow_fields.append(deserialize_from_java_arrow_field(child_field))
            arrow_type = pyarrow.struct(arrow_fields)
        if name == 'list':
            child_field = arrow_field_json['children'][0]
            arrow_type = pyarrow.list_(deserialize_from_java_arrow_field(child_field).type)
        if name == 'largelist':
            child_field = arrow_field_json['children'][0]
            arrow_type = pyarrow.large_list(deserialize_from_java_arrow_field(child_field).type)
        if name == 'fixedsizelist':
            child_field = arrow_field_json['children'][0]
            list_size = arrow_type_json['listSize']
            arrow_type = pyarrow.list_(deserialize_from_java_arrow_field(child_field).type, list_size)
        if name == 'union':
            pass
        if name == 'map':
            keys_sorted = arrow_type_json['keysSorted']
            child_field = arrow_field_json['children'][0]
            child_type = deserialize_from_java_arrow_field(child_field).type
            pyarrow.map_(child_type.field[0].type, child_type.field[1].type, keys_sorted)
        if name == 'int':
            if arrow_type_json['isSigned']:
                if arrow_type_json['bitWidth'] == 8:
                    arrow_type = pyarrow.int8()
                elif arrow_type_json['bitWidth'] == 16:
                    arrow_type = pyarrow.int16()
                elif arrow_type_json['bitWidth'] == 32:
                    arrow_type = pyarrow.int32()
                elif arrow_type_json['bitWidth'] == 64:
                    arrow_type = pyarrow.int64()
            else:
                if arrow_type_json['bitWidth'] == 8:
                    arrow_type = pyarrow.uint8()
                elif arrow_type_json['bitWidth'] == 16:
                    arrow_type = pyarrow.uint16()
                elif arrow_type_json['bitWidth'] == 32:
                    arrow_type = pyarrow.uint32()
                elif arrow_type_json['bitWidth'] == 64:
                    arrow_type = pyarrow.uint64()
        if name == 'floatingpoint':
            precision = arrow_type_json['precision']
            if precision == 'HALF':
                arrow_type = pyarrow.float16()
            elif precision == 'SINGLE':
                arrow_type = pyarrow.float32()
            elif precision == 'DOUBLE':
                arrow_type = pyarrow.float64()
        if name == 'utf8':
            arrow_type = pyarrow.utf8()
        if name == 'largeutf8':
            arrow_type = pyarrow.large_utf8()
        if name == 'binary':
            arrow_type = pyarrow.binary()
        if name == 'largebinary':
            arrow_type = pyarrow.large_binary()
        if name == 'fixedsizebinary':
            bit_width = arrow_type_json['bitWidth']
            arrow_type = pyarrow.binary(bit_width)
        if name == 'bool':
            arrow_type = pyarrow.bool_()
        if name == 'decimal':
            precision = arrow_type_json['precision']
            scale = arrow_type_json['scale']
            bit_width = arrow_type_json['bitWidth']
            if bit_width > 128:
                arrow_type = pyarrow.decimal256(precision, scale)
            else:
                arrow_type = pyarrow.decimal128(precision, scale)
        if name == 'date':
            unit = arrow_type_json['unit']
            if unit == 'DAY':
                arrow_type = pyarrow.date32()
            else:
                arrow_type = pyarrow.date64()
        if name == 'time':
            unit = arrow_type_json['unit']
            unit = arrow_type_json['unit']
            if unit == 'SECOND':
                unit = 's'
            elif unit == 'MILLISECOND':
                unit = 'ms'
            elif unit == 'MICROSECOND':
                unit = 'us'
            elif unit == 'NANOSECOND':
                unit = 'ns'
            bit_width = arrow_type_json['bitWidth']
            if bit_width > 32:
                arrow_type = pyarrow.time64(unit)
            else:
                arrow_type = pyarrow.time32(unit)
        if name == 'timestamp':
            unit = arrow_type_json['unit']
            if unit == 'SECOND':
                unit = 's'
            elif unit == 'MILLISECOND':
                unit = 'ms'
            elif unit == 'MICROSECOND':
                unit = 'us'
            elif unit == 'NANOSECOND':
                unit = 'ns'
            timezone = arrow_type_json['timezone']
            arrow_type = pyarrow.timestamp(unit, timezone)
        if name == 'interval':
            pass
        if name == 'duration':
            pass
    if arrow_type is None:
        raise IOError("Failed at deserialize_from_java_arrow_type: " + str(arrow_type_json))
    return pyarrow.field(filed_name, arrow_type, nullable)


def to_arrow_field(arrow_field_json):
    if 'data_type' in arrow_field_json:
        return pyarrow.field(arrow_field_json['name'], deserialize_from_rust_arrow_type(arrow_field_json['data_type']),
                             arrow_field_json['nullable'])
    else:
        return deserialize_from_java_arrow_field(arrow_field_json)


def to_arrow_schema(schema_json_str, exclude_columns=None):
    exclude_columns = frozenset(exclude_columns or frozenset())
    _json = json.loads(schema_json_str)
    fields = json.loads(schema_json_str)['fields']
    arrow_fields = []
    for field in fields:
        if field['name'] in exclude_columns:
            continue
        arrow_fields.append(to_arrow_field(field))
    #print(arrow_fields)
    return pyarrow.schema(arrow_fields)
