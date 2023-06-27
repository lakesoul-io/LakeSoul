/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.types;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Enum;
import io.debezium.data.EnumSet;
import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.Year;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulKeyGen;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CDC_CHANGE_COLUMN;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CDC_CHANGE_COLUMN_DEFAULT;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SORT_FIELD;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.USE_CDC;

public class LakeSoulRecordConvert implements Serializable {
    private final ZoneId serverTimeZone;
    private final String cdcColumn;

    final boolean useCDC;

    List<String> partitionFields;

    private final JSONObject properties;

    public LakeSoulRecordConvert(Configuration conf, String serverTimeZone) {
        this(conf, serverTimeZone, Collections.emptyList());
    }

    public LakeSoulRecordConvert(Configuration conf, String serverTimeZone, List<String> partitionFields) {
        this.useCDC = conf.getBoolean(USE_CDC);
        this.cdcColumn = conf.getString(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT);
        this.serverTimeZone = ZoneId.of(serverTimeZone);
        this.partitionFields = partitionFields;

        properties = FlinkUtil.getPropertiesFromConfiguration(conf);
    }

    private boolean partitionFieldsChanged(RowType beforeType, RowData beforeData, RowType afterType, RowData afterData) {
        if (this.partitionFields.isEmpty()) {
            return false;
        }
        for (String partitionField : this.partitionFields) {
            int beforeTypeIndex = beforeType.getFieldIndex(partitionField);
            int afterTypeIndex = afterType.getFieldIndex(partitionField);

            // 1. field changed if type mismatched
            if (!beforeType.getTypeAt(beforeTypeIndex).equals(afterType.getTypeAt(afterTypeIndex))) {
                return true;
            }
            RowData.FieldGetter beforeFieldGetter =
                    RowData.createFieldGetter(beforeType.getTypeAt(beforeTypeIndex), beforeTypeIndex);
            Object beforeField = beforeFieldGetter.getFieldOrNull(beforeData);
            RowData.FieldGetter afterFieldGetter =
                    RowData.createFieldGetter(afterType.getTypeAt(afterTypeIndex), afterTypeIndex);
            Object afterField = afterFieldGetter.getFieldOrNull(afterData);
            if (beforeField == null && afterField == null) {
                continue;
            }

            // if this field implements Comparable, use compareTo
            if (beforeField instanceof Comparable && afterField instanceof Comparable) {
                // compareTo returns nonzero, indicates this field has changed
                if (((Comparable) beforeField).compareTo(afterField) != 0) {
                    return true;
                } else {
                    continue;
                }
            }
            // finally, use Object.equals
            if (!beforeField.equals(afterField)) {
                return true;
            }
        }
        return false;
    }

    public LakeSoulRowDataWrapper toLakeSoulDataType(Schema sch, Struct value, TableId tableId, long sortField) throws Exception {
        Envelope.Operation op = getOperation(sch, value);
        Schema valueSchema = value.schema();
        LakeSoulRowDataWrapper.Builder builder = LakeSoulRowDataWrapper.newBuilder().setTableId(tableId).setProperties(properties);
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            RowData insert = convert(after, afterSchema, RowKind.INSERT, sortField);
            RowType rt = toFlinkRowType(afterSchema);
            insert.setRowKind(RowKind.INSERT);
            builder.setOperation("insert").setAfterRowData(insert).setAfterType(rt);
        } else if (op == Envelope.Operation.DELETE) {
            Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            RowData delete = convert(before, beforeSchema, RowKind.DELETE, sortField);
            RowType rt = toFlinkRowType(beforeSchema);
            builder.setOperation("delete").setBeforeRowData(delete).setBeforeRowType(rt);
            delete.setRowKind(RowKind.DELETE);
        } else {
            Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            RowData beforeData = convert(before, beforeSchema, RowKind.UPDATE_BEFORE, sortField);
            RowType beforeRT = toFlinkRowType(beforeSchema);
            beforeData.setRowKind(RowKind.UPDATE_BEFORE);
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            RowData afterData = convert(after, afterSchema, RowKind.UPDATE_AFTER, sortField);
            RowType afterRT = toFlinkRowType(afterSchema);
            afterData.setRowKind(RowKind.UPDATE_AFTER);
            if (partitionFieldsChanged(beforeRT, beforeData, afterRT, afterData)) {
                // partition fields changed. we need to emit both before and after RowData
                builder.setOperation("update").setBeforeRowData(beforeData).setBeforeRowType(beforeRT)
                        .setAfterRowData(afterData).setAfterType(afterRT);
            } else {
                // otherwise we only need to keep the after RowData
                builder.setOperation("update")
                        .setAfterRowData(afterData).setAfterType(afterRT);
            }
        }

        return builder.build();
    }

    public RowType toFlinkRowTypeCDC(RowType rowType) {
        if (!useCDC || rowType.getFieldNames().contains(cdcColumn)) {
            return rowType;
        }
        LogicalType[] colTypes = new LogicalType[rowType.getFieldCount() + 1];
        String[] colNames = new String[rowType.getFieldCount() + 1];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            colNames[i] = rowType.getFieldNames().get(i);
            colTypes[i] = rowType.getTypeAt(i);
        }
        colNames[rowType.getFieldCount()] = cdcColumn;
        colTypes[rowType.getFieldCount()] = new VarCharType(false, Integer.MAX_VALUE);
        return RowType.of(colTypes, colNames);
    }

    public RowType toFlinkRowType(Schema schema) {
        int arity = schema.fields().size() + 1;
        if (useCDC) ++arity;
        String[] colNames = new String[arity];
        LogicalType[] colTypes = new LogicalType[arity];
        List<Field> fieldNames = schema.fields();
        for (int i = 0; i < (useCDC ? arity - 2 : arity - 1); i++) {
            Field item = fieldNames.get(i);
            colNames[i] = item.name();
            colTypes[i] = convertToLogical(item.schema());
        }
//        colNames[useCDC ? arity - 3 : arity - 2] = BINLOG_FILE_INDEX;
//        colTypes[useCDC ? arity - 3 : arity - 2] = new BigIntType();
        colNames[useCDC ? arity - 2 : arity - 1] = SORT_FIELD;
        colTypes[useCDC ? arity - 2 : arity - 1] = new BigIntType();
        if (useCDC) {
            colNames[arity - 1] = cdcColumn;
            colTypes[arity - 1] = new VarCharType(false, Integer.MAX_VALUE);
        }
        return RowType.of(colTypes, colNames);
    }

    public LogicalType convertToLogical(Schema fieldSchema) {
        if (isPrimitiveType(fieldSchema)) {
            return primitiveLogicalType(fieldSchema);
        } else {
            return otherLogicalType(fieldSchema);
        }
    }

    private LogicalType primitiveLogicalType(Schema fieldSchema) {
        boolean nullable = fieldSchema.isOptional();
        switch (fieldSchema.type()) {
            case BOOLEAN:
                return new BooleanType(nullable);
            case INT8:
            case INT16:
            case INT32:
                return new IntType(nullable);
            case INT64:
                return new BigIntType(nullable);
            case FLOAT32:
                return new FloatType(nullable);
            case FLOAT64:
                return new DoubleType(nullable);
            case STRING:
                return new VarCharType(nullable, Integer.MAX_VALUE);
            case BYTES:
                Map<String, String> paras = fieldSchema.parameters();
                int byteLen = Integer.MAX_VALUE;
                if (null != paras) {
                    int len = Integer.parseInt(paras.get("length"));
                    byteLen = len / 8 + (len % 8 == 0 ? 0 : 1);
                }
                return new VarBinaryType(nullable, byteLen);
            default:
                return null;
        }
    }

    private LogicalType otherLogicalType(Schema fieldSchema) {
        boolean nullable = fieldSchema.isOptional();
        switch (fieldSchema.name()) {
            case Enum.LOGICAL_NAME:
            case Json.LOGICAL_NAME:
            case EnumSet.LOGICAL_NAME:
                return new VarCharType(nullable, Integer.MAX_VALUE);
            case Time.SCHEMA_NAME:
            case MicroTime.SCHEMA_NAME:
            case NanoTime.SCHEMA_NAME:
                return new BigIntType(nullable);
            case Timestamp.SCHEMA_NAME:
            case MicroTimestamp.SCHEMA_NAME:
                return new LocalZonedTimestampType(nullable, 6);
            case NanoTimestamp.SCHEMA_NAME:
                return new LocalZonedTimestampType(nullable, 9);
            case Decimal.LOGICAL_NAME:
                Map<String, String> paras = fieldSchema.parameters();
                return new DecimalType(nullable, Integer.parseInt(paras.get("connect.decimal.precision")), Integer.parseInt(paras.get("scale")));
            case Date.SCHEMA_NAME:
                return new DateType(nullable);
            case Year.SCHEMA_NAME:
                return new IntType(nullable);
            case ZonedTime.SCHEMA_NAME:
            case ZonedTimestamp.SCHEMA_NAME:
                return new LocalZonedTimestampType(nullable, LocalZonedTimestampType.DEFAULT_PRECISION);
            case Geometry.LOGICAL_NAME:
            case Point.LOGICAL_NAME:
                paras = fieldSchema.field("wkb").schema().parameters();
                int byteLen = Integer.MAX_VALUE;
                if (null != paras) {
                    int len = Integer.parseInt(paras.get("length"));
                    byteLen = len / 8 + (len % 8 == 0 ? 0 : 1);
                }
                return new VarBinaryType(nullable, byteLen);
            default:
                return null;
        }
    }

    public Envelope.Operation getOperation(Schema sch, Struct value) {
        Field opField = sch.field("op");
        return opField != null ? Envelope.Operation.forCode(value.getString(opField.name())) : null;
    }

    public long computeBinarySourceRecordPrimaryKeyHash(BinarySourceRecord sourceRecord) {
        LakeSoulRowDataWrapper data = sourceRecord.getData();
        RowType rowType = Objects.equals(data.getOp(), "delete") ? data.getBeforeType() : data.getAfterType();
        RowData rowData = Objects.equals(data.getOp(), "delete") ? data.getBefore() : data.getAfter();
        List<String> pks = sourceRecord.getPrimaryKeys();
        long hash = 42;
        for (String pk : pks) {
            int typeIndex = rowType.getFieldIndex(pk);
            LogicalType type = rowType.getTypeAt(typeIndex);
            Object fieldOrNull = RowData.createFieldGetter(type, typeIndex).getFieldOrNull(rowData);
            hash = LakeSoulKeyGen.getHash(type, fieldOrNull, hash);
        }
        return hash;
    }

    public RowData addCDCKindField(RowData rowData, RowData.FieldGetter[] fieldGetters) {
        int newArity = rowData.getArity() + 1;
        GenericRowData rowDataCDC = new GenericRowData(newArity);
        for (int i = 0; i < newArity - 1; ++i) {
            rowDataCDC.setField(i, fieldGetters[i].getFieldOrNull(rowData));
        }
        rowDataCDC.setRowKind(rowData.getRowKind());

        setCDCRowKindField(rowDataCDC);

        return rowDataCDC;
    }

    private String getRowKindStr(RowKind rowKind) {
        String rowKindStr;
        switch (rowKind) {
            case INSERT:
                rowKindStr = "insert";
                break;
            case DELETE:
            case UPDATE_BEFORE:
                rowKindStr = "delete";
                break;
            default:
                rowKindStr = "update";
        }
        return rowKindStr;
    }

    public void setCDCRowKindField(GenericRowData rowData) {
        String rowKindStr = getRowKindStr(rowData.getRowKind());
        rowData.setField(rowData.getArity() - 1, StringData.fromString(rowKindStr));
    }

    public void setCDCRowKindField(BinaryRowWriter writer, RowKind rowKind, int fieldIndex) {
        String rowKindStr = getRowKindStr(rowKind);
        writer.writeString(fieldIndex, StringData.fromString(rowKindStr));
    }

    public RowData convert(Struct struct, Schema schema, RowKind rowKind, long sortField) throws Exception {
        if (struct == null) {
            return null;
        }
        int arity = schema.fields().size() + 1; // for extra event sortField
        if (useCDC) ++arity; // for extra cdc op (RowKind) field
        List<Field> fieldNames = schema.fields();
        BinaryRowData row = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < (useCDC ? arity - 2 : arity - 1); i++) {
            Field field = fieldNames.get(i);
            String fieldName = field.name();
            Object fieldValue = struct.getWithoutDefault(fieldName);
            if (fieldValue == null) {
                writer.setNullAt(i);
                continue;
            }
            Schema fieldSchema = schema.field(fieldName).schema();
            sqlSchemaAndFieldWrite(writer, i, fieldValue, fieldSchema, serverTimeZone);
        }
        writer.writeLong(useCDC ? arity - 2 : arity - 1, sortField);
        writer.writeRowKind(rowKind);
        if (useCDC) {
            setCDCRowKindField(writer, rowKind, arity - 1);
        }
        writer.complete();
        return row;
    }

    private boolean isPrimitiveType(Schema fieldSchema) {
        return fieldSchema.name() == null;
    }

    private void sqlSchemaAndFieldWrite(BinaryRowWriter writer, int index, Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone) {
        if (isPrimitiveType(fieldSchema)) {
            primitiveTypeWrite(writer, index, fieldValue, fieldSchema);
        } else {
            otherTypeWrite(writer, index, fieldValue, fieldSchema, serverTimeZone);
        }
    }

    private void primitiveTypeWrite(BinaryRowWriter writer, int index, Object fieldValue, Schema fieldSchema) {
        switch (fieldSchema.type()) {
            case BOOLEAN:
                writeBoolean(writer, index, fieldValue);
                break;
            case INT8:
            case INT16:
            case INT32:
                writeInt(writer, index, fieldValue);
                break;
            case INT64:
                writeLong(writer, index, fieldValue);
                break;
            case FLOAT32:
                writeFloat(writer, index, fieldValue);
                break;
            case FLOAT64:
                writeDouble(writer, index, fieldValue);
                break;
            case STRING:
                writeString(writer, index, fieldValue);
                break;
            case BYTES:
                writeBinary(writer, index, fieldValue);
                break;
            default:
                throw new UnsupportedOperationException("LakeSoul doesn't support type: " + fieldSchema.type());
        }
    }

    private void otherTypeWrite(BinaryRowWriter writer, int index,
                                Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone) {
        switch (fieldSchema.name()) {
            case Enum.LOGICAL_NAME:
            case Json.LOGICAL_NAME:
            case EnumSet.LOGICAL_NAME:
                writeString(writer, index, fieldValue);
                break;
            case Time.SCHEMA_NAME:
            case MicroTime.SCHEMA_NAME:
            case NanoTime.SCHEMA_NAME:
                writeTime(writer, index, fieldValue, fieldSchema);
                break;
            case Timestamp.SCHEMA_NAME:
            case MicroTimestamp.SCHEMA_NAME:
            case NanoTimestamp.SCHEMA_NAME:
                writeTimeStamp(writer, index, fieldValue, fieldSchema, serverTimeZone);
                break;
            case Decimal.LOGICAL_NAME:
                writeDecimal(writer, index, fieldValue, fieldSchema);
                break;
            case Date.SCHEMA_NAME:
                writeDate(writer, index, fieldValue);
                break;
            case Year.SCHEMA_NAME:
                writeInt(writer, index, fieldValue);
                break;
            case ZonedTime.SCHEMA_NAME:
            case ZonedTimestamp.SCHEMA_NAME:
                writeUTCTimeStamp(writer, index, fieldValue, fieldSchema);
                break;
            // Geometry and Point can not support now
//            case Geometry.LOGICAL_NAME:
//                Object object = convertToGeometry(fieldValue, fieldSchema);
//                writeBinary(writer, index, object);
//                break;
//            case Point.LOGICAL_NAME:
//                object = convertToPoint(fieldValue, fieldSchema);
//                writeBinary(writer, index, object);
//                break;
            default:
                throw new UnsupportedOperationException("LakeSoul doesn't support type: " + fieldSchema.name());
        }
    }

    public long convertToTime(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            return (long) dbzObj;
        } else if (dbzObj instanceof Integer) {
            return (Integer) dbzObj;
        }
        // get number of milliseconds of the day
        return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000L;
    }

    public void writeTime(BinaryRowWriter writer, int index, Object dbzObj, Schema schema) {
        long data = convertToTime(dbzObj, schema);
        writer.writeLong(index, data);
    }

    public Object convertToDecimal(Object dbzObj, Schema schema) {
        BigDecimal bigDecimal;
        if (dbzObj instanceof byte[]) {
            // decimal.handling.mode=precise
            bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
        } else if (dbzObj instanceof String) {
            // decimal.handling.mode=string
            bigDecimal = new BigDecimal((String) dbzObj);
        } else if (dbzObj instanceof Double) {
            // decimal.handling.mode=double
            bigDecimal = BigDecimal.valueOf((Double) dbzObj);
        } else {
            if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
                SpecialValueDecimal decimal =
                        VariableScaleDecimal.toLogical((Struct) dbzObj);
                bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
            } else {
                // fallback to string
                bigDecimal = new BigDecimal(dbzObj.toString());
            }
        }
        Map<String, String> paras = schema.parameters();
        return DecimalData.fromBigDecimal(bigDecimal, Integer.parseInt(paras.get("connect.decimal.precision")), Integer.parseInt(paras.get("scale")));
    }

    public void writeDecimal(BinaryRowWriter writer, int index, Object dbzObj, Schema schema) {
        DecimalData data = (DecimalData) convertToDecimal(dbzObj, schema);
        writer.writeDecimal(index, data, data.precision());
    }

    public Object convertToDate(Object dbzObj, Schema schema) {
        return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
    }

    public void writeDate(BinaryRowWriter writer, int index, Object dbzObj) {
        Integer data = (Integer) convertToDate(dbzObj, null);
        writer.writeInt(index, data);
    }

    public Object convertToUTCTimeStamp(Object dbzObj) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return TimestampData.fromInstant(instant);
        }
        throw new IllegalArgumentException(
                "Unable to convert to TimestampData from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    private int getPrecision(Schema schema) {
        switch (schema.name()) {
            case Time.SCHEMA_NAME:
                return 3;
            case Timestamp.SCHEMA_NAME:
            case MicroTimestamp.SCHEMA_NAME:
            case MicroTime.SCHEMA_NAME:
                return 6;
            default:
                return 9;
        }
    }

    public void writeUTCTimeStamp(BinaryRowWriter writer, int index, Object dbzObj, Schema schema) {
        TimestampData data = (TimestampData) convertToUTCTimeStamp(dbzObj);
        writer.writeTimestamp(index, data, getPrecision(schema));
    }

    public Object convertToTimeStamp(Object dbzObj, Schema schema, ZoneId serverTimeZone) {
        if (dbzObj instanceof Long) {
            Instant instant = null;
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    instant = TimestampData.fromEpochMillis((Long) dbzObj).toInstant();
                    break;
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    instant = TimestampData.fromEpochMillis(
                            micro / 1000, (int) (micro % 1000 * 1000)).toInstant();
                    break;
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    instant = TimestampData.fromEpochMillis(
                            nano / 1000_000, (int) (nano % 1000_000)).toInstant();
            }
            if (instant != null) {
                ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("UTC")).withZoneSameLocal(serverTimeZone);
                return TimestampData.fromInstant(zonedDateTime.toInstant());
            }
            return null;
        }
        // fallback to zoned timestamp
        LocalDateTime localDateTime =
                TemporalConversions.toLocalDateTime(dbzObj, ZoneId.of("UTC"));
        return TimestampData.fromLocalDateTime(localDateTime);
    }

    public Object convertToGeometry(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Struct) {
            return ((Struct) dbzObj).getBytes("wkb");
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Struct value type: " + dbzObj.getClass().getSimpleName());
        }
    }

    private Object convertToPoint(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Struct) {
            return ((Struct) dbzObj).getBytes("wkb");
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Struct value type: " + dbzObj.getClass().getSimpleName());
        }
    }

    public void writeTimeStamp(BinaryRowWriter writer, int index, Object dbzObj, Schema schema, ZoneId serverTimeZone) {
        TimestampData data = (TimestampData) convertToTimeStamp(dbzObj, schema, serverTimeZone);
        writer.writeTimestamp(index, data, getPrecision(schema));
    }

    public void writeBoolean(BinaryRowWriter writer, int index, Object dbzObj) {
        if (dbzObj instanceof Integer) {
            writer.writeBoolean(index, (Integer) dbzObj != 0);
        } else if (dbzObj instanceof Long) {
            writer.writeBoolean(index, (Long) dbzObj != 0);
        } else if (Character.isDigit(dbzObj.toString().indexOf(0))) {
            writer.writeBoolean(index, Integer.parseInt(dbzObj.toString()) != 0);
        } else {
            writer.writeBoolean(index, Boolean.parseBoolean(dbzObj.toString()));
        }
    }

    public void writeInt(BinaryRowWriter writer, int index, Object dbzObj) {
        if (dbzObj instanceof Integer) {
            writer.writeInt(index, (Integer) dbzObj);
        } else if (dbzObj instanceof Long) {
            writer.writeInt(index, ((Long) dbzObj).intValue());
        } else {
            writer.writeInt(index, Integer.parseInt(dbzObj.toString()));
        }
    }

    public void writeFloat(BinaryRowWriter writer, int index, Object dbzObj) {
        if (dbzObj instanceof Float) {
            writer.writeFloat(index, (Float) dbzObj);
        } else if (dbzObj instanceof Double) {
            writer.writeFloat(index, ((Double) dbzObj).floatValue());
        } else {
            writer.writeFloat(index, Float.parseFloat(dbzObj.toString()));
        }
    }

    public void writeDouble(BinaryRowWriter writer, int index, Object dbzObj) {
        if (dbzObj instanceof Float) {
            writer.writeDouble(index, ((Float) dbzObj).doubleValue());
        } else if (dbzObj instanceof Double) {
            writer.writeDouble(index, (Double) dbzObj);
        } else {
            writer.writeDouble(index, Double.parseDouble(dbzObj.toString()));
        }
    }

    public void writeLong(BinaryRowWriter writer, int index, Object dbzObj) {
        if (dbzObj instanceof Integer) {
            writer.writeLong(index, ((Integer) dbzObj).longValue());
        } else if (dbzObj instanceof Long) {
            writer.writeLong(index, (Long) dbzObj);
        } else {
            writer.writeLong(index, Long.parseLong(dbzObj.toString()));
        }
    }

    public void writeString(BinaryRowWriter writer, int index, Object dbzObj) {
        writer.writeString(index, StringData.fromString(dbzObj.toString()));
    }

    public void writeBinary(BinaryRowWriter writer, int index, Object dbzObj) {
        if (dbzObj instanceof byte[]) {
            writer.writeBinary(index, (byte[]) dbzObj);
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            writer.writeBinary(index, bytes);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }
}
