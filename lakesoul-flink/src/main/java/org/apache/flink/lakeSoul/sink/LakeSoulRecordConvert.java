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
package org.apache.flink.lakeSoul.sink;


import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.*;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.*;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.lakeSoul.types.SourceRecordJsonSerde;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class LakeSoulRecordConvert {
    SourceRecordJsonSerde srj = SourceRecordJsonSerde.getInstance();
    SchemaAndValue valueAndschema;
    private ZoneId serverTimeZone = ZoneId.of("UTC");

    public LakeSoulDataType toLakeSoulDataType(JsonSourceRecord item) throws Exception {
        valueAndschema = item.getValue(srj);
        Struct value = (Struct) valueAndschema.value();
        Schema sch = valueAndschema.schema();
        Envelope.Operation op = getOperation(sch, value);
        Schema valueSchema = value.schema();
        LakeSoulDataType.Build build = LakeSoulDataType.newBuild().setTableId(item.getTableId());
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            GenericRowData insert = (GenericRowData) convert(after, afterSchema);
            RowType rt = toFlinkRowType(afterSchema);
            insert.setRowKind(RowKind.INSERT);
            build.setOperation("insert").setAfterRowData(insert).setAfterType(rt);
        } else if (op == Envelope.Operation.DELETE) {
            Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            GenericRowData delete = (GenericRowData) convert(before, beforeSchema);
            RowType rt = toFlinkRowType(beforeSchema);
            build.setOperation("delete").setBeforeRowData(delete).setBeforeRowType(rt);
            delete.setRowKind(RowKind.DELETE);
        } else {
            Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            GenericRowData beforeData = (GenericRowData) convert(before, beforeSchema);
            RowType beforeRT = toFlinkRowType(beforeSchema);
            beforeData.setRowKind(RowKind.UPDATE_BEFORE);
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            GenericRowData afterData = (GenericRowData) convert(after, afterSchema);
            RowType afterRT = toFlinkRowType(afterSchema);
            afterData.setRowKind(RowKind.UPDATE_AFTER);
            build.setOperation("update").setBeforeRowData(beforeData).setBeforeRowType(beforeRT).setAfterRowData(afterData).setAfterType(afterRT);
        }

        return build.build();
    }


    public RowType toFlinkRowType(Schema schema) {
        int arity = schema.fields().size();
        String[] colNames = new String[arity];
        LogicalType[] colTypes = new LogicalType[arity];
        List<Field> fieldNames =  schema.fields();
        for (int i = 0; i < arity; i++) {
            Field item = fieldNames.get(i);
            colNames[i] = item.name();
            colTypes[i] = convertToLogical(item.schema());
        }
        return  RowType.of(colTypes,colNames);
    }

    public LogicalType convertToLogical(Schema fieldSchema){
        if(isPrimitiveType(fieldSchema)){
            return primitiveLogicalType(fieldSchema);
        }else{
            return otherLogicalType(fieldSchema);
        }

    }
    private LogicalType primitiveLogicalType(Schema fieldSchema) {
        switch (fieldSchema.type()) {
            case BOOLEAN:
                return new BinaryType();
            case INT8:
            case INT16:
            case INT32:
                return new IntType();
            case INT64:
                return new BigIntType();
            case FLOAT32:
                return new FloatType();
            case FLOAT64:
                return new DoubleType();
            case STRING:
                return new VarCharType(Integer.MAX_VALUE);
            case BYTES:
                return new BinaryType(Integer.MAX_VALUE);
            default:
                return null;
        }
    }
        private LogicalType otherLogicalType(Schema fieldSchema){
        switch (fieldSchema.name()) {
            case MicroTime.SCHEMA_NAME:
            case NanoTime.SCHEMA_NAME:
                return new TimeType(9);//time
            case Timestamp.SCHEMA_NAME:
            case MicroTimestamp.SCHEMA_NAME:
            case NanoTimestamp.SCHEMA_NAME:           //timestamp
                return new TimestampType(9);
            case Decimal.LOGICAL_NAME:
                Map<String,String> paras= ((ConnectSchema) fieldSchema).parameters();
                return  new DecimalType(Integer.parseInt(paras.get("connect.decimal.precision")),Integer.parseInt(paras.get("scale")));
            case Date.SCHEMA_NAME:
                return new DateType();
            case Year.SCHEMA_NAME:
                return new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR);//date
            case ZonedTime.SCHEMA_NAME:
            case ZonedTimestamp.SCHEMA_NAME:
                return new ZonedTimestampType(9);
            default: return null;
        }

    }
    public Envelope.Operation getOperation(Schema sch, Struct value) {
        Field opField = sch.field("op");
        return opField != null ? Envelope.Operation.forCode(value.getString(opField.name())) : null;
    }

    public RowData convert(Object dbzObj, Schema schema) throws Exception {
        if (dbzObj == null) {
            return null;
        }
        Struct struct = (Struct) dbzObj;
        int arity = schema.fields().size();
        List<Field> fieldNames =  schema.fields();
        GenericRowData row = new GenericRowData(arity);
        for (int i = 0; i < arity; i++) {
            Field field = fieldNames.get(i);
            String fieldName = field.name();
            if (field == null) {
                row.setField(i, null);
            } else {
                Object fieldValue = struct.getWithoutDefault(fieldName);
                Schema fieldSchema = schema.field(fieldName).schema();
                Object convertedField =
                        convertSqlField(fieldValue, fieldSchema, serverTimeZone);
                row.setField(i, convertedField);
            }
        }
        return row;
    }

    private Object convertSqlField(Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone)
            throws Exception {
        if (fieldValue == null) {
            return null;
        } else {
            return convertSqlSchemaAndField(fieldValue, fieldSchema, serverTimeZone);
        }
    }


    private GenericRowData extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return (GenericRowData) convert(after, afterSchema);
    }

    private GenericRowData extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return (GenericRowData) convert(before, beforeSchema);
    }

    private boolean isPrimitiveType(Schema fieldSchema){
        String name =fieldSchema.name();
        if(MicroTime.SCHEMA_NAME.equals(name)
                ||NanoTime.SCHEMA_NAME.equals(name)
                ||Timestamp.SCHEMA_NAME.equals(name)
                ||MicroTimestamp.SCHEMA_NAME.equals(name)
                ||NanoTimestamp.SCHEMA_NAME.equals(name)
                ||Decimal.LOGICAL_NAME.equals(name)
                ||Date.SCHEMA_NAME.equals(name)
                ||Year.SCHEMA_NAME.equals(name)
                ||ZonedTime.SCHEMA_NAME.equals(name)
                ||ZonedTimestamp.SCHEMA_NAME.equals(name)
        ){
            return false;
        }else{
            return true;
        }


    }

    private Object convertSqlSchemaAndField(Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone)
            throws Exception {
        if(isPrimitiveType(fieldSchema)){
            return primitiveTypeConvert(fieldValue,fieldSchema,serverTimeZone);
        }else{
            return otherTypeConvert(fieldValue,fieldSchema,serverTimeZone);
        }

    }
    private Object primitiveTypeConvert(Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone) {
        switch (fieldSchema.type()) {
            case BOOLEAN:
                return convertToBoolean(fieldValue, fieldSchema);
            case INT8:
            case INT16:
            case INT32:
                return convertToInt(fieldValue, fieldSchema);
            case INT64:
                return convertToLong(fieldValue, fieldSchema);
            case FLOAT32:
                return convertToFloat(fieldValue, fieldSchema);
            case FLOAT64:
                return convertToDouble(fieldValue, fieldSchema);
            case STRING:
                return convertToString(fieldValue, fieldSchema);
            case BYTES:
                return convertToBinary(fieldValue, fieldSchema);
            default:
                return null;
        }
    }
    private Object otherTypeConvert(Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone){
        switch (fieldSchema.name()) {
            case MicroTime.SCHEMA_NAME:
            case NanoTime.SCHEMA_NAME:
                return convertToTime(fieldValue,fieldSchema);//time
            case Timestamp.SCHEMA_NAME:
            case MicroTimestamp.SCHEMA_NAME:
            case NanoTimestamp.SCHEMA_NAME:           //timestamp
                return convertToTimeStamp(fieldValue,fieldSchema);
            case Decimal.LOGICAL_NAME:
                return  convertToDecimal(fieldValue,fieldSchema);
            case Date.SCHEMA_NAME:
                return convertToDate(fieldValue,fieldSchema);//date
            case Year.SCHEMA_NAME:
                return  convertToInt(fieldValue, fieldSchema);
            case ZonedTime.SCHEMA_NAME:
            case ZonedTimestamp.SCHEMA_NAME:
                return convertToZonedTimeStamp(fieldValue,fieldSchema,serverTimeZone);
            default: return null;
        }
    }


    public Object convertToTime(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case MicroTime.SCHEMA_NAME:
                    return (int) ((long) dbzObj / 1000);
                case NanoTime.SCHEMA_NAME:
                    return (int) ((long) dbzObj / 1000_000);
            }
        } else if (dbzObj instanceof Integer) {
            return dbzObj;
        }
        // get number of milliseconds of the day
        return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
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
//            if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
//                SpecialValueDecimal decimal =
//                        VariableScaleDecimal.toLogical((Struct) dbzObj);
//                bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
//            } else {
                // fallback to string
                bigDecimal = new BigDecimal(dbzObj.toString());
//            }
        }
        return DecimalData.fromBigDecimal(bigDecimal, 20, 3);
    }

    public Object convertToDate(Object dbzObj, Schema schema) {
        return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
    }

    public Object convertToZonedTimeStamp(Object dbzObj, Schema schema,ZoneId serverTimeZone) {
        if (dbzObj instanceof String) {
            String str = (String) dbzObj;
            // TIMESTAMP_LTZ type is encoded in string type
            Instant instant = Instant.parse(str);
            return TimestampData.fromLocalDateTime(
                    LocalDateTime.ofInstant(instant, serverTimeZone));
        }
        throw new IllegalArgumentException(
                "Unable to convert to TimestampData from unexpected value '"
                        + dbzObj
                        + "' of type "
                        + dbzObj.getClass().getName());
    }

    public Object convertToTimeStamp(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Long) {
            switch (schema.name()) {
                case Timestamp.SCHEMA_NAME:
                    return TimestampData.fromEpochMillis((Long) dbzObj);
                case MicroTimestamp.SCHEMA_NAME:
                    long micro = (long) dbzObj;
                    return TimestampData.fromEpochMillis(
                            micro / 1000, (int) (micro % 1000 * 1000));
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) dbzObj;
                    return TimestampData.fromEpochMillis(
                            nano / 1000_000, (int) (nano % 1000_000));
            }
        }
        LocalDateTime localDateTime =
                TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
        return TimestampData.fromLocalDateTime(localDateTime);
    }



    public Object convertToBoolean(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Boolean) {
            return dbzObj;
        } else if (dbzObj instanceof Byte) {
            return (byte) dbzObj == 1;
        } else if (dbzObj instanceof Short) {
            return (short) dbzObj == 1;
        } else {
            return Boolean.parseBoolean(dbzObj.toString());
        }
    }

    public Object convertToInt(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return dbzObj;
        } else if (dbzObj instanceof Long) {
            return ((Long) dbzObj).intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
        }
    }

    public Object convertToFloat(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return dbzObj;
        } else if (dbzObj instanceof Double) {
            return ((Double) dbzObj).floatValue();
        } else {
            return Float.parseFloat(dbzObj.toString());
        }
    }

    public Object convertToDouble(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Float) {
            return ((Float) dbzObj).doubleValue();
        } else if (dbzObj instanceof Double) {
            return dbzObj;
        } else {
            return Double.parseDouble(dbzObj.toString());
        }
    }

    public Object convertToLong(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return ((Integer) dbzObj).longValue();
        } else if (dbzObj instanceof Long) {
            return dbzObj;
        } else {
            return Long.parseLong(dbzObj.toString());
        }
    }

    public Object convertToString(Object dbzObj, Schema schema) {
        return StringData.fromString(dbzObj.toString());
    }

    public Object convertToBinary(Object dbzObj, Schema schema) {
        if (dbzObj instanceof byte[]) {
            return dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }
}
