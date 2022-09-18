package org.apache.flink.lakeSoul.sink;


import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.*;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.table.DeserializationRuntimeConverter;
import io.debezium.data.Envelope;
import io.debezium.time.*;
import io.debezium.time.Timestamp;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.lakeSoul.types.SourceRecordJsonSerde;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
                    return new BinaryType();
                default:
                    System.out.println("unsupported type"+fieldSchema.type().name());
                  //  return otherLogicalType(fieldSchema);
            }
            return null;
    }
    public LogicalType otherLogicalType(Schema fieldSchema){
        switch (fieldSchema.type().name()) {
            case MicroTime.SCHEMA_NAME:
            case NanoTime.SCHEMA_NAME:
                return new TimeType();//time
            case Timestamp.SCHEMA_NAME:
            case MicroTimestamp.SCHEMA_NAME:
            case NanoTimestamp.SCHEMA_NAME:           //timestamp
                return new TimestampType();
            case Decimal.LOGICAL_NAME:
                return  new DecimalType(20,3);
            default:
        }
        return null;
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

    private Object convertSqlSchemaAndField(Object fieldValue, Schema fieldSchema, ZoneId serverTimeZone)
            throws Exception {
        switch (fieldSchema.type()) {
            case BOOLEAN:
                return convertToBoolean(fieldValue, fieldSchema);
            case INT8:
            case INT16:
            case INT32:
                return convertToInt(fieldValue, fieldSchema);
            case INT64:
                return convertToLong(fieldValue, fieldSchema);
//            case INTERVAL_YEAR_MONTH:
//                return convertToInt();
//            case BIGINT:
//            case INTERVAL_DAY_TIME:
//                return convertToLong();
//            case DATE:
//                return convertToDate();
//            case TIME_WITHOUT_TIME_ZONE:
//                return convertToTime();
//            case TIMESTAMP_WITHOUT_TIME_ZONE:
//                return convertToTimestamp(serverTimeZone);
//            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
//                return convertToLocalTimeZoneTimestamp(serverTimeZone);
            case FLOAT32:
                return convertToFloat(fieldValue, fieldSchema);
            case FLOAT64:
                return convertToDouble(fieldValue, fieldSchema);
            case STRING:
                return convertToString(fieldValue, fieldSchema);
            case BYTES:
                return convertToBinary(fieldValue, fieldSchema);

            default:
                throw new UnsupportedOperationException("Unsupported type: ");
        }
    }

    public Object convertToBoolean(Object dbzObj, Schema schema) {
        if (dbzObj instanceof Integer) {
            return dbzObj;
        } else if (dbzObj instanceof Long) {
            return ((Long) dbzObj).intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
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
