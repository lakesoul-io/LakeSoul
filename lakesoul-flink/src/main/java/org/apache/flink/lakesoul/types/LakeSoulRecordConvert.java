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

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.*;
import io.debezium.data.Envelope;
import io.debezium.time.Timestamp;
import io.debezium.time.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakesoul.tool.LakeSoulKeyGen;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;

public class LakeSoulRecordConvert implements Serializable {
    private final ZoneId serverTimeZone = ZoneId.of("UTC");

    public LakeSoulRowDataWrapper toLakeSoulDataType(JsonSourceRecord item) throws Exception {
        SourceRecordJsonSerde srj = SourceRecordJsonSerde.getInstance();
        SchemaAndValue valueAndschema = item.getValue(srj);
        Struct value = (Struct) valueAndschema.value();
        Schema sch = valueAndschema.schema();
        Envelope.Operation op = getOperation(sch, value);
        Schema valueSchema = value.schema();
        LakeSoulRowDataWrapper.Build build = LakeSoulRowDataWrapper.newBuild().setTableId(item.getTableId());
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

    public RowType toFlinkRowTypePK(Schema schema, List<String> pks) {
        String[] colNames = new String[pks.size()];
        LogicalType[] colTypes = new LogicalType[pks.size()];
        for (int i = 0; i < pks.size(); i++) {
            Field item = schema.field(pks.get(i));
            colNames[i] = item.name();
            colTypes[i] = convertToLogical(item.schema());
        }
        return RowType.of(colTypes,colNames);
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
        return RowType.of(colTypes,colNames);
    }

    public LogicalType convertToLogical(Schema fieldSchema) {
        switch (fieldSchema.type()) {
            case BOOLEAN:
                return new BooleanType();
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
                throw new UnsupportedOperationException("unsupported type" + fieldSchema.type().name());
        }
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

    private RowData convertPrimaryKey(Struct struct, Schema schema, List<String> primaryKeys) throws Exception {
        GenericRowData row = new GenericRowData(primaryKeys.size());
        int i = 0;
        for (String pk : primaryKeys) {
            Field field = schema.field(pk);
            String fieldName = field.name();
            Object fieldValue = struct.getWithoutDefault(fieldName);
            Schema fieldSchema = schema.field(fieldName).schema();
            Object convertedField =
                    convertSqlField(fieldValue, fieldSchema, serverTimeZone);
            row.setField(i, convertedField);
            ++i;
        }
        return row;
    }

    public Tuple2<RowData, RowType> convertPrimaryKey(JsonSourceRecord sourceRecord) throws Exception {
        SourceRecordJsonSerde srj = SourceRecordJsonSerde.getInstance();
        List<String> primaryKeys = sourceRecord.getPrimaryKeys();
        SchemaAndValue valueAndschema = sourceRecord.getValue(srj);
        Struct value = (Struct) valueAndschema.value();
        Schema sch = valueAndschema.schema();
        Envelope.Operation op = getOperation(sch, value);
        Schema valueSchema = value.schema();
        if (op == Envelope.Operation.DELETE) {
            Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            return Tuple2.of(convertPrimaryKey(before, beforeSchema, primaryKeys),
                             toFlinkRowTypePK(beforeSchema, primaryKeys));
        } else {
            Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
            Struct after = value.getStruct(Envelope.FieldName.AFTER);
            return Tuple2.of(convertPrimaryKey(after, afterSchema, primaryKeys),
                             toFlinkRowTypePK(afterSchema, primaryKeys));
        }
    }

    public long computePrimaryKeyHash(Tuple2<RowData, RowType> primaryKeys) {
        long hash = 42;
        RowData rowData = primaryKeys.f0;
        RowType rowType = primaryKeys.f1;
        for (int i = 0; i < rowData.getArity(); i++) {
            Object fieldOrNull =
                    RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(rowData);
            hash = LakeSoulKeyGen.getHash(rowType.getTypeAt(i), fieldOrNull, hash);
        }
        return hash;
    }

    public long computeJsonRecordPrimaryKeyHash(JsonSourceRecord sourceRecord) throws Exception {
        return computePrimaryKeyHash(convertPrimaryKey(sourceRecord));
    }

    public RowData convert(Struct struct, Schema schema) throws Exception {
        if (struct == null) {
            return null;
        }
        int arity = schema.fields().size();
        List<Field> fieldNames =  schema.fields();
        GenericRowData row = new GenericRowData(arity);
        for (int i = 0; i < arity; i++) {
            Field field = fieldNames.get(i);
            String fieldName = field.name();
            Object fieldValue = struct.getWithoutDefault(fieldName);
            Schema fieldSchema = schema.field(fieldName).schema();
            Object convertedField =
                    convertSqlField(fieldValue, fieldSchema, serverTimeZone);
            row.setField(i, convertedField);
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
            // TODO: Handle time related types
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
