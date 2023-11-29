package com.ververica.cdc.connectors.base.utils;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import io.debezium.document.DocumentReader;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.util.SchemaNameAdjuster;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import org.apache.flink.table.types.logical.RowType;

public class SourceRecordUtils {
    public static final String SCHEMA_CHANGE_EVENT_KEY_NAME = "io.debezium.connector.mysql.SchemaChangeKey";
    public static final String SCHEMA_HEARTBEAT_EVENT_KEY_NAME = "io.debezium.connector.common.Heartbeat";

    public static final String CONNECTOR = "connector";

    public static final String MYSQL_CONNECTOR = "mysql";
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    private SourceRecordUtils() {
    }

    public static Object[] rowToArray(ResultSet rs, int size) throws SQLException {
        Object[] row = new Object[size];

        for(int i = 0; i < size; ++i) {
            row[i] = rs.getObject(i + 1);
        }

        return row;
    }

    public static Long getMessageTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct)record.value();
        if (schema.field("source") == null) {
            return null;
        } else {
            Struct source = value.getStruct("source");
            return source.schema().field("ts_ms") == null ? null : source.getInt64("ts_ms");
        }
    }

    public static Long getFetchTimestamp(SourceRecord record) {
        Schema schema = record.valueSchema();
        Struct value = (Struct)record.value();
        return schema.field("ts_ms") == null ? null : value.getInt64("ts_ms");
    }

    public static boolean isSchemaChangeEvent(SourceRecord sourceRecord) {
        Schema keySchema = sourceRecord.keySchema();
        return keySchema != null && "io.debezium.connector.mysql.SchemaChangeKey".equalsIgnoreCase(keySchema.name());
    }

    public static boolean isDataChangeRecord(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct)record.value();
        return value != null && valueSchema != null && valueSchema.field("op") != null && value.getString("op") != null;
    }

    public static boolean isHeartbeatEvent(SourceRecord record) {
        Schema valueSchema = record.valueSchema();
        return valueSchema != null && "io.debezium.connector.common.Heartbeat".equalsIgnoreCase(valueSchema.name());
    }

    public static TableId getTableId(SourceRecord dataRecord) {
        Struct value = (Struct)dataRecord.value();
        Struct source = value.getStruct("source");
        String dbName = source.getString("db");
        String schemaName = source.getString("schema");
        String tableName = source.getString("table");
        return new TableId(dbName, schemaName, tableName);
    }

    public static Object[] getSplitKey(RowType splitBoundaryType, SourceRecord dataRecord, SchemaNameAdjuster nameAdjuster) {
        String splitFieldName = nameAdjuster.adjust((String)splitBoundaryType.getFieldNames().get(0));
        Struct key = (Struct)dataRecord.key();
        return new Object[]{key.get(splitFieldName)};
    }

    public static boolean splitKeyRangeContains(Object[] key, Object[] splitKeyStart, Object[] splitKeyEnd) {
        if (splitKeyStart == null && splitKeyEnd == null) {
            return true;
        } else {
            int[] lowerBoundRes;
            int i;
            if (splitKeyStart == null) {
                lowerBoundRes = new int[key.length];

                for(i = 0; i < key.length; ++i) {
                    lowerBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
                }

                return Arrays.stream(lowerBoundRes).anyMatch((value) -> {
                    return value < 0;
                }) && Arrays.stream(lowerBoundRes).allMatch((value) -> {
                    return value <= 0;
                });
            } else if (splitKeyEnd == null) {
                lowerBoundRes = new int[key.length];

                for(i = 0; i < key.length; ++i) {
                    lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
                }

                return Arrays.stream(lowerBoundRes).allMatch((value) -> {
                    return value >= 0;
                });
            } else {
                lowerBoundRes = new int[key.length];
                int[] upperBoundRes = new int[key.length];

                for(i = 0; i < key.length; ++i) {
                    lowerBoundRes[i] = compareObjects(key[i], splitKeyStart[i]);
                    upperBoundRes[i] = compareObjects(key[i], splitKeyEnd[i]);
                }

                return Arrays.stream(lowerBoundRes).anyMatch((value) -> {
                    return value >= 0;
                }) && Arrays.stream(upperBoundRes).anyMatch((value) -> {
                    return value < 0;
                }) && Arrays.stream(upperBoundRes).allMatch((value) -> {
                    return value <= 0;
                });
            }
        }
    }

    private static int compareObjects(Object o1, Object o2) {
        if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
            return ((Comparable)o1).compareTo(o2);
        } else {
            return isNumericObject(o1) && isNumericObject(o2) ? toBigDecimal(o1).compareTo(toBigDecimal(o2)) : o1.toString().compareTo(o2.toString());
        }
    }

    private static boolean isNumericObject(Object obj) {
        return obj instanceof Byte || obj instanceof Short || obj instanceof Integer || obj instanceof Long || obj instanceof Float || obj instanceof Double || obj instanceof BigInteger || obj instanceof BigDecimal;
    }

    private static BigDecimal toBigDecimal(Object numericObj) {
        return new BigDecimal(numericObj.toString());
    }

    public static HistoryRecord getHistoryRecord(SourceRecord schemaRecord) throws IOException {
        Struct value = (Struct)schemaRecord.value();
        String historyRecordStr = value.getString("historyRecord");
        return new HistoryRecord(DOCUMENT_READER.read(historyRecordStr));
    }

    public static boolean isMysqlConnector(Struct source) {
        String connector = source.getString(CONNECTOR);
        return MYSQL_CONNECTOR.equalsIgnoreCase(connector);
    }

}