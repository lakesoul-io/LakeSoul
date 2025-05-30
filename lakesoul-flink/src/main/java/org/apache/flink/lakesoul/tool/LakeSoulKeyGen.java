// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class LakeSoulKeyGen implements Serializable {

    private static final long serialVersionUID = 6042851811029368902L;
    private boolean simpleRecordKey = false;
    private final int[] hashKeyIndex;
    private final LogicalType[] hashKeyType;

    public LakeSoulKeyGen(RowType rowType, String[] recordKeyFields) {
        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldTypes = rowType.getChildren();
        if (recordKeyFields.length == 1) {
            this.simpleRecordKey = true;
        }
        this.hashKeyIndex = getFieldPositions(recordKeyFields, fieldNames);
        this.hashKeyType = Arrays.stream(hashKeyIndex).mapToObj(fieldTypes::get).toArray(LogicalType[]::new);
    }

    private static int[] getFieldPositions(String[] fields, List<String> allFields) {
        return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
    }

    public long getRePartitionHash(RowData rowData) {
        long hash = 42;
        if (hashKeyType.length == 0) {
            return hash;
        }
        if (this.simpleRecordKey) {
            Object fieldOrNull = RowData.createFieldGetter(hashKeyType[0], hashKeyIndex[0]).getFieldOrNull(rowData);
            return getHash(hashKeyType[0], fieldOrNull, hash);
        } else {
            for (int i = 0; i < hashKeyType.length; i++) {
                Object fieldOrNull = RowData.createFieldGetter(hashKeyType[i], hashKeyIndex[i]).getFieldOrNull(rowData);
                hash = getHash(hashKeyType[i], fieldOrNull, hash);
            }
            return hash;
        }
    }

    public static long getHash(LogicalType type, Object field, long seed) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                UTF8String utf8String = UTF8String.fromString(java.lang.String.valueOf(field));
                seed = Murmur3HashFunction.hash(utf8String, StringType, seed);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                seed = Murmur3HashFunction.hash(field, IntegerType, seed);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                seed = Murmur3HashFunction.hash(field, LongType, seed);
                break;
            case TINYINT:
                seed = Murmur3HashFunction.hash(field, ByteType, seed);
                break;
            case BINARY:
            case VARBINARY:
                seed = Murmur3HashFunction.hash(field, BinaryType, seed);
                break;
            case SMALLINT:
                seed = Murmur3HashFunction.hash(field, ShortType, seed);
                break;
            case FLOAT:
                seed = Murmur3HashFunction.hash(field, FloatType, seed);
                break;
            case DOUBLE:
                seed = Murmur3HashFunction.hash(field, DoubleType, seed);
                break;
            case BOOLEAN:
                seed = Murmur3HashFunction.hash(field, BooleanType, seed);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                DecimalData decimalData = (DecimalData) field;
                seed =
                        Murmur3HashFunction.hash(Decimal.apply(decimalData.toBigDecimal(), decimalType.getPrecision(),
                                        decimalType.getScale()),
                                new org.apache.spark.sql.types.DecimalType(decimalType.getPrecision(),
                                        decimalType.getScale()), seed);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampData timestampData = (TimestampData) field;
                long value = timestampData.getMillisecond() * 1000 + timestampData.getNanoOfMillisecond() / 1000;
                seed = Murmur3HashFunction.hash(value, LongType, seed);
                break;
            default:
                throw new RuntimeException("not support this partition type now :" + type.getTypeRoot().toString());
        }
        return seed;
    }
}
