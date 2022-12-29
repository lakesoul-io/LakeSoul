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

package com.dmetasoul.lakesoul.meta.external.jdbc;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.time.*;
import java.time.temporal.TemporalAdjuster;

import static org.apache.spark.sql.types.DataTypes.*;

public class JdbcDataTypeConverter {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ZoneOffset defaultOffset;

    /**
     * Fallback value for TIMESTAMP WITH TZ is epoch
     */
    private final String fallbackTimestampWithTimeZone;

    /**
     * Fallback value for TIME WITH TZ is 00:00
     */
    private final String fallbackTimeWithTimeZone;
    protected final boolean adaptiveTimePrecisionMode;
    protected final boolean adaptiveTimeMicrosecondsPrecisionMode;
    protected final JdbcValueConverters.DecimalMode decimalMode;
    private final TemporalAdjuster adjuster;
    protected final JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode;
    protected final CommonConnectorConfig.BinaryHandlingMode binaryMode;

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values without timezone
     * information
     * to values that require timezones, and uses adapts time and timestamp values based upon the precision of the
     * database
     * columns.
     */
    public JdbcDataTypeConverter() {
        this(null, TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null, null);
    }

    /**
     * Create a new instance, and specify the time zone offset that should be used only when converting values
     * without timezone
     * information to values that require timezones. This default offset should not be needed when values are
     * highly-correlated
     * with the expected SQL/JDBC types.
     *
     * @param decimalMode           how {@code DECIMAL} and {@code NUMERIC} values should be treated; may be null if
     *                              {@link JdbcValueConverters.DecimalMode#PRECISE} is to be used
     * @param temporalPrecisionMode temporal precision mode based on {@link io.debezium.jdbc.TemporalPrecisionMode}
     * @param defaultOffset         the zone offset that is to be used when converting non-timezone related values to
     *                             values that do
     *                              have timezones; may be null if UTC is to be used
     * @param adjuster              the optional component that adjusts the local date value before obtaining the
     *                              epoch day; may be null if no
     *                              adjustment is necessary
     * @param bigIntUnsignedMode    how {@code BIGINT UNSIGNED} values should be treated; may be null if
     *                              {@link JdbcValueConverters.BigIntUnsignedMode#PRECISE} is to be used
     * @param binaryMode            how binary columns should be represented
     */
    public JdbcDataTypeConverter(JdbcValueConverters.DecimalMode decimalMode,
                                 TemporalPrecisionMode temporalPrecisionMode, ZoneOffset defaultOffset,
                                 TemporalAdjuster adjuster, JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode
            , CommonConnectorConfig.BinaryHandlingMode binaryMode) {
        this.defaultOffset = defaultOffset != null ? defaultOffset : ZoneOffset.UTC;
        this.adaptiveTimePrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE);
        this.adaptiveTimeMicrosecondsPrecisionMode =
                temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        this.decimalMode = decimalMode != null ? decimalMode : JdbcValueConverters.DecimalMode.PRECISE;
        this.adjuster = adjuster;
        this.bigIntUnsignedMode = bigIntUnsignedMode != null ? bigIntUnsignedMode :
                                  JdbcValueConverters.BigIntUnsignedMode.PRECISE;
        this.binaryMode = binaryMode != null ? binaryMode : CommonConnectorConfig.BinaryHandlingMode.BYTES;

        this.fallbackTimestampWithTimeZone = ZonedTimestamp.toIsoString(
                OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.MIDNIGHT, defaultOffset),
                defaultOffset,
                adjuster);
        this.fallbackTimeWithTimeZone = ZonedTime.toIsoString(
                OffsetTime.of(LocalTime.MIDNIGHT, defaultOffset),
                defaultOffset,
                adjuster);
    }


    public DataType schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            case Types.NULL:
                logger.warn("Unexpected JDBC type: NULL");
                return NullType;

            // Single- and multi-bit values ...
            case Types.BIT:
                if (column.length() > 1) {
                    return BinaryType;
                }
                // otherwise, it is just one bit so use a boolean ...
            case Types.BOOLEAN:
                return BooleanType;

            // Fixed-length binary values ...
            case Types.BLOB:
            case Types.BINARY:
                return BinaryType;

            // Variable-length binary values ...
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return BinaryType;

            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255
                return IntegerType;
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                return IntegerType;
            case Types.INTEGER:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                return IntegerType;
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                return LongType;

            // Numeric decimal numbers
            case Types.REAL:
                // values are single precision floating point number which supports 7 digits of mantissa.
                return FloatType;
            case Types.FLOAT:
            case Types.DOUBLE:
                // values are double precision floating point number which supports 15 digits of mantissa.
                return DoubleType;
            case Types.NUMERIC:
            case Types.DECIMAL:
                switch (decimalMode) {
                    case DOUBLE:
                        return DoubleType;
                    case PRECISE:
                        return new DecimalType(column.length(), column.scale().get());
                    case STRING:
                        return StringType;
                    default:
                        throw new IllegalArgumentException("Unknown decimalMode");
                }

                // Fixed-length string values
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
                return StringType;

            // Variable-length string values
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.DATALINK:
                return StringType;
            case Types.SQLXML:
                return StringType;
            // Date and time values
            case Types.DATE:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return DateType;
                }
                return IntegerType;
            case Types.TIME:
                // CalendarIntervalType is not supported by org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter
//                if (adaptiveTimeMicrosecondsPrecisionMode) {
//                    return CalendarIntervalType;
//                }
//                if (adaptiveTimePrecisionMode) {
//                    if (getTimePrecision(column) <= 3) {
//                        return CalendarIntervalType;
//                    }
//                    if (getTimePrecision(column) <= 6) {
//                        return CalendarIntervalType;
//                    }
//                    return CalendarIntervalType;
//                }
                return IntegerType;
            case Types.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return TimestampType;
                    }
                    if (getTimePrecision(column) <= 6) {
                        return TimestampType;
                    }
                    return TimestampType;
                }
                return TimestampType;
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return TimestampType;

            // Other types ...
            case Types.ROWID:
                // often treated as a string, but we'll generalize and treat it as a byte array
                return ByteType;

            // Unhandled types
            case Types.DISTINCT:
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.STRUCT:
            default:
                break;
        }
        return null;
    }

    protected int getTimePrecision(Column column) {
        return column.length();
    }
}
