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

package com.dmetasoul.lakesoul.meta.external.mysql;

import com.dmetasoul.lakesoul.meta.external.jdbc.JdbcDataTypeConverter;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Column;
import io.debezium.util.Strings;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;

import java.util.List;

import static java.util.regex.Pattern.matches;
import static org.apache.spark.sql.types.DataTypes.*;

public class MysqlDataTypeConverter extends JdbcDataTypeConverter {

    public MysqlDataTypeConverter() {
        super();
    }

    public DataType schemaBuilder(Column column) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return StringType;
        }
        if (matches(typeName, "POINT")) {
            return StringType;
        }
        if (matches(typeName, "GEOMETRY")
                || matches(typeName, "LINESTRING")
                || matches(typeName, "POLYGON")
                || matches(typeName, "MULTIPOINT")
                || matches(typeName, "MULTILINESTRING")
                || matches(typeName, "MULTIPOLYGON")
                || isGeometryCollection(typeName)) {
            return StringType;
        }
        if (matches(typeName, "YEAR")) {
            return IntegerType;
        }
        if (matches(typeName, "ENUM")) {
            return StringType;
        }
        if (matches(typeName, "SET")) {
            return StringType;
        }
        if (matches(typeName, "SMALLINT UNSIGNED") || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT2 UNSIGNED") || matches(typeName, "INT2 UNSIGNED ZEROFILL")) {
            // In order to capture unsigned SMALLINT 16-bit data source, INT32 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return IntegerType;
        }
        if (matches(typeName, "INT UNSIGNED") || matches(typeName, "INT UNSIGNED ZEROFILL")
                || matches(typeName, "INT4 UNSIGNED") || matches(typeName, "INT4 UNSIGNED ZEROFILL")) {
            // In order to capture unsigned INT 32-bit data source, INT64 will be required to safely capture all valid values
            // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return LongType;
        }
        if (matches(typeName, "BIGINT UNSIGNED") || matches(typeName, "BIGINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT8 UNSIGNED") || matches(typeName, "INT8 UNSIGNED ZEROFILL")) {
            switch (super.bigIntUnsignedMode) {
                case LONG:
                    return LongType;
                case PRECISE:
                    // In order to capture unsigned INT 64-bit data source, org.apache.kafka.connect.data.Decimal:Byte will be required to safely capture all valid values with scale of 0
                    // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
                    return new DecimalType(column.length(),0);
            }
        }
        if (matches(typeName, "FLOAT") && column.scale().isPresent() && column.length() <= 24) {
            return FloatType;
        }
        // Otherwise, let the base class handle it ...
        return super.schemaBuilder(column);
    }

    /**
     * Determine if the uppercase form of a column's type is geometry collection independent of JDBC driver or server version.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @return {@code true} if the type is geometry collection
     */
    protected boolean isGeometryCollection(String upperCaseTypeName) {
        if (upperCaseTypeName == null) {
            return false;
        }

        return upperCaseTypeName.equals("GEOMETRYCOLLECTION") || upperCaseTypeName.equals("GEOMCOLLECTION")
                || upperCaseTypeName.endsWith(".GEOMCOLLECTION");
    }

    protected List<String> extractEnumAndSetOptions(Column column) {
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues());
    }

    protected String extractEnumAndSetOptionsAsString(Column column) {
        return Strings.join(",", extractEnumAndSetOptions(column));
    }

}
