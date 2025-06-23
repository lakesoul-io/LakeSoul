// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import static java.util.Objects.requireNonNull;

public class LakeSoulTableColumnHandle implements ColumnHandle {
    private LakeSoulTableHandle tableHandle;
    private String columnName;
    private Type columnType;
    private final Field arrowField ;

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
    private static final ObjectReader reader = mapper.readerFor(Field.class);

    @JsonCreator
    public LakeSoulTableColumnHandle(
            @JsonProperty("tableHandle") LakeSoulTableHandle tableHandle,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("arrowField") String arrowField) {
        this(tableHandle, columnName, columnType, readFieldJson(arrowField));
    }

    public LakeSoulTableColumnHandle(
            LakeSoulTableHandle tableHandle,
            String columnName,
            Type columnType,
            Field arrowField) {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle should not be null") ;
        this.columnName = requireNonNull(columnName, "columnName should not be null") ;
        this.columnType = requireNonNull(columnType, "columnType should not be null") ;
        this.arrowField = requireNonNull(arrowField, "arrowField should not be null") ;
    }

    @JsonProperty
    public LakeSoulTableHandle getTableHandle() {
        return tableHandle;
    }

    public void setTableHandle(LakeSoulTableHandle tableHandle) {
        this.tableHandle = tableHandle;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    public void setColumnType(Type columnType) {
        this.columnType = columnType;
    }

    @JsonIgnore
    public Field getArrowField() throws JsonProcessingException {
        return arrowField;
    }

    @JsonProperty("arrowField")
    public String getArrowFieldJson() throws JsonProcessingException {
        return writer.writeValueAsString(arrowField);
    }

    private static Field readFieldJson(String s) {
        try {
            return reader.readValue(s);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override public String toString() {
        return "LakeSoulTableColumnHandle{" +
                "tableHandle=" + tableHandle +
                ", columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                '}';
    }
}

