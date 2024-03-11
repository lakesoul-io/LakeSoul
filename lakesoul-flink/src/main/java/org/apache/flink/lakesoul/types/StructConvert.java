// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import java.util.List;
public class StructConvert {
    public RowData convert(Struct struct) {
        // 获取Struct的schema和字段个数
        Schema schema = struct.schema();
        int arity = schema.fields().size() + 1;  // +1 for extra event sortField
        List<Field> fieldNames = schema.fields();
        // 创建BinaryRowData
        BinaryRowData row = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        // 遍历Struct的字段
        for (int i = 0; i < schema.fields().size(); i++) {
            Field field = fieldNames.get(i);
            String fieldName = field.name();
            Schema fieldSchema = schema.field(fieldName).schema();
            // 获取Struct字段的值
            Object fieldValue = struct.getWithoutDefault(fieldName);
            // 将字段值写入BinaryRowData
            writeField(writer, i, fieldSchema, fieldValue);
        }
        // 关闭BinaryRowWriter
        writer.complete();
        return row;
    }

    public RowData convert1(Struct struct){
        Schema schema = struct.schema();
        int arity = schema.fields().size()+1;
        List<Field> fieldNames = schema.fields();
        BinaryRowData row = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        Field field = fieldNames.get(0);
        String fieldName = field.name();;
        Schema fieldSchema = schema.field(fieldName).schema();
        Object fieldValue = struct.getWithoutDefault(fieldName);
        RowDataSerializer rowDataSerializer = new RowDataSerializer();
        writer.writeRow(0,(RowData) fieldValue ,rowDataSerializer);
        writer.complete();
        return row;
    }
    // 递归处理嵌套的Struct类型
    private void writeField(BinaryRowWriter writer, int index, Schema schema, Object fieldValue) {

        if (fieldValue == null) {
            // 如果字段值为null，则写入null
            writer.setNullAt(index);
        } else if (schema.type().getName().equals("struct")) {
            // 如果字段类型是StructType，递归处理
            convertNestedStruct(writer, index, (Struct) fieldValue, schema);
        } else {
            // 根据字段类型写入值
            // 这里根据实际情况，可能需要根据字段类型进行不同的处理
            switch (schema.type()) {
                case INT8:
                case INT16:
                case INT32:
                    writer.writeInt(index, (Integer) fieldValue);
                    break;
                case STRING:
                    writer.writeString(index, StringData.fromString(fieldValue.toString()));
                    break;
                default:
                    writer.writeString(index, StringData.fromString(fieldValue.toString()));
            }

        }
    }
    // 递归处理嵌套的Struct类型
    private void convertNestedStruct(BinaryRowWriter writer, int index, Struct nestedStruct, Schema nestedSchema) {
        // 获取嵌套Struct的schema和字段个数
        int nestedArity = nestedSchema.fields().size();
        List<Field> nestedFields = nestedSchema.fields();
        // 创建嵌套Struct的BinaryRowData
        BinaryRowData nestedRow = new BinaryRowData(nestedArity);
        BinaryRowWriter nestedWriter = new BinaryRowWriter(nestedRow);
        // 遍历嵌套Struct的字段
        for (int i = 0; i < nestedArity; i++) {
            Field field = nestedFields.get(i);
            String nestedFieldName = field.name();
            Schema nestedFieldType = nestedSchema.field(nestedFieldName).schema();
            // 获取嵌套Struct字段的值
            Object nestedFieldValue = nestedStruct.getWithoutDefault(nestedFieldName);
            // 将嵌套Struct字段值写入BinaryRowData
            writeField(nestedWriter, i, nestedFieldType, nestedFieldValue);
        }
        // 关闭嵌套Struct的BinaryRowWriter
        nestedWriter.complete();
        RowDataSerializer rowDataSerializer = new RowDataSerializer();
        writer.writeRow(index, nestedRow, rowDataSerializer);
        writer.complete();
    }
}