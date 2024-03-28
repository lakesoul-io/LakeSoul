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
        Schema schema = struct.schema();
        int arity = schema.fields().size() + 1;  // +1 for extra event sortField
        List<Field> fieldNames = schema.fields();
        BinaryRowData row = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < schema.fields().size(); i++) {
            Field field = fieldNames.get(i);
            String fieldName = field.name();
            Schema fieldSchema = schema.field(fieldName).schema();
            Object fieldValue = struct.getWithoutDefault(fieldName);
            writeField(writer, i, fieldSchema, fieldValue);
        }
        writer.complete();
        return row;
    }

    private void writeField(BinaryRowWriter writer, int index, Schema schema, Object fieldValue) {

        if (fieldValue == null) {
            writer.setNullAt(index);
        } else if (schema.type().getName().equals("struct")) {
            convertNestedStruct(writer, index, (Struct) fieldValue, schema);
        } else {
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

    private void convertNestedStruct(BinaryRowWriter writer, int index, Struct nestedStruct, Schema nestedSchema) {
        int nestedArity = nestedSchema.fields().size();
        List<Field> nestedFields = nestedSchema.fields();
        BinaryRowData nestedRow = new BinaryRowData(nestedArity);
        BinaryRowWriter nestedWriter = new BinaryRowWriter(nestedRow);
        for (int i = 0; i < nestedArity; i++) {
            Field field = nestedFields.get(i);
            String nestedFieldName = field.name();
            Schema nestedFieldType = nestedSchema.field(nestedFieldName).schema();
            Object nestedFieldValue = nestedStruct.getWithoutDefault(nestedFieldName);
            writeField(nestedWriter, i, nestedFieldType, nestedFieldValue);
        }
        nestedWriter.complete();
        RowDataSerializer rowDataSerializer = new RowDataSerializer();
        writer.writeRow(index, nestedRow, rowDataSerializer);
        writer.complete();
    }
}