package com.dmetasoul.lakesoul.lakesoul;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

public class LakeSoulArrowUtils {
    public static Schema cdcColumnAlignment(Schema schema, String cdcColumn) {
        if (cdcColumn != null) {
            // set cdc column as the last field
            Field cdcField = null;
            List<Field> fields = new ArrayList<>(schema.getFields().size() + 1);
            for (Field field : schema.getFields()) {
                if (!field.getName().equals(cdcColumn)) {
                    fields.add(field);
                } else {
                    cdcField = field;
                }
            }
            if (cdcField != null) {
                fields.add(cdcField);
            } else {
                throw new RuntimeException(String.format("Invalid Schema of %s, CDC Column [%s] not found", schema, cdcColumn));
//                fields.add(new Field(cdcColumn, FieldType.notNullable(new ArrowType.Utf8()), null));
            }
            return new Schema(fields);
        }
        return schema;
    }
}
