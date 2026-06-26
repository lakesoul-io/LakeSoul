// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.table.runtime.arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulArrowUtilsTest {

    @Test
    public void largeListUsesFlinkArrayTypeAndRegularExecutionSchema() {
        Field element = new Field(
                "item",
                FieldType.nullable(ArrowType.LargeUtf8.INSTANCE),
                Collections.emptyList());
        Field list = new Field(
                "items",
                FieldType.nullable(ArrowType.LargeList.INSTANCE),
                Collections.singletonList(element));

        RowType rowType = ArrowUtils.fromArrowSchema(
                new Schema(Collections.singletonList(list)));

        assertThat(rowType.getTypeAt(0)).isInstanceOf(ArrayType.class);
        assertThat(((ArrayType) rowType.getTypeAt(0)).getElementType())
                .isInstanceOf(VarCharType.class);
        Schema executionSchema = ArrowUtils.toArrowSchema(rowType);
        assertThat(executionSchema.findField("items").getType())
                .isEqualTo(ArrowType.List.INSTANCE);
        assertThat(executionSchema.findField("items").getChildren().get(0).getType())
                .isEqualTo(ArrowType.Utf8.INSTANCE);
    }
}
