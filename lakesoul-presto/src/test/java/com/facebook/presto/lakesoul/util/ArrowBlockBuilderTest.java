// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ArrowBlockBuilderTest {

    @Test
    public void testLargeListTypeAndExecutionField() {
        Field element = new Field(
                "item",
                FieldType.nullable(ArrowType.LargeUtf8.INSTANCE),
                Collections.emptyList());
        Field list = new Field(
                "items",
                FieldType.nullable(ArrowType.LargeList.INSTANCE),
                Collections.singletonList(element));
        ArrowBlockBuilder builder = new ArrowBlockBuilder(
                FunctionAndTypeManager.createTestFunctionAndTypeManager());

        assertTrue(builder.getPrestoTypeFromArrowField(list) instanceof ArrayType);
        assertEquals(
                ((ArrayType) builder.getPrestoTypeFromArrowField(list)).getElementType(),
                VarcharType.VARCHAR);

        Field execution = ArrowBlockBuilder.toExecutionField(list);
        assertEquals(execution.getType(), ArrowType.List.INSTANCE);
        assertEquals(execution.getChildren().get(0).getType(), ArrowType.Utf8.INSTANCE);
    }
}
