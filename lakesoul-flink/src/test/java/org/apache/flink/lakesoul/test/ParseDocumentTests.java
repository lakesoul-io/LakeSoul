// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import static org.apache.flink.lakesoul.types.ParseDocument.convertBSONToStruct;

public class ParseDocumentTests {

    @Test
    public void testParseDocument() {
        String bsonValue = "{\"_id\": \"a1\", \"int_col\": 112980, \"double_col\": 123.21, \"decimal_col\": {\"$numberDecimal\": \"123.123\"}, \"string_col\": \"hello\", \"bin_col\": {\"$binary\": \"SGVsbG8sIE1vbmdvREIh\", \"$type\": \"00\"}, \"arr1\": [1, 2, 3, 4], \"arr2\": [12.12, 34.3], \"arr3\": [\"hello\", \"word\"], \"bool_col\": true, \"date_col\": {\"$date\": 1711078115342}, \"date_col1\": \"Fri Mar 22 2024 03:28:35 GMT+0000 (Coordinated Universal Time)\", \"struct_col\": {\"name\": \"linda\", \"age\": 12, \"male\": true, \"fumu\": {\"mama\": \"ldsm\", \"baba\": \"ldsb\"}}, \"long_col\": {\"$numberLong\": \"1234567890123456789\"}}";
        Struct struct = convertBSONToStruct(bsonValue);
        Assert.assertEquals(struct.getString("_id"), "a1");
        Assert.assertEquals(struct.get("int_col"), 112980);
        Assert.assertEquals(struct.get("double_col"), 123.21);
        Assert.assertEquals(struct.get("decimal_col"), BigDecimal.valueOf(123.123));
        Assert.assertEquals(struct.get("string_col"), "hello");

        byte[] expectedBytes = Base64.getDecoder().decode("SGVsbG8sIE1vbmdvREIh");
        Assert.assertArrayEquals((byte[]) struct.get("bin_col"), expectedBytes);
        List<Integer> arr1 = (List<Integer>) struct.get("arr1");
        Assert.assertEquals(arr1.size(), 4);
        Assert.assertTrue(arr1.containsAll(Arrays.asList(1, 2, 3, 4)));
        Date expectedDate = new Date(1711078115342L);
        Assert.assertEquals(struct.get("date_col"), expectedDate);
        Assert.assertEquals(struct.get("bool_col"), true);
        Assert.assertEquals(struct.get("long_col"), 1234567890123456789L);
    }
}