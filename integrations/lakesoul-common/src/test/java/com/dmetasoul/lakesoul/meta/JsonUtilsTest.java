// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.jnr.SplitDesc;
import com.dmetasoul.lakesoul.meta.security.Claims;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonUtilsTest {

    @Test
    public void claimsKeepTheirNativeFieldNames() {
        Claims claims = new Claims();
        claims.setSub("alice");
        claims.setGroup("analysts");
        claims.setExp(1234L);

        String json = JsonUtils.toJson(claims);
        Claims parsed = JsonUtils.parseObject(json, Claims.class);

        assertTrue(json, json.contains("\"sub\":\"alice\""));
        assertTrue(json, json.contains("\"group\":\"analysts\""));
        assertTrue(json, json.contains("\"exp\":1234"));
        assertEquals("alice", parsed.getSub());
        assertEquals("analysts", parsed.getGroup());
        assertEquals(1234L, parsed.getExp());
    }

    @Test
    public void splitDescUsesSnakeCaseFieldsAndIgnoresUnknownFields() {
        String json =
                "[{\"file_paths\":[\"s3://bucket/file\"],\"primary_keys\":[\"id\"],"
                        + "\"partition_desc\":{\"dt\":\"2026-01-01\"},"
                        + "\"table_schema\":\"schema\",\"new_field\":\"ignored\"}]";

        List<SplitDesc> splitDescs = JsonUtils.parseArray(json, SplitDesc.class);

        assertEquals(1, splitDescs.size());
        assertEquals("s3://bucket/file", splitDescs.get(0).getFilePaths().get(0));
        assertEquals("id", splitDescs.get(0).getPrimaryKeys().get(0));
        assertEquals("2026-01-01", splitDescs.get(0).getPartitionDesc().get("dt"));
        assertEquals("schema", splitDescs.get(0).getTableSchema());
    }

    @Test
    public void objectValuesKeepScalarTextAndContainerJson() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("hashBucketNum", "4");
        values.put("domain", "public");

        ObjectNode node = DBUtil.stringMapToJson(values);
        node.putObject("nested").put("enabled", true);

        Map<String, String> parsed = DBUtil.jsonToStringMap(node);

        assertEquals("4", parsed.get("hashBucketNum"));
        assertEquals("public", parsed.get("domain"));
        assertTrue(parsed.get("nested"), parsed.get("nested").contains("\"enabled\":true"));
    }

    @Test
    public void binaryValuesAreSerializedAsBase64() {
        ObjectNode node = JsonUtils.createObjectNode();

        JsonUtils.put(node, "bytes", new byte[] {1, 2, 3});

        assertEquals("AQID", node.path("bytes").asText());
    }
}
