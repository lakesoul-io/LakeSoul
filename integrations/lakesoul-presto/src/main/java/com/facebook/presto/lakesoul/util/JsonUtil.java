// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static <T> T parse(String str, Class<T> cls) {
        try {
            return MAPPER.readValue(str, cls);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON as " + cls.getName(), e);
        }
    }

    public static ObjectNode parseObject(String str) {
        try {
            JsonNode node = MAPPER.readTree(str);
            if (node == null || node.isNull()) {
                return null;
            }
            if (!node.isObject()) {
                throw new IllegalArgumentException("Expected a JSON object");
            }
            return (ObjectNode) node;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON object", e);
        }
    }

    public static <T> String format(T object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize value as JSON", e);
        }
    }
}
