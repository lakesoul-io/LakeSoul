// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Jackson based JSON helpers shared by the LakeSoul integrations. */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private JsonUtils() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static ObjectNode createObjectNode() {
        return MAPPER.createObjectNode();
    }

    public static ObjectNode objectNode(Map<String, ?> values) {
        ObjectNode node = createObjectNode();
        if (values != null) {
            values.forEach((key, value) -> put(node, key, value));
        }
        return node;
    }

    public static ArrayNode createArrayNode() {
        return MAPPER.createArrayNode();
    }

    public static JsonNode parse(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON", e);
        }
    }

    public static ObjectNode parseObject(String json) {
        JsonNode node = parse(json);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException("Expected a JSON object");
        }
        return (ObjectNode) node;
    }

    public static ArrayNode parseArray(String json) {
        JsonNode node = parse(json);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isArray()) {
            throw new IllegalArgumentException("Expected a JSON array");
        }
        return (ArrayNode) node;
    }

    public static <T> T parseObject(String json, Class<T> type) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON as " + type.getName(), e);
        }
    }

    public static <T> List<T> parseArray(String json, Class<T> type) {
        if (json == null || json.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            return MAPPER.readValue(
                    json, MAPPER.getTypeFactory().constructCollectionType(List.class, type));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to parse JSON array as " + type.getName(), e);
        }
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize value as JSON", e);
        }
    }

    public static void put(ObjectNode node, String key, Object value) {
        if (value == null) {
            node.putNull(key);
        } else {
            node.set(key, MAPPER.valueToTree(value));
        }
    }

    /** Returns the text of a scalar node, or the JSON text of an object or array. */
    public static String asString(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        return node.isContainerNode() ? node.toString() : node.asText();
    }
}
