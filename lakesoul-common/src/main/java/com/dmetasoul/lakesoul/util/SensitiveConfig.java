// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.util;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Redacts configuration values for diagnostics, without changing authentication options. */
public final class SensitiveConfig {
    public static final String REDACTED = "[REDACTED]";

    private SensitiveConfig() {}

    public static boolean isSensitive(String key) {
        if (key == null) {
            return false;
        }
        // Hadoop, Flink, Spark and environment variables use different key separators.
        String normalized =
                key.toLowerCase(Locale.ROOT).replace(".", "").replace("-", "").replace("_", "");
        return normalized.contains("accesskey")
                || normalized.contains("secret")
                || normalized.contains("password")
                || normalized.contains("token")
                || normalized.contains("apikey")
                || normalized.contains("privatekey");
    }

    public static String redact(String key, String value) {
        return value != null && isSensitive(key) ? REDACTED : value;
    }

    /** Returns a redacted copy for logging. The original map must still be used for IO. */
    public static Map<String, String> redact(Map<String, String> options) {
        if (options == null) {
            return null;
        }
        Map<String, String> redacted = new LinkedHashMap<>();
        options.forEach((key, value) -> redacted.put(key, redact(key, value)));
        return redacted;
    }
}
