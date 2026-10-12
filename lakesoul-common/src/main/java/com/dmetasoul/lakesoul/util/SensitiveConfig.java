// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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

    /**
     * Returns {@code value} with credentials removed for diagnostics. Sensitive option keys are
     * replaced entirely; other values stay as they are unless they are URI-shaped and carry
     * credentials ({@code user:password@host}) or sensitive query parameters, including
     * AWS/GCS/Azure signatures. The original value is never modified, so it stays usable for
     * authentication.
     */
    public static String redact(String key, String value) {
        if (value == null) {
            return null;
        }
        return isSensitive(key) ? REDACTED : redactUri(value);
    }

    /** Redacts URI userinfo and query credentials while preserving public components verbatim. */
    private static String redactUri(String value) {
        int scheme = value.indexOf("://");
        if (scheme < 0) {
            return value;
        }
        int authorityStart = scheme + 3;
        int authorityEnd = authorityStart;
        while (authorityEnd < value.length() && "/?#".indexOf(value.charAt(authorityEnd)) < 0) {
            authorityEnd++;
        }

        StringBuilder redacted = null;
        int copied = 0;
        int at = value.lastIndexOf('@', authorityEnd - 1);
        if (at >= authorityStart) {
            redacted = new StringBuilder(value.length());
            redacted.append(value, 0, authorityStart).append(REDACTED);
            copied = at;
        }

        int fragment = value.indexOf('#', authorityEnd);
        int limit = fragment < 0 ? value.length() : fragment;
        int query = value.indexOf('?', authorityEnd);
        if (query >= 0 && query < limit) {
            int start = query + 1;
            while (start < limit) {
                int amp = value.indexOf('&', start);
                int end = amp < 0 || amp > limit ? limit : amp;
                int equals = value.indexOf('=', start);
                if (equals >= start
                        && equals + 1 < end
                        && isSensitiveQueryName(value.substring(start, equals))) {
                    if (redacted == null) {
                        redacted = new StringBuilder(value.length());
                    }
                    redacted.append(value, copied, equals + 1).append(REDACTED);
                    copied = end;
                }
                start = end + 1;
            }
        }
        return redacted == null ? value : redacted.append(value, copied, value.length()).toString();
    }

    private static boolean isSensitiveQueryName(String name) {
        try {
            String decoded = name.indexOf('%') < 0 ? name : URLDecoder.decode(name, "UTF-8");
            return decoded.equalsIgnoreCase("X-Amz-Signature")
                    || decoded.equalsIgnoreCase("X-Goog-Signature")
                    || decoded.equalsIgnoreCase("Signature")
                    || decoded.equalsIgnoreCase("sig")
                    || isSensitive(decoded);
        } catch (IllegalArgumentException malformedEscape) {
            // An undecodable query name cannot safely be classified as public.
            return true;
        } catch (UnsupportedEncodingException impossible) {
            throw new AssertionError(impossible);
        }
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
