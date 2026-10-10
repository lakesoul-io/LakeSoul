// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.util;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class SensitiveConfigTest {
    @Test
    public void redactsCredentialAliases() {
        for (String key :
                new String[] {
                    "fs.s3a.access.key",
                    "fs.s3a.secret.key",
                    "s3.access-key",
                    "s3.secret-key",
                    "spark.hadoop.fs.s3a.access.key",
                    "spark.hadoop.fs.s3a.secret.key",
                    "AWS_ACCESS_KEY_ID",
                    "AWS_SECRET_ACCESS_KEY",
                    "AWS_SESSION_TOKEN",
                    "fs.s3a.session.token",
                    "lakesoul.pg.password",
                    "api-key",
                    "private_key"
                }) {
            assertTrue(key, SensitiveConfig.isSensitive(key));
            assertEquals(key, SensitiveConfig.REDACTED, SensitiveConfig.redact(key, "sentinel"));
        }
    }

    @Test
    public void matchingIsCaseInsensitiveAndLocaleIndependent() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            assertEquals(
                    SensitiveConfig.REDACTED,
                    SensitiveConfig.redact("AWS_PRIVATE_KEY", "private-key-sentinel"));
            assertEquals(
                    SensitiveConfig.REDACTED,
                    SensitiveConfig.redact("FS.S3A.ACCESS.KEY", "access-key-sentinel"));
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void redactedCopyPreservesTheOriginalValuesAndPublicOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("fs.s3a.access.key", "access-key-sentinel");
        options.put("s3.secret-key", "secret-key-sentinel");
        options.put("fs.s3a.endpoint", "http://localhost:9000");
        options.put("s3.bucket", "test-bucket");
        Map<String, String> original = new LinkedHashMap<>(options);

        Map<String, String> redacted = SensitiveConfig.redact(options);
        assertNotSame(options, redacted);
        assertEquals(original, options);
        assertEquals(SensitiveConfig.REDACTED, redacted.get("fs.s3a.access.key"));
        assertEquals(SensitiveConfig.REDACTED, redacted.get("s3.secret-key"));
        assertEquals("http://localhost:9000", redacted.get("fs.s3a.endpoint"));
        assertEquals("test-bucket", redacted.get("s3.bucket"));
        assertFalse(redacted.toString().contains("access-key-sentinel"));
        assertFalse(redacted.toString().contains("secret-key-sentinel"));
    }

    @Test
    public void handlesMissingValuesAndEmptyMaps() {
        assertNull(SensitiveConfig.redact("password", null));
        assertEquals("public", SensitiveConfig.redact(null, "public"));
        assertNull(SensitiveConfig.redact((Map<String, String>) null));
        assertTrue(SensitiveConfig.redact(new LinkedHashMap<>()).isEmpty());
    }
}
