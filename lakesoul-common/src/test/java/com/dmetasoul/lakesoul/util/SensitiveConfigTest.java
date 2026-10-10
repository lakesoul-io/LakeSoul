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
    public void redactsSensitiveQueryParametersOfPostgresJdbcUrls() {
        String url =
                "jdbc:postgresql://db.example.com:5432/lakesoul"
                        + "?user=lakesoul&password=query-secret&sslmode=require";
        String redacted = SensitiveConfig.redact("lakesoul.pg.url", url);
        assertFalse(redacted, redacted.contains("query-secret"));
        assertTrue(redacted, redacted.contains("jdbc:postgresql://db.example.com:5432/lakesoul"));
        assertTrue(redacted, redacted.contains("user=lakesoul"));
        assertTrue(redacted, redacted.contains("password=" + SensitiveConfig.REDACTED));
        assertTrue(redacted, redacted.contains("sslmode=require"));
    }

    @Test
    public void redactsUserInfoButKeepsTheHost() {
        for (String userInfo : new String[] {"lakesoul:userinfo-secret", "userinfo-token"}) {
            String url = "postgresql://" + userInfo + "@db.example.com:5432/lakesoul";
            assertEquals(
                    "postgresql://" + SensitiveConfig.REDACTED + "@db.example.com:5432/lakesoul",
                    SensitiveConfig.redact("lakesoul.pg.url", url));
        }
    }

    @Test
    public void redactsPercentEncodedSensitiveQueryNames() {
        String url =
                "jdbc:postgresql://db.example.com/lakesoul?pass%77ord=encoded-secret&user=lakesoul";
        String redacted = SensitiveConfig.redact("lakesoul.pg.url", url);
        assertFalse(redacted, redacted.contains("encoded-secret"));
        assertTrue(redacted, redacted.contains("pass%77ord=" + SensitiveConfig.REDACTED));
        assertTrue(redacted, redacted.contains("user=lakesoul"));
    }

    @Test
    public void redactsEveryRepeatedSensitiveQueryParameter() {
        String url =
                "jdbc:postgresql://db.example.com/lakesoul"
                        + "?password=first-secret&user=lakesoul&password=second-secret";
        String redacted = SensitiveConfig.redact("lakesoul.pg.url", url);
        assertFalse(redacted, redacted.contains("first-secret"));
        assertFalse(redacted, redacted.contains("second-secret"));
        assertTrue(redacted, redacted.contains("user=lakesoul"));
        assertEquals(
                "jdbc:postgresql://db.example.com/lakesoul?password="
                        + SensitiveConfig.REDACTED
                        + "&user=lakesoul&password="
                        + SensitiveConfig.REDACTED,
                redacted);
    }

    @Test
    public void redactsUrlsWithoutAPathAndKeepsTheFragment() {
        String url = "jdbc:postgresql://db.example.com?password=nopath-secret#lakesoul";
        String redacted = SensitiveConfig.redact("lakesoul.pg.url", url);
        assertFalse(redacted, redacted.contains("nopath-secret"));
        assertTrue(redacted, redacted.contains("jdbc:postgresql://db.example.com"));
        assertTrue(redacted, redacted.contains("password=" + SensitiveConfig.REDACTED));
        assertTrue(redacted, redacted.endsWith("#lakesoul"));
    }

    @Test
    public void preservesPublicAndEmptyQueryValues() {
        String publicUrl =
                "jdbc:postgresql://db.example.com:5432/lakesoul?user=lakesoul&sslmode=require";
        assertEquals(publicUrl, SensitiveConfig.redact("lakesoul.pg.url", publicUrl));
        String emptySecret = "jdbc:postgresql://db.example.com/lakesoul?password=";
        assertEquals(emptySecret, SensitiveConfig.redact("lakesoul.pg.url", emptySecret));
        String fragment = "postgresql://db/lakesoul#fragment?password=not-a-query";
        assertEquals(fragment, SensitiveConfig.redact("lakesoul.pg.url", fragment));
        assertEquals("plain-value", SensitiveConfig.redact("s3.bucket", "plain-value"));
        assertEquals(
                "http://localhost:9000",
                SensitiveConfig.redact("fs.s3a.endpoint", "http://localhost:9000"));
    }

    @Test
    public void redactedCopyKeepsUrlCredentialsOutAndTheOriginalMapIntact() {
        String url =
                "jdbc:postgresql://lakesoul:uri-secret@db.example.com:5432/lakesoul"
                        + "?password=query-secret&user=lakesoul";
        Map<String, String> options = new LinkedHashMap<>();
        options.put("lakesoul.pg.url", url);
        options.put("s3.bucket", "test-bucket");

        Map<String, String> redacted = SensitiveConfig.redact(options);
        assertEquals(url, options.get("lakesoul.pg.url"));
        assertEquals("test-bucket", redacted.get("s3.bucket"));
        assertFalse(redacted.toString(), redacted.get("lakesoul.pg.url").contains("uri-secret"));
        assertFalse(redacted.toString(), redacted.get("lakesoul.pg.url").contains("query-secret"));
        assertTrue(redacted.get("lakesoul.pg.url").contains("db.example.com:5432/lakesoul"));
    }

    @Test
    public void redactsProviderSignaturesWithoutChangingOriginalOptions() {
        for (String name :
                new String[] {"X-Amz-Signature", "X-Goog-Signature", "Signature", "sig"}) {
            String uri =
                    "https://host/blob?"
                            + name
                            + "=signature-sentinel&signatureAlgorithm=SHA256&signal=keep#fragment";
            Map<String, String> options = new LinkedHashMap<>();
            options.put("fs.s3a.endpoint", uri);
            options.put("sig", "public-option");
            options.put("signal", "public-option");
            Map<String, String> original = new LinkedHashMap<>(options);

            Map<String, String> redacted = SensitiveConfig.redact(options);

            assertEquals(
                    "https://host/blob?"
                            + name
                            + "="
                            + SensitiveConfig.REDACTED
                            + "&signatureAlgorithm=SHA256&signal=keep#fragment",
                    redacted.get("fs.s3a.endpoint"));
            assertEquals("public-option", redacted.get("sig"));
            assertEquals("public-option", redacted.get("signal"));
            assertEquals(original, options);
        }
    }

    @Test
    public void signatureQueryMatchingDecodesNamesAndRedactsRepeatedValues() {
        String uri =
                "https://host/blob?%58-aMz-%53iGnAtUrE=aws-secret"
                        + "&x-gOoG-sIgNaTuRe=gcs-secret&SiGnAtUrE=legacy-secret"
                        + "&s%69g=azure-first&SiG=azure-second&signal=keep";
        assertEquals(
                "https://host/blob?%58-aMz-%53iGnAtUrE="
                        + SensitiveConfig.REDACTED
                        + "&x-gOoG-sIgNaTuRe="
                        + SensitiveConfig.REDACTED
                        + "&SiGnAtUrE="
                        + SensitiveConfig.REDACTED
                        + "&s%69g="
                        + SensitiveConfig.REDACTED
                        + "&SiG="
                        + SensitiveConfig.REDACTED
                        + "&signal=keep",
                SensitiveConfig.redact("fs.s3a.endpoint", uri));
    }

    @Test
    public void handlesMissingValuesAndEmptyMaps() {
        assertNull(SensitiveConfig.redact("password", null));
        assertEquals("public", SensitiveConfig.redact(null, "public"));
        assertNull(SensitiveConfig.redact((Map<String, String>) null));
        assertTrue(SensitiveConfig.redact(new LinkedHashMap<>()).isEmpty());
    }
}
