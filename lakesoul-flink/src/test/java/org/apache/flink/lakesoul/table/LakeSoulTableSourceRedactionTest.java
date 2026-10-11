// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.table;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class LakeSoulTableSourceRedactionTest {
    @Test
    public void sourceDiagnosticsRedactOptionsWithoutChangingThem() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("s3.access-key", "access-sentinel");
        options.put("fs.s3a.secret.key", "secret-sentinel");
        options.put("AWS_SESSION_TOKEN", "token-sentinel");
        options.put("s3.endpoint", "http://localhost:9000");
        Map<String, String> original = new LinkedHashMap<>(options);
        LakeSoulTableSource source =
                new LakeSoulTableSource(
                        null,
                        null,
                        true,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options);

        for (String diagnostic :
                new String[] {
                    source.toString(), source.asSummaryString(), source.copy().toString()
                }) {
            assertFalse(diagnostic.contains("access-sentinel"));
            assertFalse(diagnostic.contains("secret-sentinel"));
            assertFalse(diagnostic.contains("token-sentinel"));
            assertTrue(diagnostic.contains("[REDACTED]"));
            assertTrue(diagnostic.contains("http://localhost:9000"));
        }
        assertEquals(original, options);
    }
}
