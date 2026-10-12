// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.local;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LakeSoulLocalJavaWriterLogEscapingTest {

    @Test
    public void escapesControlCharactersLikeProtobufTextFormat() {
        // Escaped exactly as protobuf text-format field values, so a newline or
        // bell character in a table identifier cannot split a log record.
        assertEquals("table\\nname", LakeSoulLocalJavaWriter.textFormatEscape("table\nname"));
        assertEquals("id\\aforge", LakeSoulLocalJavaWriter.textFormatEscape("id\u0007forge"));
        assertEquals("back\\\\slash", LakeSoulLocalJavaWriter.textFormatEscape("back\\slash"));
        assertEquals("plain", LakeSoulLocalJavaWriter.textFormatEscape("plain"));
    }
}
