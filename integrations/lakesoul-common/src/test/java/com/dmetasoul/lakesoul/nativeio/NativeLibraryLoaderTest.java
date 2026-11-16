// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.nativeio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class NativeLibraryLoaderTest {

    @Test
    public void normalizesDevelopmentPlatforms() {
        assertEquals("linux-x86_64", NativeLibraryLoader.normalizePlatform("Linux", "amd64"));
        assertEquals("linux-aarch64", NativeLibraryLoader.normalizePlatform("linux", "arm64"));
        assertEquals("darwin-x86_64", NativeLibraryLoader.normalizePlatform("Mac OS X", "x86_64"));
        assertEquals("darwin-aarch64", NativeLibraryLoader.normalizePlatform("Darwin", "aarch64"));
        assertEquals(
                "windows-x86_64", NativeLibraryLoader.normalizePlatform("Windows 11", "amd64"));
    }

    @Test
    public void missingDevelopmentResourceContainsActionableDiagnostics() {
        String originalOs = System.getProperty("os.name");
        String originalArchitecture = System.getProperty("os.arch");
        String originalVersion = System.getProperty("lakesoul.artifact.version");
        try {
            System.setProperty("os.name", "Mac OS X");
            System.setProperty("os.arch", "aarch64");
            System.setProperty("lakesoul.artifact.version", "4.0.0-SNAPSHOT");

            assertEquals(
                    "META-INF/native/darwin-aarch64/liblakesoul_io_c.dylib",
                    NativeLibraryLoader.expectedResourcePath("liblakesoul_io_c.dylib"));
            NativeLibraryLoader.extract(NativeLibraryLoaderTest.class, "liblakesoul_io_c.dylib");
            fail("missing development library should have been rejected");
        } catch (IllegalStateException error) {
            String message = error.getMessage();
            assertTrue(message.contains("os.name='Mac OS X'"));
            assertTrue(message.contains("os.arch='aarch64'"));
            assertTrue(message.contains("normalized platform ID='darwin-aarch64'"));
            assertTrue(
                    message.contains(
                            "expected resource"
                                    + " path='META-INF/native/darwin-aarch64/"
                                    + "liblakesoul_io_c.dylib'"));
            assertTrue(message.contains("artifact/native version='4.0.0-SNAPSHOT/4.0.0-dev.0'"));
        } finally {
            restoreProperty("os.name", originalOs);
            restoreProperty("os.arch", originalArchitecture);
            restoreProperty("lakesoul.artifact.version", originalVersion);
        }
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}
