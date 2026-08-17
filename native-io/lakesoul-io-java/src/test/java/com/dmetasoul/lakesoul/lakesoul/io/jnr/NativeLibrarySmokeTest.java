// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.jnr.LibLakeSoulMetaData;
import com.dmetasoul.lakesoul.nativeio.NativeLibraryLoader;

import jnr.ffi.Pointer;

import org.junit.Assume;
import org.junit.Test;

public class NativeLibrarySmokeTest {

    @Test
    public void loadsBothLinuxX8664LibrariesAndReadsTheirBuildIdentity() {
        String platform =
                NativeLibraryLoader.normalizePlatform(
                        System.getProperty("os.name", "unknown"),
                        System.getProperty("os.arch", "unknown"));
        Assume.assumeTrue(NativeLibraryLoader.RELEASE_PLATFORM.equals(platform));

        assertResourcePresent("liblakesoul_io_c.so");
        assertResourcePresent("liblakesoul_metadata_c.so");

        LibLakeSoulIO io = JnrLoader.get();
        LibLakeSoulMetaData metadata = com.dmetasoul.lakesoul.meta.jnr.JnrLoader.get();
        assertNotNull(io);
        assertNotNull(metadata);

        String ioVersion = nativeString(io.lakesoul_io_version());
        String metadataVersion = nativeString(metadata.lakesoul_metadata_version());
        assertEquals(ioVersion, metadataVersion);
        assertTrue(!ioVersion.isEmpty());
        assertTrue(nativeString(io.lakesoul_io_build_info()).contains(ioVersion));
        assertTrue(nativeString(metadata.lakesoul_metadata_build_info()).contains(metadataVersion));
    }

    private static void assertResourcePresent(String libraryFileName) {
        String resource = NativeLibraryLoader.expectedResourcePath(libraryFileName);
        assertNotNull(
                resource, NativeLibrarySmokeTest.class.getClassLoader().getResource(resource));
    }

    private static String nativeString(Pointer pointer) {
        assertNotNull(pointer);
        return pointer.getString(0);
    }
}
