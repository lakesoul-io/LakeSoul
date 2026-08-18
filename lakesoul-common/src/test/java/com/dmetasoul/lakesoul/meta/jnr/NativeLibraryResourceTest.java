// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NativeLibraryResourceTest {

    @Test
    public void normalizesSupportedPlatforms() {
        assertEquals("linux-x86_64", NativeLibraryResource.platform("Linux", "amd64"));
        assertEquals("linux-aarch64", NativeLibraryResource.platform("Linux", "arm64"));
        assertEquals("darwin-x86_64", NativeLibraryResource.platform("Mac OS X", "x86_64"));
        assertEquals("darwin-aarch64", NativeLibraryResource.platform("Darwin", "aarch64"));
        assertEquals("windows-x86_64", NativeLibraryResource.platform("Windows 11", "amd64"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rejectsUnsupportedOperatingSystems() {
        NativeLibraryResource.platform("Plan 9", "amd64");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rejectsUnsupportedArchitectures() {
        NativeLibraryResource.platform("Linux", "s390x");
    }
}
