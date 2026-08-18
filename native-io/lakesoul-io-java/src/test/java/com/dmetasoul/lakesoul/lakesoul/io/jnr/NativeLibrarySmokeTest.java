// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class NativeLibrarySmokeTest {

    @Test
    public void loadsPackagedNativeLibraries() {
        assertNotNull(com.dmetasoul.lakesoul.meta.jnr.JnrLoader.get());
        assertNotNull(JnrLoader.get());
    }
}
