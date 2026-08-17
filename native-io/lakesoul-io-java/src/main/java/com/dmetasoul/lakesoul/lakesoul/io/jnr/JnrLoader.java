// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import com.dmetasoul.lakesoul.nativeio.NativeLibraryLoader;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;

import java.util.HashMap;
import java.util.Map;

public class JnrLoader {

    private static final String LIBRARY_NAME = "lakesoul_io_c";

    private LibLakeSoulIO libLakeSoulIO = null;

    private boolean hasLoaded = false;

    public static JnrLoader INSTANCE = new JnrLoader();

    public static LibLakeSoulIO get() {
        JnrLoader.tryLoad();
        return INSTANCE.libLakeSoulIO;
    }

    public static synchronized void tryLoad() {
        if (INSTANCE.hasLoaded) {
            return;
        }

        String libraryFile = System.mapLibraryName(LIBRARY_NAME);
        String finalPath = NativeLibraryLoader.extract(JnrLoader.class, libraryFile);
        Map<LibraryOption, Object> libraryOptions = new HashMap<>();
        libraryOptions.put(LibraryOption.LoadNow, true);

        try {
            INSTANCE.libLakeSoulIO =
                    LibraryLoader.loadLibrary(LibLakeSoulIO.class, libraryOptions, finalPath);
        } catch (RuntimeException | LinkageError error) {
            throw NativeLibraryLoader.loadingError(
                    JnrLoader.class, libraryFile, "unavailable", error);
        }
        String nativeVersion;
        try {
            nativeVersion = INSTANCE.libLakeSoulIO.lakesoul_io_version().getString(0);
        } catch (RuntimeException | LinkageError error) {
            throw NativeLibraryLoader.loadingError(
                    JnrLoader.class, libraryFile, "unavailable", error);
        }
        NativeLibraryLoader.validateNativeVersion(JnrLoader.class, libraryFile, nativeVersion);
        if (INSTANCE.libLakeSoulIO != null) {
            // Spark performs bound and null checking, so disable the duplicate Arrow checks.
            System.setProperty("arrow.enable_unsafe_memory_access", "true");
            System.setProperty("arrow.enable_null_check_for_get", "false");
            System.setProperty("arrow.allocation.manager.type", "Netty");
        }
        INSTANCE.hasLoaded = true;
    }

    public static synchronized void unload() {
        INSTANCE = new JnrLoader();
    }
}
