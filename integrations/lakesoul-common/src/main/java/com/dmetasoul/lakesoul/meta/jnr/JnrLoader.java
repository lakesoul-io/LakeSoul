// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import com.dmetasoul.lakesoul.nativeio.NativeLibraryLoader;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;

import java.util.HashMap;
import java.util.Map;

public class JnrLoader {

    private static final String LIBRARY_NAME = "lakesoul_metadata_c";

    private LibLakeSoulMetaData libLakeSoulMetaData = null;

    private boolean hasLoaded = false;

    public static JnrLoader INSTANCE = new JnrLoader();

    public static LibLakeSoulMetaData get() {
        JnrLoader.tryLoad();
        return INSTANCE.libLakeSoulMetaData;
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
            INSTANCE.libLakeSoulMetaData =
                    LibraryLoader.loadLibrary(LibLakeSoulMetaData.class, libraryOptions, finalPath);
        } catch (RuntimeException | LinkageError error) {
            throw NativeLibraryLoader.loadingError(
                    JnrLoader.class, libraryFile, "unavailable", error);
        }
        String nativeVersion;
        try {
            nativeVersion = INSTANCE.libLakeSoulMetaData.lakesoul_metadata_version().getString(0);
        } catch (RuntimeException | LinkageError error) {
            throw NativeLibraryLoader.loadingError(
                    JnrLoader.class, libraryFile, "unavailable", error);
        }
        NativeLibraryLoader.validateNativeVersion(JnrLoader.class, libraryFile, nativeVersion);
        INSTANCE.hasLoaded = true;
    }
}
