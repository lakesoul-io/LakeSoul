// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

public class JnrLoader {

    private LibLakeSoulMetaData libLakeSoulMetaData = null;

    private boolean hasLoaded = false;

    public static final JnrLoader INSTANCE = new JnrLoader();

    public static LibLakeSoulMetaData get() {
        JnrLoader.tryLoad();
        return INSTANCE.libLakeSoulMetaData;
    }

    public synchronized static void tryLoad() {
        if (INSTANCE.hasLoaded) {
            return;
        }

        String libName = System.mapLibraryName("lakesoul_metadata_c");

        String finalPath = null;

        if (JnrLoader.class.getClassLoader().getResource(libName) != null) {
            try {
                File temp = File.createTempFile(libName + "_", ".tmp", new File(System.getProperty("java.io.tmpdir")));
                temp.deleteOnExit();
                try (final InputStream is = JnrLoader.class.getClassLoader().getResourceAsStream(libName)) {
                    if (is == null) {
                        throw new FileNotFoundException(libName);
                    }
                    Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    finalPath = temp.getAbsolutePath();
                }
            } catch (IOException e) {
                throw new IllegalStateException("error loading native libraries: " + e);
            }
        }

        if (finalPath != null) {
            Map<LibraryOption, Object> libraryOptions = new HashMap<>();
            libraryOptions.put(LibraryOption.LoadNow, true);
            libraryOptions.put(LibraryOption.IgnoreError, true);

            JnrLoader.INSTANCE.libLakeSoulMetaData = LibraryLoader.loadLibrary(
                    LibLakeSoulMetaData.class,
                    libraryOptions,
                    finalPath
            );

        }

        INSTANCE.hasLoaded = true;
    }
}
