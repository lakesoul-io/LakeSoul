// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

public class JnrLoader {

    private LibLakeSoulIO libLakeSoulIO = null;

    private boolean hasLoaded = false;

    public static final JnrLoader INSTANCE = new JnrLoader();

    public static LibLakeSoulIO get() {
        JnrLoader.tryLoad();
        return INSTANCE.libLakeSoulIO;
    }

    public synchronized static void tryLoad() {
        if (INSTANCE.hasLoaded) {
            return;
        }

        String libName = System.mapLibraryName("lakesoul_io_c");

        String finalPath = null;

        try {
            URLConnection connection = com.dmetasoul.lakesoul.meta.jnr.JnrLoader.class.getResource(libName).openConnection();
            if (connection != null) {
                connection.setUseCaches(false);
                try (final InputStream is = connection.getInputStream()) {
                    if (is == null) {
                        throw new FileNotFoundException(libName);
                    }
                    File temp = File.createTempFile(libName + "_", ".tmp", new File(System.getProperty("java.io.tmpdir")));
                    temp.deleteOnExit();
                    Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    finalPath = temp.getAbsolutePath();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("error loading native libraries: " + e);
        }

        if (finalPath != null) {
            Map<LibraryOption, Object> libraryOptions = new HashMap<>();
            libraryOptions.put(LibraryOption.LoadNow, true);
            libraryOptions.put(LibraryOption.IgnoreError, true);

            JnrLoader.INSTANCE.libLakeSoulIO = LibraryLoader.loadLibrary(
                    LibLakeSoulIO.class,
                    libraryOptions,
                    finalPath
            );
            if (INSTANCE.libLakeSoulIO != null) {
                // spark will do the bound checking and null checking
                // so disable them
                System.setProperty("arrow.enable_unsafe_memory_access", "true");
                System.setProperty("arrow.enable_null_check_for_get", "false");
                System.setProperty("arrow.allocation.manager.type", "Netty");
            }
        }

        INSTANCE.hasLoaded = true;
    }
}
