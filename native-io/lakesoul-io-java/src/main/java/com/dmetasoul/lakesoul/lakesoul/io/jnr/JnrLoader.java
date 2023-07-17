/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import org.apache.arrow.c.jni.JniWrapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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
        
        if (JnrLoader.class.getClassLoader().getResource(libName) != null) {
            try {
                File temp = File.createTempFile(libName + "_", ".tmp", new File(System.getProperty("java.io.tmpdir")));
                temp.deleteOnExit();
                try (final InputStream is = JniWrapper.class.getClassLoader().getResourceAsStream(libName)) {
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
            }
        }

        INSTANCE.hasLoaded = true;
    }
}
