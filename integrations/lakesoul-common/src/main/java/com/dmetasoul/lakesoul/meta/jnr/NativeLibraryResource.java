// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import java.util.Locale;

public final class NativeLibraryResource {

    private static final String RESOURCE_PREFIX = "META-INF/native/";

    private NativeLibraryResource() {}

    public static String path(String libraryName) {
        return RESOURCE_PREFIX
                + platform(System.getProperty("os.name"), System.getProperty("os.arch"))
                + "/"
                + libraryName;
    }

    static String platform(String osName, String osArch) {
        String normalizedOs = osName.toLowerCase(Locale.ROOT);
        String os;
        if (normalizedOs.contains("linux")) {
            os = "linux";
        } else if (normalizedOs.contains("mac") || normalizedOs.contains("darwin")) {
            os = "darwin";
        } else if (normalizedOs.contains("win")) {
            os = "windows";
        } else {
            throw new UnsupportedOperationException("unsupported operating system: " + osName);
        }

        String normalizedArch = osArch.toLowerCase(Locale.ROOT);
        String arch;
        if (normalizedArch.equals("amd64") || normalizedArch.equals("x86_64")) {
            arch = "x86_64";
        } else if (normalizedArch.equals("aarch64") || normalizedArch.equals("arm64")) {
            arch = "aarch64";
        } else {
            throw new UnsupportedOperationException("unsupported architecture: " + osArch);
        }

        return os + "-" + arch;
    }
}
