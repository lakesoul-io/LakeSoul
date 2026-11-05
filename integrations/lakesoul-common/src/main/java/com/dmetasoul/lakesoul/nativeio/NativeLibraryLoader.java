// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.nativeio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/** Extracts the supported LakeSoul native libraries from architecture-aware JAR resources. */
public final class NativeLibraryLoader {

    public static final String RELEASE_PLATFORM = "linux-x86_64";
    private static final String RESOURCE_PREFIX = "META-INF/native/";

    private NativeLibraryLoader() {}

    public static String normalizePlatform(String osName, String architecture) {
        return normalizeOs(osName) + "-" + normalizeArchitecture(architecture);
    }

    public static String expectedResourcePath(String libraryFileName) {
        String platform =
                normalizePlatform(
                        System.getProperty("os.name", "unknown"),
                        System.getProperty("os.arch", "unknown"));
        return RESOURCE_PREFIX + platform + "/" + libraryFileName;
    }

    public static String artifactVersion(Class<?> owner) {
        Package ownerPackage = owner.getPackage();
        String version = ownerPackage == null ? null : ownerPackage.getImplementationVersion();
        if (version == null || version.trim().isEmpty()) {
            version = System.getProperty("lakesoul.artifact.version", "unknown");
        }
        return version;
    }

    public static String expectedNativeVersion(Class<?> owner) {
        String artifactVersion = artifactVersion(owner);
        String snapshotSuffix = "-SNAPSHOT";
        if (artifactVersion.endsWith(snapshotSuffix)) {
            return artifactVersion.substring(0, artifactVersion.length() - snapshotSuffix.length())
                    + "-dev.0";
        }
        return artifactVersion;
    }

    public static String extract(Class<?> owner, String libraryFileName) {
        String osName = System.getProperty("os.name", "unknown");
        String architecture = System.getProperty("os.arch", "unknown");
        String platform = normalizePlatform(osName, architecture);
        String resourcePath = RESOURCE_PREFIX + platform + "/" + libraryFileName;

        try {
            ClassLoader classLoader = owner.getClassLoader();
            URL resource =
                    classLoader == null
                            ? ClassLoader.getSystemResource(resourcePath)
                            : classLoader.getResource(resourcePath);
            if (resource == null) {
                throw new FileNotFoundException(resourcePath);
            }
            URLConnection connection = resource.openConnection();
            connection.setUseCaches(false);
            try (InputStream input = connection.getInputStream()) {
                File temporary =
                        File.createTempFile(
                                libraryFileName + "_",
                                librarySuffix(libraryFileName),
                                new File(System.getProperty("java.io.tmpdir")));
                temporary.deleteOnExit();
                Files.copy(input, temporary.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return temporary.getAbsolutePath();
            }
        } catch (IOException error) {
            throw new IllegalStateException(
                    diagnostic(
                            "Failed to extract native library",
                            owner,
                            osName,
                            architecture,
                            platform,
                            resourcePath,
                            expectedNativeVersion(owner)),
                    error);
        }
    }

    public static void validateNativeVersion(
            Class<?> owner, String libraryFileName, String nativeVersion) {
        String expected = expectedNativeVersion(owner);
        if (!"unknown".equals(expected) && !expected.equals(nativeVersion)) {
            throw loadingError(
                    owner,
                    libraryFileName,
                    nativeVersion,
                    new LinkageError(
                            "native version " + nativeVersion + " does not match " + expected));
        }
    }

    public static IllegalStateException loadingError(
            Class<?> owner, String libraryFileName, String nativeVersion, Throwable cause) {
        String osName = System.getProperty("os.name", "unknown");
        String architecture = System.getProperty("os.arch", "unknown");
        return new IllegalStateException(
                diagnostic(
                        "Failed to load native library",
                        owner,
                        osName,
                        architecture,
                        normalizePlatform(osName, architecture),
                        expectedResourcePath(libraryFileName),
                        nativeVersion),
                cause);
    }

    private static String diagnostic(
            String problem,
            Class<?> owner,
            String osName,
            String architecture,
            String platform,
            String resourcePath,
            String nativeVersion) {
        return problem
                + ": os.name='"
                + osName
                + "', os.arch='"
                + architecture
                + "', normalized platform ID='"
                + platform
                + "', expected resource path='"
                + resourcePath
                + "', artifact/native version='"
                + artifactVersion(owner)
                + "/"
                + nativeVersion
                + "'";
    }

    private static String normalizeOs(String osName) {
        String value = osName.toLowerCase(Locale.ROOT);
        if (value.contains("linux")) {
            return "linux";
        }
        if (value.contains("mac") || value.contains("darwin")) {
            return "darwin";
        }
        if (value.contains("windows")) {
            return "windows";
        }
        return sanitize(value);
    }

    private static String normalizeArchitecture(String architecture) {
        String value = architecture.toLowerCase(Locale.ROOT);
        if (value.equals("amd64") || value.equals("x86_64") || value.equals("x86-64")) {
            return "x86_64";
        }
        if (value.equals("aarch64") || value.equals("arm64")) {
            return "aarch64";
        }
        return sanitize(value);
    }

    private static String librarySuffix(String libraryFileName) {
        int extension = libraryFileName.lastIndexOf('.');
        return extension < 0 ? ".tmp" : libraryFileName.substring(extension);
    }

    private static String sanitize(String value) {
        String sanitized = value.replaceAll("[^a-z0-9]+", "-");
        sanitized = sanitized.replaceAll("^-+|-+$", "");
        return sanitized.isEmpty() ? "unknown" : sanitized;
    }
}
