// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

// Modified from https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/core/fs/Path.java
// to avoid using Hadoop's Path class

package com.dmetasoul.lakesoul.meta;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class Path implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The directory separator, a slash. */
    public static final String SEPARATOR = "/";

    /** Character denoting the current directory. */
    public static final String CUR_DIR = ".";

    /** A pre-compiled regex/state-machine to match the windows drive pattern. */
    private static final Pattern WINDOWS_ROOT_DIR_REGEX = Pattern.compile("/\\p{Alpha}+:/");

    /** The internal representation of the path, a hierarchical URI. */
    private URI uri;

    /** Constructs a new (empty) path object (used to reconstruct path object after RPC call). */
    public Path() {}

    /**
     * Constructs a path object from a given URI.
     *
     * @param uri the URI to construct the path object from
     */
    public Path(URI uri) {
        this.uri = uri;
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(String parent, String child) {
        this(new Path(parent), new Path(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, String child) {
        this(parent, new Path(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(String parent, Path child) {
        this(new Path(parent), child);
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, Path child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        final String parentPath = parentUri.getPath();
        if (!(parentPath.equals("/") || parentPath.equals(""))) {
            try {
                parentUri =
                        new URI(
                                parentUri.getScheme(),
                                parentUri.getAuthority(),
                                parentUri.getPath() + "/",
                                null,
                                null);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (child.uri.getPath().startsWith(Path.SEPARATOR)) {
            child =
                    new Path(
                            child.uri.getScheme(),
                            child.uri.getAuthority(),
                            child.uri.getPath().substring(1));
        }

        final URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(), resolved.getAuthority(), resolved.getPath());
    }

    /**
     * Checks if the provided path string is either null or has zero length and throws a {@link
     * IllegalArgumentException} if any of the two conditions apply.
     *
     * @param path the path string to be checked
     * @return The checked path.
     */
    private String checkPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        return path;
    }

    /**
     * Construct a path from a String. Path strings are URIs, but with unescaped elements and some
     * additional normalization.
     *
     * @param pathString the string to construct a path from
     */
    public Path(String pathString) {
        pathString = checkPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString, false)) {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start)
                && (pathString.length() - start > 2)) { // has authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path);
    }

    /**
     * Construct a Path from a scheme, an authority and a path string.
     *
     * @param scheme the scheme string
     * @param authority the authority string
     * @param path the path string
     */
    public Path(String scheme, String authority, String path) {
        path = checkPathArg(path);
        initialize(scheme, authority, path);
    }

    /**
     * Initializes a path object given the scheme, authority and path string.
     *
     * @param scheme the scheme string.
     * @param authority the authority string.
     * @param path the path string.
     */
    private void initialize(String scheme, String authority, String path) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Normalizes a path string.
     *
     * @param path the path string to normalize
     * @return the normalized path string
     */
    private String normalizePath(String path) {
        // remove consecutive slashes & backslashes
        path = path.replace("\\", "/");
        path = path.replaceAll("/+", "/");

        // remove tailing separator
        if (path.endsWith(SEPARATOR)
                && !path.equals(SEPARATOR)
                && // UNIX root path
                !WINDOWS_ROOT_DIR_REGEX.matcher(path).matches()) { // Windows root path)

            // remove tailing slash
            path = path.substring(0, path.length() - SEPARATOR.length());
        }

        return path;
    }

    /**
     * Converts the path object to a {@link URI}.
     *
     * @return the {@link URI} object converted from the path object
     */
    public URI toUri() {
        return uri;
    }

    /**
     * Checks if the directory of this path is absolute.
     *
     * @return <code>true</code> if the directory of this path is absolute, <code>false</code>
     *     otherwise
     */
    public boolean isAbsolute() {
        final int start = hasWindowsDrive(uri.getPath(), true) ? 3 : 0;
        return uri.getPath().startsWith(SEPARATOR, start);
    }

    /**
     * Returns the final component of this path, i.e., everything that follows the last separator.
     *
     * @return the final component of the path
     */
    public String getName() {
        final String path = uri.getPath();
        final int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash + 1);
    }

    /**
     * Return full path.
     *
     * @return full path
     */
    public String getPath() {
        return uri.getPath();
    }

    /**
     * Returns the parent of a path, i.e., everything that precedes the last separator or <code>null
     * </code> if at root.
     *
     * @return the parent of a path or <code>null</code> if at root.
     */
    public Path getParent() {
        final String path = uri.getPath();
        final int lastSlash = path.lastIndexOf('/');
        final int start = hasWindowsDrive(path, true) ? 3 : 0;
        if ((path.length() == start)
                || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            final int end = hasWindowsDrive(path, true) ? 3 : 0;
            parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
        }
        return new Path(uri.getScheme(), uri.getAuthority(), parent);
    }

    /**
     * Adds a suffix to the final name in the path.
     *
     * @param suffix The suffix to be added
     * @return the new path including the suffix
     */
    public Path suffix(String suffix) {
        return new Path(getParent(), getName() + suffix);
    }

    @Override
    public String toString() {
        // we can't use uri.toString(), which escapes everything, because we want
        // illegal characters unescaped in the string, for glob processing, etc.
        final StringBuilder buffer = new StringBuilder();
        if (uri.getScheme() != null) {
            buffer.append(uri.getScheme());
            buffer.append(":");
        }
        if (uri.getAuthority() != null) {
            buffer.append("//");
            buffer.append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf('/') == 0
                    && hasWindowsDrive(path, true)
                    && // has windows drive
                    uri.getScheme() == null
                    && // but no scheme
                    uri.getAuthority() == null) { // or authority
                path = path.substring(1); // remove slash before drive
            }
            buffer.append(path);
        }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Path)) {
            return false;
        }
        Path that = (Path) o;
        return this.uri.equals(that.uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    public int compareTo(Object o) {
        Path that = (Path) o;
        return this.uri.compareTo(that.uri);
    }

    /**
     * Returns the number of elements in this path.
     *
     * @return the number of elements in this path
     */
    public int depth() {
        String path = uri.getPath();
        int depth = 0;
        int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
        while (slash != -1) {
            depth++;
            slash = path.indexOf(SEPARATOR, slash + 1);
        }
        return depth;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @return True, if the path string contains a windows drive letter, false otherwise.
     */
    public boolean hasWindowsDrive() {
        return hasWindowsDrive(uri.getPath(), true);
    }

    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @param path the path to check
     * @param slashed true to indicate the first character of the string is a slash, false otherwise
     * @return <code>true</code> if the path string contains a windows drive letter, false otherwise
     */
    private boolean hasWindowsDrive(String path, boolean slashed) {
        final int start = slashed ? 1 : 0;
        return path.length() >= start + 2
                && (!slashed || path.charAt(0) == '/')
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z')
                || (path.charAt(start) >= 'a' && path.charAt(start) <= 'z'));
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Creates a path for the given local file.
     *
     * <p>This method is useful to make sure the path creation for local files works seamlessly
     * across different operating systems. Especially Windows has slightly different rules for
     * slashes between schema and a local file path, making it sometimes tricky to produce
     * cross-platform URIs for local files.
     *
     * @param file The file that the path should represent.
     * @return A path representing the local file URI of the given file.
     */
    public static Path fromLocalFile(File file) {
        return new Path(file.toURI());
    }
}
