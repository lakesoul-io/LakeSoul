// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.utils;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    public static String readHDFSFile(String sqlFilePath) throws IOException, URISyntaxException {
        URI uri = new URI(sqlFilePath);
        Path path = new Path(sqlFilePath);
        if (!FileSystem.get(uri).exists(path)) {
            LOG.error("Cannot find sql file at {}", sqlFilePath);
            throw new IOException("Cannot find sql file at " + sqlFilePath);
        }
        LOG.info("Reading sql file from path: {}", sqlFilePath);
        FileSystem fs = FileSystem.get(path.toUri());
        String sql = new BufferedReader(new InputStreamReader(fs.open(path)))
                .lines().collect(Collectors.joining("\n"));
        return replaceDefaultKeywordFromZeppelin(sql);

    }

    /*
     * In zeppelin sql, "use default" is running well,
     * but in flink sql, "default" is a keyword. need to replace it to "`default`".
     * so, this function is used to replace "use default" to "use `default`".
     *
     * */
    public static String replaceDefaultKeywordFromZeppelin(String text) {
        if (text == null || text.length() <= 0) {
            LOG.info("text is null or empty");
            return text;
        }
        String pattern = "(?i)use\\s+(?i)default";
        String replaceText = "use `default`";
        Pattern pc = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        String replacedStr = pc.matcher(text).replaceAll(replaceText);
        return replacedStr;
    }
}
