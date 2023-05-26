/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
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
            LOG.error("Cannot find sql file at " + sqlFilePath);
            throw new IOException("Cannot find sql file at " + sqlFilePath);
        }
        LOG.info("Reading sql file from hdfs: " + sqlFilePath);
        FileSystem fs = FileSystem.get(path.toUri());
        String sql = new BufferedReader(new InputStreamReader(fs.open(path)))
                .lines().collect(Collectors.joining("\n"));
        String replaceDefaultKeywordFromZeppelinStr = replaceDefaultKeywordFromZeppelin(sql);
        return replaceDefaultKeywordFromZeppelinStr;

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
