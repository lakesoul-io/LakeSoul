// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.presto.lakesoul.pojo.Path;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;

public class PrestoUtil {

    public static final String CDC_CHANGE_COLUMN = "lakesoul_cdc_change_column";
    public static final String CDC_CHANGE_COLUMN_DEFAULT = "rowKinds";

    private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

    public static LinkedHashMap<String, String> extractPartitionSpecFromPath(Path path) {
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap();
        List<String[]> kvs = new ArrayList();
        org.apache.hadoop.fs.Path currPath = path;
        do {
            String component = currPath.getName();
            Matcher m = PARTITION_NAME_PATTERN.matcher(component);
            if (m.matches()) {
                String k = unescapePathName(m.group(1));
                String v = unescapePathName(m.group(2));
                String[] kv = new String[]{k, v};
                kvs.add(kv);
            }

            currPath = currPath.getParent();
        } while (currPath != null && !currPath.getName().isEmpty());

        for (int i = kvs.size(); i > 0; --i) {
            fullPartSpec.put(((String[]) kvs.get(i - 1))[0], ((String[]) kvs.get(i - 1))[1]);
        }

        return fullPartSpec;
    }

    public static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < path.length(); ++i) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;

                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception var6) {
                }

                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }

            sb.append(c);
        }

        return sb.toString();
    }

    public static boolean isExistHashPartition(TableInfo tif) {
        JSONObject tableProperties = JSON.parseObject(tif.getProperties());
        if (tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM()) &&
                tableProperties.getString(LakeSoulOptions.HASH_BUCKET_NUM()).equals("-1")) {
            return false;
        } else {
            return tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM());
        }
    }

    public static Map<String, Map<Integer, List<Path>>> splitDataInfosToRangeAndHashPartition(String tid,
                                                                                              DataFileInfo[] dfinfos) {
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        TableInfo tif = DataOperation.dbManager().getTableInfoByTableId(tid);
        for (DataFileInfo pif : dfinfos) {
            if (isExistHashPartition(tif) && pif.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(pif.file_bucket_id(), v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(-1, v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            }
        }
        return splitByRangeAndHashPartition;
    }
}
