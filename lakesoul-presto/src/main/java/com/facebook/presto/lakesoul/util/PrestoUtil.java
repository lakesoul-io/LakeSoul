package com.facebook.presto.lakesoul.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.presto.lakesoul.pojo.Path;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PrestoUtil {

    public static boolean isExistHashPartition(TableInfo tif) {
        JSONObject tableProperties = JSON.parseObject(tif.getProperties());
        if (tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM()) && tableProperties.getString(LakeSoulOptions.HASH_BUCKET_NUM()).equals("-1")) {
            return false;
        } else {
            return tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM());
        }
    }

    public static Map<String, Map<Integer, List<Path>>> splitDataInfosToRangeAndHashPartition(String tid, DataFileInfo[] dfinfos) {
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
