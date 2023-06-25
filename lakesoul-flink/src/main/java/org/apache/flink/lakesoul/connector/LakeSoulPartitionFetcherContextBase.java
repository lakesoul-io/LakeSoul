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
 *
 */

package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.meta.*;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.shaded.guava30.com.google.common.base.Splitter;
import org.apache.flink.table.filesystem.PartitionFetcher;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.util.*;
import java.util.stream.Collectors;

/** Base class for table partition fetcher context. */
public abstract class LakeSoulPartitionFetcherContextBase<P> implements PartitionFetcher.Context<P> {


    List<Map<String, String>> remainingPartitions;

    protected TableId tableId;

    public TableId getTableId() {
        return tableId;
    }

    protected final List<String> partitionKeys;

    protected final String partitionOrderKeys;

    protected transient DBManager dbManager;

    public LakeSoulPartitionFetcherContextBase(TableId tableId, List<String> partitionKeys, String partitionOrderKeys){
        this.tableId = tableId;
        this.partitionKeys = partitionKeys;
        this.partitionOrderKeys = partitionOrderKeys;
    }

    /**
     * open the resources of the Context, this method should first call before call other
     * methods.
     */
    @Override
    public void open() throws Exception {
        dbManager = new DBManager();
    }

    private DataFileInfo[] getTargetDataFileInfo(TableInfo tableInfo) throws Exception {
        return FlinkUtil.getTargetDataFileInfo(tableInfo, null);
    }


    /**
     * Get list that contains partition with comparable object.
     *
     * <p>For 'create-time' and 'partition-time',the comparable object type is Long which
     * represents time in milliseconds, for 'partition-name', the comparable object type is
     * String which represents the partition names string.
     */
    @Override
    public List<ComparablePartitionValue> getComparablePartitionValueList() throws Exception {
        List<ComparablePartitionValue> partitionValueList = new ArrayList<>();
        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        List<String> partitionDescs = this.dbManager.getAllPartitionInfo(tableInfo.getTableId()).stream()
                .map(partitionInfo -> partitionInfo.getPartitionDesc()).collect(Collectors.toList());
        for (String partitionDesc: partitionDescs) {
            partitionValueList.add(getComparablePartitionByName(partitionDesc));
        }
        return partitionValueList;
    }

    private ComparablePartitionValue<List<String>, String> getComparablePartitionByName(
            String partitionDesc) {
        return new ComparablePartitionValue<List<String>, String>() {
            private static final long serialVersionUID = 1L;
            // TODO: delete the option
            private static final long simpleLatestPartition = 1L;

            @Override
            public List<String> getPartitionValue() {
                return extractPartitionValues(partitionDesc.replaceAll(",", "/"));
            }

            @Override
            public String getComparator() {
                // order by partition-order-key name in alphabetical order
                // partition-order-key name format: pt_year=2020,pt_month=10,pt_day=14
                // TODO: update logic here
                if (simpleLatestPartition == 1) {
                    StringBuilder comparator = new StringBuilder();
                    // if partitionOrderKeys is null, default to use all partitionKeys to sort
                    Set<String> partitionOrderKeySet =
                            partitionOrderKeys == null ? new HashSet<>(partitionKeys) : new HashSet<>(Splitter.on(",").splitToList(partitionOrderKeys));
                    List<String> singleParDescList = Splitter.on(",").splitToList(partitionDesc);
                    Map<String, String> parDescMap = new HashMap<>();
                    singleParDescList.forEach(singleParDesc -> {
                        List<String> kvs = Splitter.on("=").splitToList(singleParDesc);
                        parDescMap.put(kvs.get(0), kvs.get(1));
                    });
                    // construct a comparator according to the order in which partitionOrderKeys appear in partitionKeys
                    partitionKeys.forEach(partitionKey -> {
                        if (partitionOrderKeySet.contains(partitionKey)) {
                            comparator.append(partitionKey + "=" + parDescMap.get(partitionKey) + ",");
                        }
                    });
                    comparator.deleteCharAt(comparator.length() - 1);
                    return comparator.toString();
                } else {
                    return String.join(",", getPartitionValue());
                }
            }

        };
    }

    private static List<String> extractPartitionValues(String partitionName) {
        return PartitionPathUtils.extractPartitionValues(
                new org.apache.flink.core.fs.Path(partitionName));
    }

    /**
     * close the resources of the Context, this method should call when the context do not need
     * any more.
     */
    @Override
    public void close() throws Exception {
    }
}
