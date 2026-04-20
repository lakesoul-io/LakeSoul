// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.PrestoUtil;
import com.facebook.presto.lakesoul.substrait.SubstraitPlanBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import scala.collection.JavaConverters;
import io.substrait.expression.Expression;
import io.substrait.type.TypeCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.applyPartitionFilters;
import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.and;
import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.substraitExprToProto;
import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.arrowFieldToSubstraitField;
import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC;
import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER;

public class LakeSoulSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(LakeSoulSplitManager.class);

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session,
                                          ConnectorTableLayoutHandle layout,
                                          SplitSchedulingContext splitSchedulingContext) {

        LakeSoulTableLayoutHandle tableLayout = (LakeSoulTableLayoutHandle) layout;
        String tid = tableLayout.getTableHandle().getId();
        String tableName = tableLayout.getTableHandle().getNames().getTableName();
        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByTableId(tid);
        List<String> partitions = new ArrayList<>();
        try { 
            Schema arrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
            List<String> partitionColumns = tableLayout.getRangeKeys();
            List<Field> partitionFields = partitionColumns.stream().map(arrowSchema::findField).collect(Collectors.toList());
            Schema partitionSchema = new Schema(partitionFields);
            List<PartitionInfo> allPartitionInfo;
            List<FilterPredicate> parFilters = tableLayout.getParFilters();
            Map<String, String> eqFilters = SubstraitPlanBuilder.extractEqualityFilters(parFilters, partitionSchema);
            log.info("DEBUG: parFilters = " + parFilters + ", partitionColumns = " + partitionColumns);
            if (eqFilters != null && !eqFilters.isEmpty()) {
                log.info("DEBUG: eqFilters = " + eqFilters  );
                boolean containsAllKeys = partitionColumns.stream().allMatch(eqFilters::containsKey);
                if (containsAllKeys && eqFilters.size() == partitionColumns.size()) {
                    String partitionDesc = partitionColumns.stream()
                    .map(col -> col + "=" + eqFilters.get(col))
                    .collect(Collectors.joining(LAKESOUL_RANGE_PARTITION_SPLITTER)); 
                    allPartitionInfo = DataOperation.dbManager().getOnePartition(tid, partitionDesc);
                    log.info("DEBUG: Apply all equality partition filter with partitionDesc: " + partitionDesc + " , table:" + tid);
                }else {
                    String partitionQuery = eqFilters.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(" & "));
                    log.info("DEBUG: Apply partial equality partition filter with query: " + partitionQuery + ", table:" + tid);
                    allPartitionInfo = DataOperation.dbManager().getPartitionInfosByPartialFilter(tid, partitionQuery);
                }
            }else {
                log.info("DEBUG: Full fetch table:" + tid);
                allPartitionInfo = MetaVersion.getAllPartitionInfo(tid);
            }
            
            io.substrait.proto.Plan partitionFilterPlan = SubstraitPlanBuilder.buildSubstraitPlan(parFilters, tableLayout.getAllColumns(), tableName);
            log.info("DEBUG: partiitonFilterPlan " + partitionFilterPlan );
            List<PartitionInfo> filteredPartitionInfo = SubstraitUtil.applyPartitionFilters(allPartitionInfo, partitionSchema, partitionFilterPlan);   

            if (filteredPartitionInfo != null) {
                for (PartitionInfo pInfo : filteredPartitionInfo) {
                    partitions.add(pInfo.getPartitionDesc());
                }
            }
        }catch (Exception e) {
            log.error(e, "Error generating splits for LakeSoul table %s", tableName);
            throw new RuntimeException("Failed to get splits for LakeSoul table: " + tableName, e);
        }

        log.info("LakeSoul table %s, filtered partitions: %s", tableName, partitions);
        if (partitions.isEmpty()) {
            return new LakeSoulSplitSource(Collections.emptyList());
        }
        log.info("LakeSoul table %s, split partitions %s",
                tableLayout.getTableHandle().getNames(),
                partitions);
        List<DataFileInfo> allDataFiles = new ArrayList<>();
        for (String partition : partitions) {
            List<String> singlePartition = Collections.singletonList(partition);
            DataFileInfo[] files = DataOperation.getTableDataInfo(tid, JavaConverters.asScalaBuffer(singlePartition).toList());
            if (files != null) {
                Collections.addAll(allDataFiles, files);
            }   
        }
        DataFileInfo[] dfinfos = allDataFiles.toArray(new DataFileInfo[0]);
        ArrayList<ConnectorSplit> splits = new ArrayList<>(16);
        Map<String, Map<Integer, List<Path>>>
                splitByRangeAndHashPartition =
                PrestoUtil.splitDataInfosToRangeAndHashPartition(tid, dfinfos);
        for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                String partitionKey = entry.getKey();
                if (tableLayout.getPrimaryKeys().isEmpty()) {
                    for (Path path : split.getValue()) {
                        splits.add(new LakeSoulSplit(tableLayout, partitionKey, Collections.singletonList(path)));
                    }
                } else {
                    splits.add(new LakeSoulSplit(tableLayout, partitionKey, split.getValue()));
                }
            }
        }
        String tableNames = tableLayout.getTableHandle().getNames().toString();
        log.info("Finished building LakeSoul splits for table " + tableNames +
         ". Total split count: " + splits.size() +
         ". Splits: " + splits);
        return new LakeSoulSplitSource(splits);
    }

}
