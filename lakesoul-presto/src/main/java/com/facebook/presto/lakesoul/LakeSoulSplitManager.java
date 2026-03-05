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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.applyPartitionFilters;
import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.and;
import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.substraitExprToProto;
import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.arrowFieldToSubstraitField;
import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC;


public class LakeSoulSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(LakeSoulSplitManager.class);


    private io.substrait.proto.Plan buildSubstraitPlan(List<FilterPredicate> parFilters,
                                                   Schema arrowSchema,
                                                   String tableName) throws Exception {
        if (parFilters == null || parFilters.isEmpty()) {
            return null;
        }

        Expression finalExpr = null;

        for (FilterPredicate fp : parFilters) {
            Expression expr = convertToSubstraitExpr(fp, arrowSchema);
            if (expr != null) {
                if (finalExpr == null) {
                    finalExpr = expr;
                } else {
                    finalExpr = SubstraitUtil.and(finalExpr, expr);
                }
            }
        }

        if (finalExpr == null) {
            return null;
        }
        return SubstraitUtil.substraitExprToProto(finalExpr, tableName);
    }


    private Expression convertToSubstraitExpr(FilterPredicate fp, Schema arrowSchema) throws Exception {
        log.info("DEBUG: Converting FilterPredicate type: " + fp.getClass().getSimpleName());
        if (fp instanceof Operators.Eq) {
            return buildBinaryExpr(((Operators.Eq) fp).getColumn(), ((Operators.Eq) fp).getValue(),
                               "equal:any_any", arrowSchema);
        } else if (fp instanceof Operators.GtEq) {
            return buildBinaryExpr(((Operators.GtEq) fp).getColumn(), ((Operators.GtEq) fp).getValue(),
                               "gte:any_any", arrowSchema);
        } else if (fp instanceof Operators.Lt) {
            return buildBinaryExpr(((Operators.Lt) fp).getColumn(), ((Operators.Lt) fp).getValue(),
                               "lt:any_any", arrowSchema);
        } else if (fp instanceof Operators.Gt) {
            return buildBinaryExpr(((Operators.Gt) fp).getColumn(), ((Operators.Gt) fp).getValue(),
                               "gt:any_any", arrowSchema);
        } else if (fp instanceof Operators.LtEq) {
            return buildBinaryExpr(((Operators.LtEq) fp).getColumn(), ((Operators.LtEq) fp).getValue(),
                               "lte:any_any", arrowSchema);
        } else if (fp instanceof Operators.And) {
            Expression left = convertToSubstraitExpr(((Operators.And) fp).getLeft(), arrowSchema);
            Expression right = convertToSubstraitExpr(((Operators.And) fp).getRight(), arrowSchema);
            return SubstraitUtil.and(left, right);
        } else if (fp instanceof Operators.Or) {
            Expression left = convertToSubstraitExpr(((Operators.Or) fp).getLeft(), arrowSchema);
            Expression right = convertToSubstraitExpr(((Operators.Or) fp).getRight(), arrowSchema);
            return SubstraitUtil.or(left, right);
        }
        log.info("DEBUG: Unsupported FilterPredicate:" + fp.getClass().getName());
        return null;
    }

    private Expression buildBinaryExpr(Operators.Column column, Object value, String funcKey, Schema arrowSchema) throws Exception {
        String columnName = column.getColumnPath().toDotString();
        log.info("DEBUG: Building binary expr for column: " + columnName + ", value: " + value + ", func: " + funcKey);
        Field arrowField = arrowSchema.findField(columnName);

        if (arrowField == null) {
            return null;
        }
        Object realValue = value;
        if (value instanceof org.apache.parquet.io.api.Binary) {
            realValue = ((org.apache.parquet.io.api.Binary) value).toStringUsingUTF8();
        }
;
        io.substrait.type.Type subType = SubstraitUtil.arrowFieldToSubstraitType(arrowField);
        Expression literal = SubstraitUtil.anyToSubstraitLiteral(subType, realValue);

        Expression fieldRef = SubstraitUtil.arrowFieldToSubstraitField(arrowField);
        log.info("DEBUG: Successfully built binary expr for columnName " + columnName + ", fieldRef" + fieldRef + ", literal" + literal + ", subType" + subType);
        return SubstraitUtil.makeBinary(
                fieldRef,
                literal,
                SubstraitUtil.CompNamespace,
                funcKey,
                subType
        );
    }



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

            if (partitionColumns.isEmpty()) {
                allPartitionInfo = DataOperation.dbManager().getPartitionInfos(tid, Collections.singletonList(LAKESOUL_NON_PARTITION_TABLE_PART_DESC));
            } else {
                allPartitionInfo = MetaVersion.getAllPartitionInfo(tid);
            }
            
            List<FilterPredicate> parFilters = tableLayout.getParFilters();
            log.info("DEBUG: parFilters = " + parFilters);
            io.substrait.proto.Plan partitionFilterPlan = buildSubstraitPlan(parFilters, partitionSchema, tableName);
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
                if (tableLayout.getPrimaryKeys().isEmpty()) {
                    for (Path path : split.getValue()) {
                        splits.add(new LakeSoulSplit(tableLayout, Collections.singletonList(path)));
                    }
                } else {
                    splits.add(new LakeSoulSplit(tableLayout, split.getValue()));
                }
            }
        }
        String tableNames = tableLayout.getTableHandle().getNames().toString();
        log.info("Finished building LakeSoul splits for table " + tableLayout.getTableHandle().getNames() +
         ". Total split count: " + splits.size() +
         ". Splits: " + splits);
        return new LakeSoulSplitSource(splits);
    }

}
