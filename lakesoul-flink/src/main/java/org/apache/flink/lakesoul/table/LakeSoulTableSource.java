// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.table;

import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.lakesoul.source.LakeSoulSource;
import org.apache.flink.lakesoul.source.ParquetFilters;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LakeSoulTableSource
        implements SupportsFilterPushDown, SupportsPartitionPushDown, SupportsProjectionPushDown, ScanTableSource, SupportsRowLevelModificationScan {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulTableSource.class);

    // NOTE: if adding fields in this class, do remember to add assignments in copy methods
    // of both this class and its subclass.

    protected TableId tableId;

    protected RowType rowType;

    protected boolean isStreaming;

    protected List<String> pkColumns;

    protected int[][] projectedFields;

    protected Map<String, String> optionParams;

    protected List<Map<String, String>> remainingPartitions;

    protected FilterPredicate filter;

    public LakeSoulTableSource(TableId tableId,
                               RowType rowType,
                               boolean isStreaming,
                               List<String> pkColumns,
                               Map<String, String> optionParams) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.isStreaming = isStreaming;
        this.pkColumns = pkColumns;
        this.optionParams = optionParams;
    }

    @Override
    public DynamicTableSource copy() {
        LakeSoulTableSource lsts = new LakeSoulTableSource(this.tableId,
                this.rowType,
                this.isStreaming,
                this.pkColumns,
                this.optionParams);
        lsts.projectedFields = this.projectedFields;
        lsts.remainingPartitions = this.remainingPartitions;
        lsts.filter = this.filter;
        return lsts;
    }

    @Override
    public String asSummaryString() {
        return toString();
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // first we filter out partition filter conditions
        LOG.info("Applying filters to native io: {}", filters);
        List<ResolvedExpression> remainingFilters = new ArrayList<>();
        List<ResolvedExpression> nonPartitionFilters = new ArrayList<>();
        DBManager dbManager = new DBManager();
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        Set<String> partitionCols = new HashSet<>(partitionKeys.rangeKeys);
        for (ResolvedExpression filter : filters) {
            if (ParquetFilters.filterContainsPartitionColumn(filter, partitionCols)) {
                remainingFilters.add(filter);
            } else {
                nonPartitionFilters.add(filter);
            }
        }
        // find acceptable non partition filters
        Tuple2<Result, FilterPredicate> filterPushDownResult = ParquetFilters.toParquetFilter(nonPartitionFilters,
                remainingFilters);
        this.filter = filterPushDownResult.f1;
        LOG.info("Applied filters to native io: {}, accepted {}, remaining {}", this.filter,
                filterPushDownResult.f0.getAcceptedFilters(),
                filterPushDownResult.f0.getRemainingFilters());
        return filterPushDownResult.f0;
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        DBManager dbManager = new DBManager();
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        List<PartitionInfo> allPartitionInfo = dbManager.getAllPartitionInfo(tableInfo.getTableId());
        List<Map<String, String>> partitions = new ArrayList<>();
        for (PartitionInfo info : allPartitionInfo) {
            if (!info.getPartitionDesc().equals(DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC)) {
                partitions.add(DBUtil.parsePartitionDesc(info.getPartitionDesc()));
            }
        }
        return Optional.of(partitions);
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        this.remainingPartitions = remainingPartitions;
        LOG.info("Applied partitions to native io: {}", this.remainingPartitions);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
    }

    private int[] getFieldIndexs() {
        return (projectedFields == null || projectedFields.length == 0) ?
                IntStream.range(0, this.rowType.getFieldCount()).toArray() :
                Arrays.stream(projectedFields).mapToInt(array -> array[0]).toArray();
    }

    protected RowType readFields() {
        int[] fieldIndexs = getFieldIndexs();
        return RowType.of(Arrays.stream(fieldIndexs).mapToObj(this.rowType::getTypeAt).toArray(LogicalType[]::new),
                Arrays.stream(fieldIndexs).mapToObj(this.rowType.getFieldNames()::get).toArray(String[]::new));
    }

    private RowType readFieldsAddPk(String cdcColumn) {
        int[] fieldIndexs = getFieldIndexs();
        List<LogicalType> projectTypes =
                Arrays.stream(fieldIndexs).mapToObj(this.rowType::getTypeAt).collect(Collectors.toList());
        List<String> projectNames =
                Arrays.stream(fieldIndexs).mapToObj(this.rowType.getFieldNames()::get).collect(Collectors.toList());
        List<String> pkNamesNotExistInReadFields = new ArrayList<>();
        List<LogicalType> pkTypesNotExistInReadFields = new ArrayList<>();
        for (String pk : pkColumns) {
            if (!projectNames.contains(pk)) {
                pkNamesNotExistInReadFields.add(pk);
                pkTypesNotExistInReadFields.add(this.rowType.getTypeAt(rowType.getFieldIndex(pk)));
            }
        }
        projectNames.addAll(pkNamesNotExistInReadFields);
        projectTypes.addAll(pkTypesNotExistInReadFields);
        if (!cdcColumn.equals("") && !projectNames.contains(cdcColumn)) {
            projectNames.add(cdcColumn);
            projectTypes.add(new VarCharType());
        }
        return RowType.of(projectTypes.toArray(new LogicalType[0]),
                projectNames.toArray(new String[0]));
    }

    @Override
    public ChangelogMode getChangelogMode() {
        boolean isCdc = !optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN, "").isEmpty();
        if (this.isStreaming && isCdc) {
            return ChangelogMode.upsert();
        } else if (this.isStreaming && !this.pkColumns.isEmpty()) {
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        } else {
            // batch read or streaming read without pk
            return ChangelogMode.insertOnly();
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        String cdcColumn = optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN,
                "");
        return SourceProvider.of(
                new LakeSoulSource(this.tableId,
                        readFields(),
                        readFieldsAddPk(cdcColumn),
                        this.isStreaming,
                        this.pkColumns,
                        this.optionParams,
                        this.remainingPartitions,
                        this.filter));
    }

    @Override
    public String toString() {
        return "LakeSoulTableSource{" +
                "tableId=" + tableId +
                ", rowType=" + rowType +
                ", isStreaming=" + isStreaming +
                ", pkColumns=" + pkColumns +
                ", projectedFields=" + Arrays.toString(projectedFields) +
                ", optionParams=" + optionParams +
                ", remainingPartitions=" + remainingPartitions +
                ", filter=" + filter +
                '}';
    }

    @Override
    public RowLevelModificationScanContext applyRowLevelModificationScan(RowLevelModificationType rowLevelModificationType, @Nullable RowLevelModificationScanContext previousContext) {
        return null;
    }
}
