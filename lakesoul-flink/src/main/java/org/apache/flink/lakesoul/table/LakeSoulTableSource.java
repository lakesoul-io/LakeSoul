package org.apache.flink.lakesoul.table;

import org.apache.flink.lakesoul.source.LakeSoulSource;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.types.RowKind;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LakeSoulTableSource
        implements SupportsFilterPushDown,
        SupportsPartitionPushDown,
        SupportsProjectionPushDown,
        ScanTableSource {
    TableId tableId;
    RowType rowType;

    boolean isStreaming;
    List<String> pkColumns;

    int[][] projectedFields;

    private List<Map<String, String>> remainingPartitions;

    public LakeSoulTableSource(TableId tableId, RowType rowType, boolean isStreaming, List<String> pkColumns) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.isStreaming = isStreaming;
        this.pkColumns = pkColumns;
    }


    @Override
    public DynamicTableSource copy() {
        LakeSoulTableSource lsts = new LakeSoulTableSource(this.tableId, this.rowType, this.isStreaming, this.pkColumns);
        lsts.projectedFields = this.projectedFields;
        lsts.remainingPartitions = this.remainingPartitions;
        return lsts;
    }

    @Override
    public String asSummaryString() {
        return "LakeSoul table source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        return Result.of(new ArrayList(filters), new ArrayList(filters));
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        return Optional.empty();
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        this.remainingPartitions = remainingPartitions;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
    }

    private RowType readFields() {
        int[] fieldIndexs = projectedFields == null
                ? IntStream.range(0, this.rowType.getFieldCount()).toArray()
                : Arrays.stream(projectedFields).mapToInt(array -> array[0]).toArray();
        LogicalType[] projectTypes = Arrays.stream(fieldIndexs).mapToObj(this.rowType::getTypeAt).toArray(LogicalType[]::new);
        String[] projectNames = Arrays.stream(fieldIndexs).mapToObj(this.rowType.getFieldNames()::get).toArray(String[]::new);
        return RowType.of(projectTypes, projectNames);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        //.addContainedKind(RowKind.UPDATE_BEFORE).addContainedKind(RowKind.UPDATE_AFTER).addContainedKind(RowKind.DELETE)
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        return SourceProvider.of(new LakeSoulSource(this.tableId, readFields(), this.isStreaming, this.pkColumns,this.remainingPartitions));
    }
}
