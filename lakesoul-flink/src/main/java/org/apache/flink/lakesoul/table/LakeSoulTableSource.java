package org.apache.flink.lakesoul.table;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LakeSoulTableSource
        implements SupportsFilterPushDown,
                   SupportsPartitionPushDown,
                   SupportsProjectionPushDown,
                   ScanTableSource {
    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        return null;
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        return Optional.empty();
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {

    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {

    }

    @Override
    public ChangelogMode getChangelogMode() {
        return null;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return null;
    }
}
