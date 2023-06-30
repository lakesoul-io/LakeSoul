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

package org.apache.flink.lakesoul.table;

import org.apache.flink.lakesoul.source.LakeSoulSource;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
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
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LakeSoulTableSource
        implements SupportsFilterPushDown, SupportsPartitionPushDown, SupportsProjectionPushDown, ScanTableSource {
    protected TableId tableId;
    protected RowType rowType;

    protected boolean isStreaming;
    protected List<String> pkColumns;

    protected int[][] projectedFields;
    protected Map<String, String> optionParams;

    protected List<Map<String, String>> remainingPartitions;

    public LakeSoulTableSource(TableId tableId, RowType rowType, boolean isStreaming, List<String> pkColumns,
                               Map<String, String> optionParams) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.isStreaming = isStreaming;
        this.pkColumns = pkColumns;
        this.optionParams = optionParams;
    }


    @Override
    public DynamicTableSource copy() {
        LakeSoulTableSource lsts = new LakeSoulTableSource(this.tableId, this.rowType, this.isStreaming, this.pkColumns,
                this.optionParams);
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
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
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
        return RowType.of(projectTypes.toArray(new LogicalType[0]), projectNames.toArray(new String[0]));
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
        String cdcColumn = optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN, "");
        return SourceProvider.of(
                new LakeSoulSource(this.tableId, readFields(), readFieldsAddPk(cdcColumn), this.isStreaming,
                        this.pkColumns, this.optionParams, this.remainingPartitions));
    }
}
