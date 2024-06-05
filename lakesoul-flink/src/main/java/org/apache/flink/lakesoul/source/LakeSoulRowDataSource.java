// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import io.substrait.proto.Plan;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class LakeSoulRowDataSource extends LakeSoulSource<RowData> {

    public LakeSoulRowDataSource(TableId tableId,
                                 RowType tableRowType,
                                 RowType projectedRowType,
                                 RowType rowTypeWithPk,
                                 boolean isBounded,
                                 List<String> pkColumns,
                                 List<String> partitionColumns,
                                 Map<String, String> optionParams,
                                 @Nullable List<Map<String, String>> remainingPartitions,
                                 @Nullable Plan pushedFilter,
                                 @Nullable Plan partitionFilters
    ) {
        super(tableId,
                tableRowType,
                projectedRowType,
                rowTypeWithPk,
                isBounded,
                pkColumns,
                partitionColumns,
                optionParams,
                remainingPartitions,
                pushedFilter,
                partitionFilters
        );
    }


    @Override
    public SourceReader<RowData, LakeSoulPartitionSplit> createReader(SourceReaderContext readerContext) throws Exception {
        Configuration conf = Configuration.fromMap(optionParams);
        conf.addAll(readerContext.getConfiguration());
        return new LakeSoulSourceReader(
                () -> new LakeSoulSplitReader(
                        conf,
                        this.tableRowType,
                        this.projectedRowType,
                        this.rowTypeWithPk,
                        this.pkColumns,
                        this.isBounded,
                        this.optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN, ""),
                        this.partitionColumns,
                        this.pushedFilter),
                new LakeSoulRecordEmitter(),
                readerContext.getConfiguration(),
                readerContext);
    }

}
