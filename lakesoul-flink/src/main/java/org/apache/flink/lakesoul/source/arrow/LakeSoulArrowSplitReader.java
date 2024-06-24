// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source.arrow;

import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import io.substrait.proto.Plan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.lakesoul.source.LakeSoulPartitionSplit;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.INFERRING_SCHEMA;

public class LakeSoulArrowSplitReader implements SplitReader<LakeSoulArrowWrapper, LakeSoulPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulArrowSplitReader.class);

    private final Configuration conf;

    private final Queue<LakeSoulPartitionSplit> splits;
    private final List<String> partitionColumns;
    private final RowType tableRowType;
    private final byte[] encodedTableInfo;
    RowType projectedRowType;

    RowType projectedRowTypeWithPk;

    List<String> pkColumns;

    boolean isBounded;

    String cdcColumn;

    Plan filter;

    private LakeSoulArrowSplitRecordsReader lastSplitReader;

    public LakeSoulArrowSplitReader(
            byte[] encodedTableInfo,
            Configuration conf,
            RowType tableRowType,
            RowType projectedRowType,
            RowType projectedRowTypeWithPk,
            List<String> pkColumns,
            boolean isBounded,
            String cdcColumn,
            List<String> partitionColumns,
            Plan filter
    ) {
        this.encodedTableInfo = encodedTableInfo;
        this.conf = conf;
        this.splits = new ArrayDeque<>();
        this.tableRowType = tableRowType;
        this.projectedRowType = projectedRowType;
        this.projectedRowTypeWithPk = projectedRowTypeWithPk;
        this.pkColumns = pkColumns;
        this.isBounded = isBounded;
        this.cdcColumn = cdcColumn;
        this.partitionColumns = partitionColumns;
        this.filter = filter;
    }

    @Override
    public RecordsWithSplitIds<LakeSoulArrowWrapper> fetch() throws IOException {
        try {
            close();
            lastSplitReader =
                    new LakeSoulArrowSplitRecordsReader(
                            this.encodedTableInfo,
                            this.conf,
                            Objects.requireNonNull(splits.poll()),
                            this.tableRowType,
                            this.projectedRowType,
                            this.projectedRowTypeWithPk,
                            this.pkColumns,
                            this.isBounded,
                            this.cdcColumn,
                            this.partitionColumns,
                            this.filter
                    );
            return lastSplitReader;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<LakeSoulPartitionSplit> splitChange) {
        if (!(splitChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format("The SplitChange type of %s is not supported.",
                            splitChange.getClass()));
        }

        LOG.info("Handling split change {}",
                splitChange);
        splits.addAll(splitChange.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        if (lastSplitReader != null) {
            lastSplitReader.close();
            lastSplitReader = null;
        }
    }
}
