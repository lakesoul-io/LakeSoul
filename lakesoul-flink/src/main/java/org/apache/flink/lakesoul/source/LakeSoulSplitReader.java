// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import io.substrait.proto.Plan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

public class LakeSoulSplitReader implements SplitReader<RowData, LakeSoulPartitionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSplitReader.class);

    private final Configuration conf;

    private final Queue<LakeSoulPartitionSplit> splits;
    private final List<String> partitionColumns;
    private final RowType tableRowType;
    RowType projectedRowType;

    RowType projectedRowTypeWithPk;

    List<String> pkColumns;

    boolean isBounded;

    String cdcColumn;

    Plan filter;

    private LakeSoulOneSplitRecordsReader lastSplitReader;

    public LakeSoulSplitReader(Configuration conf,
                               RowType tableRowType,
                               RowType projectedRowType,
                               RowType projectedRowTypeWithPk,
                               List<String> pkColumns,
                               boolean isBounded,
                               String cdcColumn,
                               List<String> partitionColumns,
                               Plan filter) {
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
    public synchronized RecordsWithSplitIds<RowData> fetch() throws IOException {
        try {
            close();
            LakeSoulPartitionSplit split = splits.poll();
            LOG.info("Fetched split {}, oid {}, tid {}",
                    split,
                    System.identityHashCode(this),
                    Thread.currentThread().getId());
            lastSplitReader =
                    new LakeSoulOneSplitRecordsReader(this.conf,
                            Objects.requireNonNull(split),
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
    public synchronized void handleSplitsChanges(SplitsChange<LakeSoulPartitionSplit> splitChange) {
        if (!(splitChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format("The SplitChange type of %s is not supported.",
                            splitChange.getClass()));
        }

        LOG.info("Handling split change {}, oid {}, tid {}",
                splitChange,
                System.identityHashCode(this),
                Thread.currentThread().getId());
        splits.addAll(splitChange.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public synchronized void close() throws Exception {
        if (lastSplitReader != null) {
            lastSplitReader.close();
            lastSplitReader = null;
        }
    }
}
