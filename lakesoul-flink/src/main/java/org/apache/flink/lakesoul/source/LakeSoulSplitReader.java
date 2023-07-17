// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

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

public class LakeSoulSplitReader implements SplitReader<RowData, LakeSoulSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSplitReader.class);

    private final Configuration conf;

    private final Queue<LakeSoulSplit> splits;
    RowType rowType;
    RowType rowTypeWithPk;
    List<String> pkColumns;
    boolean isStreaming;
    String cdcColumn;

    private LakeSoulOneSplitRecordsReader lastSplitReader;

    public LakeSoulSplitReader(Configuration conf, RowType rowType, RowType rowTypeWithPk, List<String> pkColumns,
                               boolean isStreaming, String cdcColumn) {
        this.conf = conf;
        this.splits = new ArrayDeque<>();
        this.rowType = rowType;
        this.rowTypeWithPk = rowTypeWithPk;
        this.pkColumns = pkColumns;
        this.isStreaming = isStreaming;
        this.cdcColumn = cdcColumn;
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        try {
            close();
            lastSplitReader =
                    new LakeSoulOneSplitRecordsReader(this.conf, Objects.requireNonNull(splits.poll()), this.rowType,
                            this.rowTypeWithPk, this.pkColumns, this.isStreaming, this.cdcColumn);
            return lastSplitReader;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<LakeSoulSplit> splitChange) {
        if (!(splitChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format("The SplitChange type of %s is not supported.", splitChange.getClass()));
        }

        LOG.info("Handling split change {}", splitChange);
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
