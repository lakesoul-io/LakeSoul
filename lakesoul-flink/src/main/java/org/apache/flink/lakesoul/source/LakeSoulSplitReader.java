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

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
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
    @Nullable
    private LakeSoulArrowReader currentReader;

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
        return new LakeSoulOneSplitRecordsReader(this.conf, splits.peek(), this.rowType, this.rowTypeWithPk,
                this.pkColumns, this.isStreaming, this.cdcColumn);
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
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }
}
