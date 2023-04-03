package org.apache.flink.lakesoul.source;

import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
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
import java.util.Queue;

public class LakeSoulSplitReader
        implements SplitReader<RowData, LakeSoulSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSplitReader.class);

    private final Configuration conf;

    private final Queue<LakeSoulSplit> splits;

    @Nullable private LakeSoulArrowReader currentReader;

    @Nullable private String currentSplitId;

    RowType rowType;

    public LakeSoulSplitReader(Configuration conf,RowType rowType) {
        this.conf = conf;
        this.splits = new ArrayDeque<>();
        this.rowType = rowType;
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        return new LakeSoulOneSplitRecordsReader(this.conf,splits.peek(),this.rowType);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<LakeSoulSplit> splitChange) {
        if (!(splitChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitChange.getClass()));
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
