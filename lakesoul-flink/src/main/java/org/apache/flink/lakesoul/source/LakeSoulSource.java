package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

public class LakeSoulSource implements Source<RowData, LakeSoulSplit, LakeSoulPendingSplits>
{
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, LakeSoulSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext, LakeSoulPendingSplits checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulPendingSplits> getEnumeratorCheckpointSerializer() {
        return null;
    }
}
