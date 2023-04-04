package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;

public class LakeSoulSource implements Source<RowData, LakeSoulSplit, LakeSoulPendingSplits>
{
    TableId tableId;
    RowType rowType;

    public LakeSoulSource(TableId tableId,RowType rowType){
        this.tableId = tableId;
        this.rowType = rowType;
    }
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, LakeSoulSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new LakeSoulSourceReader(() -> {
            return new LakeSoulSplitReader(readerContext.getConfiguration(),this.rowType);
        },new LakeSoulRecordEmitter(),readerContext.getConfiguration(),readerContext);
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext) throws Exception {
        TableInfo tif = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(),tableId.schema());
        DataFileInfo[] dfinfos = DataOperation.getTableDataInfo(tif.getTableId());
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        int i=0;
        for(DataFileInfo pif : dfinfos){
            ArrayList<Path> tmp = new ArrayList<>();
            tmp.add(new Path(pif.path()));
           splits.add(new LakeSoulSplit(i+"",tmp));
        }

        return new LakeSoulStaticSplitEnumerator(enumContext,new LakeSoulSimpleSplitAssigner(splits));
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext, LakeSoulPendingSplits checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulSplit> getSplitSerializer() {
        return new SimpleLakeSoulSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulPendingSplits> getEnumeratorCheckpointSerializer() {
        return null;
    }
}
