package org.apache.flink.lakesoul.source;

import org.apache.arrow.lakesoul.io.NativeIOReader;
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class LakeSoulOneSplitRecordsReader implements RecordsWithSplitIds<RowData> {

    private final LakeSoulSplit split;

    private final Configuration conf;

    private final RowType schema;

    private LakeSoulArrowReader reader;

    private VectorSchemaRoot currentVCR;

    private int curRecordId = 0;

    private ArrowReader curArrowReader;

    public LakeSoulOneSplitRecordsReader(Configuration conf, LakeSoulSplit split, RowType schema) throws IOException {
        this.split = split;
        this.conf = conf;
        this.schema = schema;

        initializeReader();
    }

    private void initializeReader() throws IOException {
        NativeIOReader reader = new NativeIOReader();
        for (Path path : split.getFiles()) {
            reader.addFile(FlinkUtil.makeQualifiedPath(path).toString());
        }
        Schema arrowSchema = ArrowUtils.toArrowSchema(schema);
        reader.setSchema(arrowSchema);
        FlinkUtil.setFSConfigs(conf, reader);
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader, 10000);
    }

    @Nullable
    @Override
    public String nextSplit() {
        return null;
    }

    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        if (this.currentVCR == null) {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                this.curArrowReader = ArrowUtils.createArrowReader(currentVCR, this.schema);

                if (this.currentVCR == null) {
                    return null;
                }
                curRecordId = 0;
            } else {
                this.reader.close();
            }
        }
        if (curRecordId < currentVCR.getRowCount()) {
            int tmp = curRecordId;
            curRecordId++;
            return this.curArrowReader.read(curRecordId);
        } else {
            return null;
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return Collections.singleton(split.splitId());
    }
}
