package org.apache.flink.lakesoul.connector;

import org.apache.flink.lakesoul.connector.LakeSoulPartition;
import org.apache.flink.lakesoul.connector.LakeSoulPartitionReader;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.filesystem.PartitionReader;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestPartitionReader implements PartitionReader<LakeSoulPartition, RowData> {
    private transient int count;
    private  transient List<RowData> data;

    public TestPartitionReader() {
        this.count = 0;

    }
    /**
     * Opens the reader with given partitions.
     *
     * @param partitions
     */
    @Override
    public void open(List<LakeSoulPartition> partitions) throws IOException {
        this.data = new ArrayList<RowData>(){{
            add(GenericRowData.of(1, StringData.fromString("a"), 10));
            add(GenericRowData.of(2, StringData.fromString("a"), 21));
            add(GenericRowData.of(2, StringData.fromString("b"), 22));
            add(GenericRowData.of(3, StringData.fromString("c"), 33));
        }};
    }

    /**
     * Reads the next record from the partitions.
     *
     * <p>When this method is called, the reader it guaranteed to be opened.
     *
     * @param reuse Object that may be reused.
     * @return Read record.
     */
    @Nullable
    @Override
    public RowData read(RowData reuse) throws IOException {
        if (count >= 4) return null;
        int pos = count;
        count++;
        return this.data.get(pos);
    }

    /**
     * Close the reader, this method should release all resources.
     *
     * <p>When this method is called, the reader it guaranteed to be opened.
     */
    @Override
    public void close() throws IOException {

    }
}

