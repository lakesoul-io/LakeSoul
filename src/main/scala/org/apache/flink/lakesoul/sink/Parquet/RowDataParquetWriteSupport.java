package org.apache.flink.lakesoul.sink.Parquet;

import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import java.util.HashMap;


/**
 * Row data parquet write support.
 */
public class RowDataParquetWriteSupport extends WriteSupport<RowData> {

    private final RowType rowType;
    private final MessageType schema;
    private ParquetRowDataWriter writer;

    public RowDataParquetWriteSupport(RowType rowType) {
        super();
        this.rowType = rowType;
        this.schema = ParquetSchemaConverter.convertToParquetMessageType("flink_schema", rowType);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        // should make the utc timestamp configurable
        this.writer = new ParquetRowDataWriter(recordConsumer, rowType, schema, true);
    }

    @Override
    public void write(RowData record) {
        try {
            this.writer.write(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

