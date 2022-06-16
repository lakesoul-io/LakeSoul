package org.apache.flink.lakesoul.sink;

import org.apache.flink.lakesoul.sink.Parquet.RowDataParquetWriteSupport;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class LakeSoulRowDataParquetWriter extends ParquetWriter<RowData> {
    public LakeSoulRowDataParquetWriter(Path file, WriteSupport<RowData> writeSupport, CompressionCodecName compressionCodecName, int blockSize, int pageSize) throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize);
    }




    }

