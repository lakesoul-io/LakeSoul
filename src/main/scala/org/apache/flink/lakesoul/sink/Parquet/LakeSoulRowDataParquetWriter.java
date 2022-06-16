package org.apache.flink.lakesoul.sink.Parquet;

import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;

public class LakeSoulRowDataParquetWriter extends ParquetWriter<RowData> {


//
//    private final Path file;
//    private final HadoopFileSystem fs;
//    private final long maxFileSize;
//    private final RowDataParquetWriteSupport writeSupport;

    public LakeSoulRowDataParquetWriter(Path file, LakeSoulBaseParquetConfig<RowDataParquetWriteSupport> parquetConfig)
            throws IOException {
        super(file,ParquetFileWriter.Mode.CREATE, parquetConfig.getWriteSupport(), parquetConfig.getCompressionCodecName(),
                parquetConfig.getBlockSize(), parquetConfig.getPageSize(), parquetConfig.getPageSize(),
                DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED,
                DEFAULT_WRITER_VERSION,parquetConfig.getHadoopConf());

    }


    @Override
    public void close() throws IOException {
        super.close();
    }
}

