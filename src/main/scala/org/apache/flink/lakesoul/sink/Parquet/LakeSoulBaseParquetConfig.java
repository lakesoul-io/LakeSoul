package org.apache.flink.lakesoul.sink.Parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class LakeSoulBaseParquetConfig<RowDataParquetWriteSupport> {

    private final RowDataParquetWriteSupport writeSupport;
    private final CompressionCodecName compressionCodecName;
    private final int blockSize;
    private final int pageSize;
    private final long maxFileSize;
    private final Configuration hadoopConf;
    private final double compressionRatio;
    private final boolean dictionaryEnabled;

    public LakeSoulBaseParquetConfig(RowDataParquetWriteSupport writeSupport,
                                     CompressionCodecName compressionCodecName,
                                     int blockSize,
                                     int pageSize,
                                     long maxFileSize,
                                     Configuration hadoopConf,
                                     double compressionRatio) {
        this(writeSupport, compressionCodecName, blockSize, pageSize, maxFileSize, hadoopConf, compressionRatio, false);
    }

    public LakeSoulBaseParquetConfig(RowDataParquetWriteSupport writeSupport, CompressionCodecName compressionCodecName, int blockSize,
                                   int pageSize, long maxFileSize, Configuration hadoopConf, double compressionRatio, boolean dictionaryEnabled) {
        this.writeSupport = writeSupport;
        this.compressionCodecName = compressionCodecName;
        this.blockSize = blockSize;
        this.pageSize = pageSize;
        this.maxFileSize = maxFileSize;
        this.hadoopConf = hadoopConf;
        this.compressionRatio = compressionRatio;
        this.dictionaryEnabled = dictionaryEnabled;
    }

    public CompressionCodecName getCompressionCodecName() {
        return compressionCodecName;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public double getCompressionRatio() {
        return compressionRatio;
    }

    public RowDataParquetWriteSupport getWriteSupport() {
        return writeSupport;
    }

    public boolean dictionaryEnabled() {
        return dictionaryEnabled;
    }
}





