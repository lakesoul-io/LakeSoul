package org.apache.flink.lakesoul.entry.clean;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.lakesoul.entry.clean.PartitionInfoRecordGets.PartitionInfo;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

public class CompactProcessFunction extends KeyedProcessFunction<String, PartitionInfo, CompactProcessFunction.CompactionOut> {

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;
    private transient ValueState<Long> switchCompactionVersionState;
    private transient DataSource dataSource;


    public CompactProcessFunction(String pgUrl, String pgUserName, String pgPasswd) {
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
    }

    @Override
    public void open(Configuration openContext) throws Exception {
        Class.forName("org.postgresql.Driver");

        ValueStateDescriptor<Long> switchCompactionVersionDesc = new ValueStateDescriptor<>("switchCompactionVersion",
                Long.class,
                -1L);

        switchCompactionVersionState = getRuntimeContext().getState(switchCompactionVersionDesc);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(pgUrl);
        config.setUsername(pgUserName);
        config.setPassword(pgPasswd);
        config.setDriverClassName("org.postgresql.Driver");

        config.setMaximumPoolSize(5);            // 每个 TM 的最大连接数
        config.setMinimumIdle(1);
        config.setConnectionTimeout(10000);      // 10 秒超时
        config.setIdleTimeout(60000);            // 1 分钟空闲回收
        config.setMaxLifetime(300000);           // 5 分钟重建连接
        config.setAutoCommit(true);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
    }

    @Override
    public void processElement(PartitionInfo value, KeyedProcessFunction<String, PartitionInfo, CompactionOut>.Context ctx, Collector<CompactionOut> out) throws Exception {
        String partitionDesc = value.partitionDesc;
        String tableId = value.tableId;
        long timestamp = value.timestamp;
        long version = value.version;
        String commitOp = value.commitOp;
        List<String> snapshot = value.snapshot;
        CleanUtils cleanUtils = new CleanUtils();
        if (snapshot.size() == 1) {
            try (Connection pgConnection = dataSource.getConnection()) { // ✅ 自动关闭连接
                boolean isOldCompaction = commitOp.equals("UpdateCommit")
                        || cleanUtils.getCompactVersion(tableId, partitionDesc, version, pgConnection);

                Long current = switchCompactionVersionState.value();
                if (current == null) {
                    current = -1L;
                }
                if (isOldCompaction && version > current) {
                    switchCompactionVersionState.update(version);
                }
                out.collect(new CompactionOut(tableId, partitionDesc, version, timestamp, isOldCompaction, switchCompactionVersionState.value()));
            }
        }

    }

    public static class CompactionOut{
        String tableId;
        String partitionDesc;
        long version;
        long timestamp;
        boolean isOldCompaction;
        long switchVersion;

        public CompactionOut(String tableId, String partitionDesc, long version, long timestamp, boolean isOldCompaction, long switchTime) {
            this.tableId = tableId;
            this.partitionDesc = partitionDesc;
            this.version = version;
            this.timestamp = timestamp;
            this.isOldCompaction = isOldCompaction;
            this.switchVersion = switchTime;
        }

        public String getTableId() {
            return tableId;
        }

        public String getPartitionDesc() {
            return partitionDesc;
        }

        public long getVersion() {
            return version;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isOldCompaction() {
            return isOldCompaction;
        }

        @Override
        public String toString() {
            return "CompactionOut{" +
                    "tableId='" + tableId + '\'' +
                    ", partitionDesc='" + partitionDesc + '\'' +
                    ", version=" + version +
                    ", timestamp=" + timestamp +
                    ", isOldCompaction=" + isOldCompaction +
                    ", switchVersion=" + switchVersion +
                    '}';
        }
    }

}