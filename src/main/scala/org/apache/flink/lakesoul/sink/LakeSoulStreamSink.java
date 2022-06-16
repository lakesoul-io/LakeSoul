package org.apache.flink.lakesoul.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Map;

public class LakeSoulStreamSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    private final Configuration conf;
    private final ResolvedSchema schema;
    private final DataType physicalRowDataType;
    private final List<String> partitionKeys;


    public LakeSoulStreamSink(Configuration conf, ResolvedSchema schema, DataType physicalRowDataType, List<String> partitionKeys) {
        this.conf = conf;
        this.schema = schema;
        this.physicalRowDataType=physicalRowDataType;
        this.partitionKeys=partitionKeys;

    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode)  {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            builder.addContainedKind(kind);
        }
        return builder.build();
    }


    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return (DataStreamSinkProvider) dataStream -> {
            RowType rowType = (RowType) schema.toSourceRowDataType().notNull().getLogicalType();

            return LakeSoulSink.partitionSortWrite(conf,rowType,dataStream,physicalRowDataType,partitionKeys);
        };

    }




    @Override
    public DynamicTableSink copy() {
       return new LakeSoulStreamSink(conf, schema,physicalRowDataType,partitionKeys) ;
    }


    @Override
    public String asSummaryString(){
        return "lakesoul stream sink";
    }


    @Override
    public void applyOverwrite(boolean b) {

    }

    @Override
    public void applyStaticPartition(Map<String, String> map) {

    }
}
