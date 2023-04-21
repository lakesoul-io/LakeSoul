package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.connector.LakeSoulPartition;
import org.apache.flink.lakesoul.connector.LakeSoulPartitionFetcherContextBase;
import org.apache.flink.lakesoul.connector.LakeSoulPartitionReader;
import org.apache.flink.lakesoul.table.LakeSoulTableLookupFunction;
import org.apache.flink.lakesoul.table.LakeSoulTableSource;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionFetcher;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.lakesoul.tool.JobOptions.LOOKUP_JOIN_CACHE_TTL;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.STREAMING_SOURCE_PARTITION_INCLUDE;


public class LakeSoulLookupTableSource extends LakeSoulTableSource implements LookupTableSource, SupportsProjectionPushDown {

    protected DataType producedDataType;

    private final Configuration configuration;
    private Duration lakeSoulTableReloadInterval;

    protected ResolvedCatalogTable catalogTable;

    public LakeSoulLookupTableSource(TableId tableId, RowType rowType, boolean isStreaming, List<String> pkColumns, ResolvedCatalogTable catalogTable) {
        super(tableId, rowType, isStreaming, pkColumns);
        this.catalogTable = catalogTable;
        this.producedDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        this.configuration = new Configuration();
        catalogTable.getOptions().forEach(configuration::setString);
        validateLookupConfigurations();
    }

    private void validateLookupConfigurations() {
        String partitionInclude = configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE);

        Preconditions.checkArgument(
                "all".equals(partitionInclude),
                String.format(
                        "The only supported %s for lookup is '%s' in batch source,"
                                + " but actual is '%s'",
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(), "all", partitionInclude));

        lakeSoulTableReloadInterval = configuration.get(LOOKUP_JOIN_CACHE_TTL);
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(getLookupFunction(context.getKeys()));
    }

    public TableFunction<RowData> getLookupFunction(int[][] keys) {
        int[] keyIndices = new int[keys.length];
        int i = 0;
        for (int[] key : keys) {
            if (key.length > 1) {
                throw new UnsupportedOperationException(
                        "Hive lookup can not support nested key now.");
            }
            keyIndices[i] = key[0];
            i++;
        }
        return getLookupFunction(keyIndices);
    }

    private TableFunction<RowData> getLookupFunction(int[] keys) {
        PartitionFetcher.Context<LakeSoulPartition> fetcherContext = new LakeSoulTablePartitionFetcherContext(tableId);

        final PartitionFetcher<LakeSoulPartition> partitionFetcher = context -> {
            List<LakeSoulPartition> partValueList = new ArrayList<>();

            partValueList.add(
                    context.getPartition(new ArrayList<>())
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    String.format(
                                                            "Fetch partition fail for hive table %s.", ""
                                                            ))));
            return partValueList;
        };
        PartitionReader<LakeSoulPartition, RowData> partitionReader = new LakeSoulPartitionReader(readFields(), this.pkColumns);

        return new LakeSoulTableLookupFunction<>(
                partitionFetcher,
                fetcherContext,
                partitionReader,
                readFields(),
                keys,
                lakeSoulTableReloadInterval);
    }


    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSource copy() {
        LakeSoulLookupTableSource lsts = new LakeSoulLookupTableSource(this.tableId, this.rowType, this.isStreaming, this.pkColumns, this.catalogTable);
        lsts.projectedFields = this.projectedFields;
        lsts.remainingPartitions = this.remainingPartitions;
        return lsts;
    }

    /**
     * Returns a string that summarizes this source for printing to a console or log.
     */
    @Override
    public String asSummaryString() {
        return null;
    }

    /** PartitionFetcher.Context for {@link LakeSoulPartition}. */
    static class LakeSoulTablePartitionFetcherContext
            extends LakeSoulPartitionFetcherContextBase<LakeSoulPartition> {

        private static final long serialVersionUID = 1L;

        public LakeSoulTablePartitionFetcherContext(TableId tableId) {
            super(tableId);
            System.out.println(this.tableId);
        }

        @Override
        public Optional<LakeSoulPartition> getPartition(List<String> partValues) throws Exception {
            TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
//        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
//
            DataFileInfo[] dataFileInfos = FlinkUtil.getTargetDataFileInfo(tableInfo, null);
//            System.out.println(dataFileInfos[0]);
//        int capacity = 100;
//        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
//        int i = 0;
            Map<String, Map<String, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(dataFileInfos);
            System.out.println(splitByRangeAndHashPartition);
            List<Path> paths = new ArrayList<>();
            splitByRangeAndHashPartition.forEach((rangeKey, rangeValue) -> {
                rangeValue.forEach((hashKey, hashValue) -> {
                    hashValue.forEach(path -> paths.add(path));
                });
            });

            return Optional.of(new LakeSoulPartition(paths));
//            return Optional.of(new LakeSoulPartition(splitByRangeAndHashPartition.get("-5").get("2")));
        }
    }

}
