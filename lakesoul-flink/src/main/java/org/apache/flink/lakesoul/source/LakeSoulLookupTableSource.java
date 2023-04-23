package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
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
import java.util.*;

import static org.apache.flink.lakesoul.tool.JobOptions.*;

public class LakeSoulLookupTableSource extends LakeSoulTableSource implements LookupTableSource, SupportsProjectionPushDown {

    protected DataType producedDataType;

    private final Configuration configuration;
    private Duration lakeSoulTableReloadInterval;

    protected ResolvedCatalogTable catalogTable;


    public LakeSoulLookupTableSource(TableId tableId, RowType rowType, boolean isStreaming, List<String> pkColumns, ResolvedCatalogTable catalogTable) {
        super(tableId, rowType, isStreaming, pkColumns, new HashMap<>());
        this.catalogTable = catalogTable;
        this.producedDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        this.configuration = new Configuration();
        catalogTable.getOptions().forEach(configuration::setString);
        validateLookupConfigurations();
    }

    private void validateLookupConfigurations() {
        String partitionInclude = configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE);

        if (isStreamingSource()) {
            Preconditions.checkArgument(
                    "latest".equals(partitionInclude),
                    String.format(
                            "The only supported %s for lookup is '%s' in streaming source,"
                                    + " but actual is '%s'",
                            STREAMING_SOURCE_PARTITION_INCLUDE.key(), "latest", partitionInclude));
        } else {
            Preconditions.checkArgument(
                    "all".equals(partitionInclude),
                    String.format(
                            "The only supported %s for lookup is '%s' in batch source,"
                                    + " but actual is '%s'",
                            STREAMING_SOURCE_PARTITION_INCLUDE.key(), "all", partitionInclude));

        }

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
        PartitionFetcher.Context<LakeSoulPartition> fetcherContext = new LakeSoulTablePartitionFetcherContext(tableId, catalogTable.getPartitionKeys(), configuration.get(PARTITION_ORDER_KEYS));
        int latestPartitionNumber = getLatestPartitionNumber();
        final PartitionFetcher<LakeSoulPartition> partitionFetcher;
        // TODO: support reading latest partition for streaming-read
        if (catalogTable.getPartitionKeys().isEmpty()) {
            // non-partitioned table, the fetcher fetches the partition which represents the given table
            partitionFetcher = context -> {
                List<LakeSoulPartition> partValueList = new ArrayList<>();

                partValueList.add(
                        context.getPartition(new ArrayList<>())
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        String.format(
                                                                "Fetch partition fail for lakesoul table %s.", ""
                                                        ))));
                return partValueList;
            };
        } else if(isStreamingSource()) {
            // streaming-read partitioned table, the fetcher fetches the latest partition of the given table
            partitionFetcher = context -> {
                List<LakeSoulPartition> partValueList = new ArrayList<>();
                List<PartitionFetcher.Context.ComparablePartitionValue> comparablePartitionValues = context.getComparablePartitionValueList();
                // fetch latest partitions for partitioned table
                if (comparablePartitionValues.size() > 0) {
                    // sort in desc order
                    comparablePartitionValues.sort(
                            (o1, o2) -> o2.getComparator().compareTo(o1.getComparator()));
                    List<PartitionFetcher.Context.ComparablePartitionValue> latestPartitions = new ArrayList<>();
                    // TODO: update code here
                    for (int i = 0; i < latestPartitionNumber && i < comparablePartitionValues.size(); i++) {
                        latestPartitions.add(comparablePartitionValues.get(i));
                    }
                    for (int i = latestPartitionNumber; i < comparablePartitionValues.size(); i++) {
                        if (comparablePartitionValues.get(i).getComparator().compareTo(latestPartitions.get(latestPartitionNumber - 1).getComparator()) != 0) {
                            break;
                        } else {
                            latestPartitions.add(comparablePartitionValues.get(i));
                        }
                    }
                    for (PartitionFetcher.Context.ComparablePartitionValue comparablePartitionValue: latestPartitions) {
                        partValueList.add(
                                context.getPartition(
                                                (List<String>)
                                                        comparablePartitionValue.getPartitionValue())
                                        .orElseThrow(
                                                () ->
                                                        new IllegalArgumentException(
                                                                String.format(
                                                                        "Fetch partition fail for lakesoul table %s.", ""
                                                                ))));
                    }
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "At least one partition is required when set '%s' to 'latest' in temporal join,"
                                            + " but actual partition number is '%s' for lakesoul table %s",
                                    STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                                    comparablePartitionValues.size(), ""));
                }

                System.out.println("[debug][yuchanghui] context fetch result is: ");
                for (LakeSoulPartition partition: partValueList) {
                    for (Path path: partition.getPaths()) System.out.println(path);
                    System.out.println("[debug][yuchanghui]-------------");
                }
                return partValueList;
            };
        } else {
            // bounded-read partitioned table, the fetcher fetches all partitions of the given table
            partitionFetcher = context -> {
                List<LakeSoulPartition> partValueList = new ArrayList<>();
                List<PartitionFetcher.Context.ComparablePartitionValue> comparablePartitionValues = context.getComparablePartitionValueList();
                for (PartitionFetcher.Context.ComparablePartitionValue comparablePartitionValue: comparablePartitionValues) {
                    partValueList.add(
                            context.getPartition(
                                            (List<String>)
                                                    comparablePartitionValue.getPartitionValue())
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            String.format(
                                                                    "Fetch partition fail for lakesoul table %s.", ""
                                                            ))));
                }
                System.out.println("[debug][yuchanghui] context fetch result is: ");
                for (LakeSoulPartition partition: partValueList) {
                    for (Path path: partition.getPaths()) System.out.println(path);
                    System.out.println("[debug][yuchanghui]-------------");
                }
                return partValueList;
            };

        }

        PartitionReader<LakeSoulPartition, RowData> partitionReader = new LakeSoulPartitionReader(readFields(""), this.pkColumns);

        return new LakeSoulTableLookupFunction<>(
                partitionFetcher,
                fetcherContext,
                partitionReader,
                readFields(""),
                keys,
                lakeSoulTableReloadInterval);
    }

    protected boolean isStreamingSource() {
        return Boolean.parseBoolean(
                catalogTable
                        .getOptions()
                        .getOrDefault(
                                STREAMING_SOURCE_ENABLE.key(),
                                STREAMING_SOURCE_ENABLE.defaultValue().toString()));
    }

    protected int getLatestPartitionNumber() {
        return Integer.parseInt(
                catalogTable
                        .getOptions()
                        .getOrDefault(
                                STREAMING_SOURCE_LATEST_PARTITION_NUMBER.key(),
                                STREAMING_SOURCE_LATEST_PARTITION_NUMBER.defaultValue().toString()));
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

        public LakeSoulTablePartitionFetcherContext(TableId tableId, List<String> partitionKeys, String partitionOrderKeys) {
            super(tableId, partitionKeys, partitionOrderKeys);
        }

        @Override
        public Optional<LakeSoulPartition> getPartition(List<String> partValues) throws Exception {
            Preconditions.checkArgument(
                    partitionKeys.size() == partValues.size(),
                    String.format(
                            "The partition keys length should equal to partition values length, "
                                    + "but partition keys length is %s and partition values length is %s",
                            partitionKeys.size(), partValues.size()));
            TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
            if (partValues.isEmpty()) {

//        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
                List<PartitionInfo> partitionInfos = DataOperation.dbManager().getAllPartitionInfo(tableInfo.getTableId());
                System.out.println("[debug][yuchanghui] partitionInfos is " + partitionInfos);
                if (partitionInfos.isEmpty()) return Optional.empty();
                partitionInfos.forEach(partitionInfo -> System.out.println(partitionInfo.getPartitionDesc()));
//
                DataFileInfo[] dataFileInfos = FlinkUtil.getTargetDataFileInfo(tableInfo, null);
                Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(tableId.table(), dataFileInfos);
                System.out.println(splitByRangeAndHashPartition);
                List<Path> paths = new ArrayList<>();
                splitByRangeAndHashPartition.forEach((rangeKey, rangeValue) -> {
                    rangeValue.forEach((hashKey, hashValue) -> {
                        hashValue.forEach(path -> paths.add(path));
                    });
                });

                return Optional.of(new LakeSoulPartition(paths, partitionKeys, partValues));
            } else {
                Map<String, String> kvs = new LinkedHashMap<>(100);
                for (int i = 0; i < partitionKeys.size(); i++) {
                    kvs.put(partitionKeys.get(i), partValues.get(i));
                }
                // TODO: update syntax here
                int len = kvs.toString().length();
                String partitionDesc = kvs.toString().substring(1, len - 1).replaceAll(" ", "");
                DataFileInfo[] dataFileInfos = FlinkUtil.getSinglePartitionDataFileInfo(tableInfo, partitionDesc);
                List<Path> paths = new ArrayList<>();
                for (DataFileInfo dif: dataFileInfos) paths.add(new Path(dif.path()));
                return Optional.of(new LakeSoulPartition(paths, partitionKeys, partValues));
            }
        }
    }

}