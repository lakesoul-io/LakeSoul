// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.PartitionFetcher;
import org.apache.flink.connector.file.table.PartitionReader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.connector.LakeSoulPartition;
import org.apache.flink.lakesoul.connector.LakeSoulPartitionFetcherContextBase;
import org.apache.flink.lakesoul.connector.LakeSoulPartitionReader;
import org.apache.flink.lakesoul.table.LakeSoulTableLookupFunction;
import org.apache.flink.lakesoul.table.LakeSoulTableSource;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER;
import static org.apache.flink.lakesoul.tool.JobOptions.*;

public class LakeSoulLookupTableSource extends LakeSoulTableSource implements LookupTableSource {

    private final Configuration configuration;
    protected DataType producedDataType;
    protected ResolvedCatalogTable catalogTable;
    private Duration lakeSoulTableReloadInterval;


    public LakeSoulLookupTableSource(TableId tableId,
                                     RowType rowType,
                                     boolean isStreaming,
                                     List<String> pkColumns,
                                     List<String> partitionColumns,
                                     ResolvedCatalogTable catalogTable,
                                     Map<String, String> optionParams) {
        super(tableId,
                rowType,
                isStreaming,
                pkColumns,
                partitionColumns,
                optionParams);
        this.catalogTable = catalogTable;
        this.producedDataType = catalogTable.getResolvedSchema().toPhysicalRowDataType();
        this.configuration = new Configuration();
        catalogTable.getOptions().forEach(configuration::setString);
        validateLookupConfigurations();
    }

    private void validateLookupConfigurations() {
        String partitionInclude = configuration.get(STREAMING_SOURCE_PARTITION_INCLUDE);

        if (!isStreamingSource()) {
            Preconditions.checkArgument("all".equals(partitionInclude),
                    String.format("The only supported %s for lookup is '%s' in batch source," + " but actual is '%s'",
                            STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                            "all",
                            partitionInclude));

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
                throw new UnsupportedOperationException("Hive lookup can not support nested key now.");
            }
            keyIndices[i] = key[0];
            i++;
        }
        return getLookupFunction(keyIndices);
    }

    private TableFunction<RowData> getLookupFunction(int[] keys) {
        PartitionFetcher.Context<LakeSoulPartition> fetcherContext =
                new LakeSoulTablePartitionFetcherContext(tableId,
                        catalogTable.getPartitionKeys(),
                        configuration.get(PARTITION_ORDER_KEYS));
        int latestPartitionNumber = getLatestPartitionNumber(); // the number of latest partitions to fetch
        final PartitionFetcher<LakeSoulPartition> partitionFetcher;

        // TODO: delete the option
        int simpleLatestPartition = 1; // 1: true; other: false

        if (catalogTable.getPartitionKeys().isEmpty()) {
            // non-partitioned table, the fetcher fetches the partition which represents the given table
            partitionFetcher = context -> {
                List<LakeSoulPartition> partValueList = new ArrayList<>();

                context.getPartition(new ArrayList<>()).map(partValueList::add);
                return partValueList;
            };
        } else if (isStreamingSource() && isReadingLatest()) {
            // streaming-read partitioned table which is set to read latest partition, the fetcher fetches the latest partition of the given table
            partitionFetcher = context -> {
                List<LakeSoulPartition> partValueList = new ArrayList<>();
                List<PartitionFetcher.Context.ComparablePartitionValue> comparablePartitionValues =
                        context.getComparablePartitionValueList();
                String partitionLowerLimit = LocalDateTime.now().plusMinutes(-latestPartitionNumber + 1)
                        .format(DateTimeFormatter.ofPattern("yyyy,MM,dd,HH,mm"));
                // fetch latest partitions for partitioned table
                if (comparablePartitionValues.size() > 0) {
                    // sort in desc order
                    comparablePartitionValues.sort((o1, o2) -> o2.getComparator().compareTo(o1.getComparator()));
                    List<PartitionFetcher.Context.ComparablePartitionValue> latestPartitions = new ArrayList<>();

                    // TODO: update logic here
                    if (simpleLatestPartition == 1) {
                        for (int i = 0; i < latestPartitionNumber && i < comparablePartitionValues.size(); i++) {
                            latestPartitions.add(comparablePartitionValues.get(i));
                        }
                        for (int i = latestPartitionNumber; i < comparablePartitionValues.size(); i++) {
                            if (comparablePartitionValues.get(i).getComparator()
                                    .compareTo(latestPartitions.get(latestPartitionNumber - 1).getComparator()) != 0) {
                                break;
                            } else {
                                latestPartitions.add(comparablePartitionValues.get(i));
                            }
                        }
                    } else {
                        for (int i = 0; i < comparablePartitionValues.size(); i++) {
                            if (comparablePartitionValues.get(i).getComparator().compareTo(partitionLowerLimit) >= 0)
                                latestPartitions.add(comparablePartitionValues.get(i));
                        }
                    }

                    for (PartitionFetcher.Context.ComparablePartitionValue comparablePartitionValue : latestPartitions) {
                        context.getPartition((List<String>) comparablePartitionValue.getPartitionValue())
                                .map(partValueList::add);
                    }
                } else {
                    throw new IllegalArgumentException(String.format(
                            "At least one partition is required when set '%s' to 'latest' in temporal join," +
                                    " but actual partition number is '%s' for lakesoul table %s",
                            STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                            comparablePartitionValues.size(),
                            ""));
                }

                return partValueList;
            };
        } else {
            // bounded-read partitioned table, or streaming-read partitioned table which is set to read latest partition, the fetcher fetches all partitions of the given table
            partitionFetcher = context -> {
                List<LakeSoulPartition> partValueList = new ArrayList<>();
                List<PartitionFetcher.Context.ComparablePartitionValue> comparablePartitionValues =
                        context.getComparablePartitionValueList();
                for (PartitionFetcher.Context.ComparablePartitionValue comparablePartitionValue : comparablePartitionValues) {
                    context.getPartition((List<String>) comparablePartitionValue.getPartitionValue())
                            .map(partValueList::add);
                }
                return partValueList;
            };

        }

        PartitionReader<LakeSoulPartition, RowData> partitionReader =
                new LakeSoulPartitionReader(this.configuration,
                        readFields(),
                        this.pkColumns);

        return new LakeSoulTableLookupFunction<>(partitionFetcher,
                fetcherContext,
                partitionReader,
                readFields(),
                keys,
                lakeSoulTableReloadInterval);
    }

    protected List<String> getPartitionKeys() {
        List<String> res = new ArrayList<>();
        for (int i = 0; i < catalogTable.getPartitionKeys().size(); i++) {
            res.add(catalogTable.getPartitionKeys().get(i));
        }
        return res;
    }

    protected boolean isStreamingSource() {
        return Boolean.parseBoolean(catalogTable.getOptions()
                .getOrDefault(STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_ENABLE.defaultValue().toString()));
    }

    protected int getLatestPartitionNumber() {
        return Integer.parseInt(catalogTable.getOptions().getOrDefault(STREAMING_SOURCE_LATEST_PARTITION_NUMBER.key(),
                STREAMING_SOURCE_LATEST_PARTITION_NUMBER.defaultValue().toString()));
    }

    protected boolean isReadingLatest() {
        return catalogTable.getOptions().getOrDefault(STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                STREAMING_SOURCE_PARTITION_INCLUDE.defaultValue()).equals("latest");
    }

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSource copy() {
        LakeSoulLookupTableSource newInstance =
                new LakeSoulLookupTableSource(this.tableId,
                        this.tableRowType,
                        this.isBounded,
                        this.pkColumns,
                        this.partitionColumns,
                        this.catalogTable,
                        this.optionParams);
        newInstance.projectedFields = this.projectedFields;
        newInstance.remainingPartitions = this.remainingPartitions;
        newInstance.pushedFilters = this.pushedFilters;
        newInstance.modificationContext = this.modificationContext;
        newInstance.partitionFilters = this.partitionFilters;
        return newInstance;
    }

    /**
     * Returns a string that summarizes this source for printing to a console or log.
     */
    @Override
    public String asSummaryString() {
        return null;
    }

    /**
     * PartitionFetcher.Context for {@link LakeSoulPartition}.
     */
    static class LakeSoulTablePartitionFetcherContext extends LakeSoulPartitionFetcherContextBase<LakeSoulPartition> {

        private static final long serialVersionUID = 1L;

        public LakeSoulTablePartitionFetcherContext(TableId tableId,
                                                    List<String> partitionKeys,
                                                    String partitionOrderKeys) {
            super(tableId,
                    partitionKeys,
                    partitionOrderKeys);
        }

        @Override
        public Optional<LakeSoulPartition> getPartition(List<String> partValues) throws Exception {
            Preconditions.checkArgument(partitionKeys.size() == partValues.size(),
                    String.format(
                            "The partition keys length should equal to partition values length, " +
                                    "but partition keys length is %s and partition values length is %s",
                            partitionKeys.size(),
                            partValues.size()));
            TableInfo tableInfo =
                    DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(),
                            tableId.schema());
            if (partValues.isEmpty()) {
                List<PartitionInfo> partitionInfos =
                        DataOperation.dbManager().getAllPartitionInfo(tableInfo.getTableId());
                if (partitionInfos.isEmpty()) return Optional.empty();

                DataFileInfo[] dataFileInfos = FlinkUtil.getTargetDataFileInfo(tableInfo,
                        null);
                List<Path> paths = new ArrayList<>();
                for (DataFileInfo dfi : dataFileInfos) paths.add(new Path(dfi.path()));

                return Optional.of(new LakeSoulPartition(paths,
                        partitionKeys,
                        partValues));
            } else {
                List<String> kvPairs = new ArrayList<>();
                for (int i = 0; i < partitionKeys.size(); i++) {
                    kvPairs.add(String.join("=",
                            partitionKeys.get(i),
                            partValues.get(i)));
                }
                String partitionDesc = String.join(LAKESOUL_RANGE_PARTITION_SPLITTER,
                        kvPairs);
                DataFileInfo[] dataFileInfos = FlinkUtil.getSinglePartitionDataFileInfo(tableInfo,
                        partitionDesc);
                List<Path> paths = new ArrayList<>();
                for (DataFileInfo dif : dataFileInfos) paths.add(new Path(dif.path()));

                return Optional.of(new LakeSoulPartition(paths,
                        partitionKeys,
                        partValues));
            }
        }
    }
}