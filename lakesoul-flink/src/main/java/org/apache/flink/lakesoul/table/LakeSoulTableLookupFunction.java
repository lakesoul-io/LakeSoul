// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.file.table.PartitionFetcher;
import org.apache.flink.connector.file.table.PartitionReader;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LakeSoulTableLookupFunction<P> extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulTableLookupFunction.class);

    // the max number of retries before throwing exception, in case of failure to load the table into cache
    private static final int MAX_RETRIES = 3;
    // interval between retries
    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(5);

    private final long cacheMaxSize;
    private final TableId tableId;

    private final TypeSerializer<RowData> serializer;

    private final RowData.FieldGetter[] lookupFieldGetters;
    private final Duration reloadInterval;

    private final RowType rowType;

    private final PartitionFetcher<P> partitionFetcher;
    private final PartitionFetcher.Context<P> fetcherContext;

    private final PartitionReader<P, RowData> partitionReader;

    // cache for lookup data
    private transient Map<RowData, List<RowData>> cache;

    // timestamp when cache expires
    private transient long nextLoadTime;

    public LakeSoulTableLookupFunction(
            TableId tableId,
            PartitionFetcher<P> partitionFetcher,
            PartitionFetcher.Context<P> fetcherContext,
            PartitionReader<P, RowData> partitionReader,
            RowType rowType,
            int[] lookupKeys,
            Duration reloadInterval,
            long cacheMaxSize) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.reloadInterval = reloadInterval;
        this.fetcherContext = fetcherContext;
        this.partitionFetcher = partitionFetcher;
        this.partitionReader = partitionReader;
        this.lookupFieldGetters = new RowData.FieldGetter[lookupKeys.length];
        for (int i = 0; i < lookupKeys.length; i++) {
            lookupFieldGetters[i] =
                    RowData.createFieldGetter(rowType.getTypeAt(lookupKeys[i]), lookupKeys[i]);
        }
        this.serializer = InternalSerializers.create(rowType);
        this.cacheMaxSize = cacheMaxSize;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        cache = new HashMap<>();
        nextLoadTime = -1L;
        fetcherContext.open();
    }

    public void eval(Object... values) {
        checkCacheReload();
        RowData lookupKey = GenericRowData.of(values);
        List<RowData> matchedRows = cache.get(lookupKey);
        if (matchedRows != null) {
            for (RowData matchedRow : matchedRows) {
                collect(matchedRow);
            }
        }
    }

    private void checkCacheReload() {
        if (nextLoadTime > System.currentTimeMillis()) {
            return;
        }
        if (nextLoadTime > 0) {
            LOG.info(
                    "Lookup join cache for {} has expired after {}, reloading, cache max size {}",
                    tableId,
                    reloadInterval,
                    cacheMaxSize);
        } else {
            LOG.info("Populating lookup join cache for {}, cache max size {}, interval {}",
                    tableId,  cacheMaxSize, reloadInterval);
        }
        int numRetry = 0;
        // load data from lakesoul to cache
        while (true) {
            cache.clear();
            try {
                long count = 0;
                GenericRowData reuse = new GenericRowData(rowType.getFieldCount());
                partitionReader.
                        open(partitionFetcher.fetch(fetcherContext));
                RowData row;
                while ((row = partitionReader.read(reuse)) != null) {
                    count++;
                    RowData rowData = serializer.copy(row);
                    RowData key = extractLookupKey(rowData);
                    List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
                    rows.add(rowData);

                    if (cache.size() >= this.cacheMaxSize) {
                        String err = String.format(
                                "Lookup Cache for %s has too many rows than cache limit %s",
                                tableId.toString(),
                                cacheMaxSize);
                        LOG.error(err);
                        throw new SuppressRestartsException(new IllegalStateException(err));
                    }
                }
                partitionReader.close();
                nextLoadTime = System.currentTimeMillis() + reloadInterval.toMillis();
                LOG.info("Loaded {} row(s) into lookup join cache for {}, next load time {}",
                        count, tableId, nextLoadTime);
                return;
            } catch (Exception e) {
                if (numRetry >= MAX_RETRIES) {
                    throw new SuppressRestartsException(new FlinkRuntimeException(
                            String.format(
                                    "Failed to load table into cache after %d retries", numRetry),
                            e));
                }
                numRetry++;
                long toSleep = numRetry * RETRY_INTERVAL.toMillis();
                LOG.warn("Failed to load table {} into cache, will retry in {} seconds", tableId, toSleep / 1000, e);
                try {
                    Thread.sleep(toSleep);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while waiting to retry failed cache load for {}, aborting", tableId, ex);
                    throw new FlinkRuntimeException(ex);
                }
            }
        }
    }

    private RowData extractLookupKey(RowData row) {
        GenericRowData key = new GenericRowData(lookupFieldGetters.length);
        for (int i = 0; i < lookupFieldGetters.length; i++) {
            key.setField(i, lookupFieldGetters[i].getFieldOrNull(row));
        }
        return key;
    }

    @Override
    public void close() throws Exception {
        this.fetcherContext.close();
    }

    @VisibleForTesting
    public Duration getReloadInterval() {
        return reloadInterval;
    }

    @VisibleForTesting
    public PartitionFetcher<P> getPartitionFetcher() {
        return partitionFetcher;
    }

    @VisibleForTesting
    public PartitionFetcher.Context<P> getFetcherContext() {
        return fetcherContext;
    }

    @VisibleForTesting
    public PartitionReader<P, RowData> getPartitionReader() {
        return partitionReader;
    }
}
