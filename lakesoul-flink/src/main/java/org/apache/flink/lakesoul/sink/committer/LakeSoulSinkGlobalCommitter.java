// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.committer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.dao.TableInfoDao;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkGlobalCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.spark.sql.arrow.ArrowUtils;
import org.apache.spark.sql.arrow.DataTypeCastUtils;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_HASH_PARTITION_SPLITTER;
import static org.apache.flink.lakesoul.metadata.LakeSoulCatalog.TABLE_ID_PREFIX;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

/**
 * Global Committer implementation for {@link LakeSoulMultiTablesSink}.
 *
 * <p>This global committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link AbstractLakeSoulMultiTableSinkWriter}
 * and commit them globally, or put them in "finished" state and ready to be consumed by downstream
 * applications or systems.
 */
public class LakeSoulSinkGlobalCommitter
        implements GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSinkGlobalCommitter.class);

    private final LakeSoulSinkCommitter committer;
    private final DBManager dbManager;
    private final Configuration conf;

    private final boolean logicallyDropColumn;

    public LakeSoulSinkGlobalCommitter(Configuration conf) {
        committer = LakeSoulSinkCommitter.INSTANCE;
        dbManager = new DBManager();
        this.conf = conf;
        logicallyDropColumn = conf.getBoolean(LOGICALLY_DROP_COLUM);
    }


    @Override
    public void close() throws Exception {
        // Do nothing.
    }

    /**
     * Find out which global committables need to be retried when recovering from the failure.
     *
     * @param globalCommittables A list of {@link LakeSoulMultiTableSinkGlobalCommittable} for which we want to
     *                           verify which
     *                           ones were successfully committed and which ones did not.
     * @return A list of {@link LakeSoulMultiTableSinkGlobalCommittable} that should be committed again.
     * @throws IOException if fail to filter the recovered committables.
     */
    @Override
    public List<LakeSoulMultiTableSinkGlobalCommittable> filterRecoveredCommittables(
            List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    /**
     * Compute an aggregated committable from a list of committables.
     *
     * @param committables A list of {@link LakeSoulMultiTableSinkCommittable} to be combined into a
     *                     {@link LakeSoulMultiTableSinkGlobalCommittable}.
     * @return an aggregated committable
     * @throws IOException if fail to combine the given committables.
     */
    @Override
    public LakeSoulMultiTableSinkGlobalCommittable combine(List<LakeSoulMultiTableSinkCommittable> committables)
            throws IOException {
        return LakeSoulMultiTableSinkGlobalCommittable.fromLakeSoulMultiTableSinkCommittable(committables);
    }

    /**
     * Commit the given list of {@link LakeSoulMultiTableSinkGlobalCommittable}.
     *
     * @param globalCommittables a list of {@link LakeSoulMultiTableSinkGlobalCommittable}.
     * @return A list of {@link LakeSoulMultiTableSinkGlobalCommittable} needed to re-commit, which is needed in case we
     * implement a "commit-with-retry" pattern.
     * @throws IOException if the commit operation fail and do not want to retry any more.
     */
    @Override
    public List<LakeSoulMultiTableSinkGlobalCommittable> commit(
            List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) throws IOException, InterruptedException {
        LakeSoulMultiTableSinkGlobalCommittable globalCommittable =
                LakeSoulMultiTableSinkGlobalCommittable.fromLakeSoulMultiTableSinkGlobalCommittable(globalCommittables);
        LOG.info("Committing: {}", globalCommittable);

        int index = 0;
        for (Map.Entry<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> entry :
                globalCommittable.getGroupedCommitables()
                        .entrySet()) {
            TableSchemaIdentity identity = entry.getKey().f0;
            List<LakeSoulMultiTableSinkCommittable> lakeSoulMultiTableSinkCommittable = entry.getValue();
            String tableName = identity.tableId.table();
            String tableNamespace = identity.tableId.schema();
            boolean isCdc = identity.useCDC;
            Schema msgSchema = FlinkUtil.toArrowSchema(identity.rowType, isCdc ? Optional.of(
                    identity.cdcColumn) :
                    Optional.empty());
            StructType sparkSchema = ArrowUtils.fromArrowSchema(msgSchema);

            TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(tableName, tableNamespace);
            LOG.info("Committing: {}, {}, {}, {} {}", tableNamespace, tableName, isCdc, msgSchema, tableInfo);
            if (tableInfo == null) {
                String tableId = TABLE_ID_PREFIX + UUID.randomUUID();
                String partition = DBUtil.formatTableInfoPartitionsField(identity.primaryKeys,
                        identity.partitionKeyList);

                LOG.info("Creating table: {}, {}, {}, {}, {}, {}, {}, {}", tableId, tableNamespace, tableName,
                        identity.tableLocation, msgSchema, identity.useCDC, identity.cdcColumn, partition);
                JSONObject properties = new JSONObject();
                if (!identity.primaryKeys.isEmpty()) {
                    properties.put(HASH_BUCKET_NUM.key(), Integer.toString(conf.getInteger(BUCKET_PARALLELISM)));
                    properties.put(HASH_PARTITIONS, String.join(LAKESOUL_HASH_PARTITION_SPLITTER, identity.primaryKeys));
                    if (isCdc) {
                        properties.put(USE_CDC.key(), "true");
                        properties.put(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT);
                    }
                }
                dbManager.createNewTable(tableId, tableNamespace, tableName, identity.tableLocation, msgSchema.toJson(),
                        properties, partition);
            } else {
                DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
                if (partitionKeys.primaryKeys.size() != identity.primaryKeys.size() ||
                        !new HashSet<>(partitionKeys.primaryKeys).containsAll(identity.primaryKeys)) {
                    throw new IOException("Change of primary key column of table " + tableName + " is forbidden");
                }
                if (partitionKeys.rangeKeys.size() != identity.partitionKeyList.size() ||
                        !new HashSet<>(partitionKeys.rangeKeys).containsAll(identity.partitionKeyList)) {
                    throw new IOException("Change of partition key column of table " + tableName + " is forbidden");
                }
                StructType origSchema = null;
                if (TableInfoDao.isArrowKindSchema(tableInfo.getTableSchema())) {
                    Schema arrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
                    origSchema = ArrowUtils.fromArrowSchema(arrowSchema);
                } else {
                    origSchema = (StructType) StructType.fromJson(tableInfo.getTableSchema());
                }
                scala.Tuple3<String, Object, StructType>
                        equalOrCanCastTuple3 =
                        DataTypeCastUtils.checkSchemaEqualOrCanCast(origSchema,
                                ArrowUtils.fromArrowSchema(msgSchema),
                                identity.partitionKeyList,
                                identity.primaryKeys);
                String equalOrCanCast = equalOrCanCastTuple3._1();
                boolean schemaChanged = (boolean) equalOrCanCastTuple3._2();
                StructType mergeStructType = equalOrCanCastTuple3._3();
                if (equalOrCanCast.equals(DataTypeCastUtils.CAN_CAST())) {
                    LOG.warn("Schema change found, origin schema = {}, changed schema = {}",
                            origSchema.json(),
                            msgSchema.toJson());
                    if (logicallyDropColumn) {
                        List<String> droppedColumn = DataTypeCastUtils.getDroppedColumn(origSchema, sparkSchema);
                        if (droppedColumn.size() > 0) {
                            LOG.warn("Dropping Column {} Logically", droppedColumn);
                            dbManager.logicallyDropColumn(tableInfo.getTableId(), droppedColumn);
                            if (schemaChanged) {
                                dbManager.updateTableSchema(tableInfo.getTableId(), mergeStructType.json());
                            }
                        } else {
                            dbManager.updateTableSchema(tableInfo.getTableId(), msgSchema.toJson());
                        }
                    } else {
                        LOG.info("Changing table schema: {}, {}, {}, {}, {}, {}",
                                tableNamespace,
                                tableName,
                                identity.tableLocation,
                                msgSchema,
                                identity.useCDC,
                                identity.cdcColumn);
                        dbManager.updateTableSchema(tableInfo.getTableId(), msgSchema.toJson());
                        if (JSONObject.parseObject(tableInfo.getProperties()).containsKey(DBConfig.TableInfoProperty.DROPPED_COLUMN)) {
                            dbManager.removeLogicallyDropColumn(tableInfo.getTableId());
                        }
                    }
                } else if (!equalOrCanCast.equals(DataTypeCastUtils.IS_EQUAL())) {
                    long schemaLastChangeTime = JSON.parseObject(tableInfo.getProperties()).getLong(DBConfig.TableInfoProperty.LAST_TABLE_SCHEMA_CHANGE_TIME);
                    if (equalOrCanCast.contains("Change of Partition Column") || equalOrCanCast.contains("Change of Primary Key Column")) {
                        throw new IOException(equalOrCanCast);
                    }
                    for (LakeSoulMultiTableSinkCommittable committable : lakeSoulMultiTableSinkCommittable) {
                        if (committable.getTsMs() > schemaLastChangeTime) {
                            LOG.error("incompatible cast data created and delayThreshold time: {}, dml create time: {}", schemaLastChangeTime, committable.getTsMs());
                            throw new IOException(equalOrCanCast);
                        }
                    }
                }
            }

            committer.commit(lakeSoulMultiTableSinkCommittable);
        }
        return Collections.emptyList();
    }

    /**
     * Signals that there is no committable any more.
     */
    @Override
    public void endOfInput() {
        // do nothing
    }
}
