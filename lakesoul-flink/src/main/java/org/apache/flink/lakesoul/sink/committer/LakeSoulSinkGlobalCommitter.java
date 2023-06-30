/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.committer;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkGlobalCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.dmetasoul.lakesoul.meta.DBConfig.*;
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
public class LakeSoulSinkGlobalCommitter implements GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSinkGlobalCommitter.class);

    private final LakeSoulSinkCommitter committer;
    private final DBManager dbManager;
    private final Configuration conf;

    public LakeSoulSinkGlobalCommitter(Configuration conf) {
        committer = LakeSoulSinkCommitter.INSTANCE;
        dbManager = new DBManager();
        this.conf = conf;
    }


    @Override
    public void close() throws Exception {
        // Do nothing.
    }

    /**
     * Find out which global committables need to be retried when recovering from the failure.
     *
     * @param globalCommittables A list of {@link LakeSoulMultiTableSinkGlobalCommittable} for which we want to verify which
     *                           ones were successfully committed and which ones did not.
     * @return A list of {@link LakeSoulMultiTableSinkGlobalCommittable} that should be committed again.
     * @throws IOException if fail to filter the recovered committables.
     */
    @Override
    public List<LakeSoulMultiTableSinkGlobalCommittable> filterRecoveredCommittables(List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    /**
     * Compute an aggregated committable from a list of committables.
     *
     * @param committables A list of {@link LakeSoulMultiTableSinkCommittable} to be combined into a {@link LakeSoulMultiTableSinkGlobalCommittable}.
     * @return an aggregated committable
     * @throws IOException if fail to combine the given committables.
     */
    @Override
    public LakeSoulMultiTableSinkGlobalCommittable combine(List<LakeSoulMultiTableSinkCommittable> committables) throws IOException {
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
    public List<LakeSoulMultiTableSinkGlobalCommittable> commit(List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables) throws IOException, InterruptedException {
        LakeSoulMultiTableSinkGlobalCommittable globalCommittable = LakeSoulMultiTableSinkGlobalCommittable.fromLakeSoulMultiTableSinkGlobalCommittable(globalCommittables);

        for (Map.Entry<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> entry : globalCommittable.getGroupedCommitables().entrySet()) {
            TableSchemaIdentity identity = entry.getKey().f0;
            String tableName = identity.tableId.table();
            String tableNamespace = identity.tableId.schema();
            Boolean isCdc = Boolean.valueOf(identity.properties.getOrDefault(USE_CDC.key(), "false").toString());
            String sparkSchema = FlinkUtil.toSparkSchema(identity.rowType, isCdc ? Optional.of(identity.properties.getOrDefault(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT).toString()) : Optional.empty()).json();
            TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(tableName, tableNamespace);
            if (tableInfo == null) {
                String tableId = TABLE_ID_PREFIX + UUID.randomUUID();
                String partition = String.join(LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH, String.join(LAKESOUL_RANGE_PARTITION_SPLITTER, identity.partitionKeyList), String.join(LAKESOUL_HASH_PARTITION_SPLITTER, identity.primaryKeys));

                dbManager.createNewTable(
                        tableId,
                        tableNamespace,
                        tableName,
                        identity.tableLocation,
                        sparkSchema,
                        identity.properties,
                        partition
                );
            } else if (!tableInfo.getTableSchema().equals(sparkSchema)) {
                // TODO: 2023/6/15 order of schema changes should be considered
                dbManager.updateTableSchema(tableInfo.getTableId(), sparkSchema);
            }

            committer.commit(entry.getValue());
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
