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

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo;
import com.dmetasoul.lakesoul.meta.entity.DataFileOp;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.entity.TableNameId;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link LakeSoulMultiTablesSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link AbstractLakeSoulMultiTableSinkWriter}
 * and commit them, or put them in "finished" state and ready to be consumed by downstream
 * applications or systems.
 */
public class LakeSoulSinkCommitter implements Committer<LakeSoulMultiTableSinkCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSinkCommitter.class);

    private final BucketWriter<?, ?> bucketWriter;

    public LakeSoulSinkCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }

    @Override
    public List<LakeSoulMultiTableSinkCommittable> commit(List<LakeSoulMultiTableSinkCommittable> committables)
            throws IOException {
        LOG.info("Found {} committables for LakeSoul to commit", committables.size());
        // commit by file creation time in descending order
        committables.sort(LakeSoulMultiTableSinkCommittable::compareTo);

        DBManager lakeSoulDBManager = new DBManager();
        for (LakeSoulMultiTableSinkCommittable committable : committables) {
            LOG.info("Commtting {}", committable);
            if (committable.hasPendingFile()) {
                assert committable.getPendingFiles() != null;
                if (committable.getPendingFiles().isEmpty()) {
                    continue;
                }
                // commit files
                for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable :
                        committable.getPendingFiles()) {
                    // We should always use commitAfterRecovery which contains additional checks.
                    bucketWriter.recoverPendingFile(pendingFileRecoverable).commitAfterRecovery();
                }
                // commit LakeSoul Meta
                TableSchemaIdentity identity = committable.getIdentity();
                List<DataFileOp> dataFileOpList = new ArrayList<>();
                List<String> files = committable.getFilePaths();
                assert files != null;
                String fileExistCols = String.join(",",
                                                   identity.rowType.getFieldNames());
                for (String file : files) {
                    DataFileOp dataFileOp = new DataFileOp();
                    dataFileOp.setFileOp(LakeSoulSinkOptions.FILE_OPTION_ADD);
                    dataFileOp.setPath(file);
                    Path path = new Path(file);
                    FileStatus fileStatus = FileSystem.get(path.toUri()).getFileStatus(path);
                    dataFileOp.setSize(fileStatus.getLen());
                    dataFileOp.setFileExistCols(fileExistCols);
                    dataFileOpList.add(dataFileOp);
                }
                String partition = committable.getBucketId();

                TableNameId tableNameId = lakeSoulDBManager.shortTableName(identity.tableId.table(),
                                                                           identity.tableId.schema());

                DataCommitInfo dataCommitInfo = new DataCommitInfo();
                dataCommitInfo.setTableId(tableNameId.getTableId());
                dataCommitInfo.setPartitionDesc(partition.isEmpty() ? "-5" : partition);
                dataCommitInfo.setFileOps(dataFileOpList);
                dataCommitInfo.setCommitOp(LakeSoulSinkOptions.APPEND_COMMIT_TYPE);
                dataCommitInfo.setTimestamp(System.currentTimeMillis());
                assert committable.getCommitId() != null;
                dataCommitInfo.setCommitId(UUID.fromString(committable.getCommitId()));

                if (LOG.isInfoEnabled()) {
                    String fileOpStr = dataFileOpList.stream().map(
                            op -> String.format("%s,%s,%d,%s",
                                                op.getPath(), op.getFileOp(),
                                                op.getSize(), op.getFileExistCols()))
                                                     .collect(Collectors.joining("\n\t"));
                    LOG.info("Commit to LakeSoul: Table={}, TableId={}, Partition={}, Files:\n\t{}, " +
                             "CommitOp={}, Timestamp={}, UUID={}",
                             identity.tableId.identifier(), tableNameId.getTableId(), partition,
                             fileOpStr, dataCommitInfo.getCommitOp(),
                             dataCommitInfo.getTimestamp(), dataCommitInfo.getCommitId().toString()
                             );
                }

                lakeSoulDBManager.commitDataCommitInfo(dataCommitInfo);
            }

            if (committable.hasInProgressFileToCleanup()) {
                bucketWriter.cleanupInProgressFileRecoverable(
                        committable.getInProgressFileToCleanup());
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
