// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.committer;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.*;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.NativeParquetWriter;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.meta.DBConfig.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SORT_FIELD;

/**
 * Committer implementation for {@link LakeSoulMultiTablesSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending"
 * state, created by the {@link AbstractLakeSoulMultiTableSinkWriter}
 * and commit them, or put them in "finished" state and ready to be consumed by downstream
 * applications or systems.
 */
public class LakeSoulSinkCommitter implements Committer<LakeSoulMultiTableSinkCommittable> {

    public static final LakeSoulSinkCommitter INSTANCE = new LakeSoulSinkCommitter();
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulSinkCommitter.class);

    public LakeSoulSinkCommitter() {
    }

    @Override
    public List<LakeSoulMultiTableSinkCommittable> commit(List<LakeSoulMultiTableSinkCommittable> committables)
            throws IOException {
        LOG.info("Found {} committables for LakeSoul to commit", committables.size());
        // commit by file creation time in ascending order
        committables.sort(LakeSoulMultiTableSinkCommittable::compareTo);

        DBManager lakeSoulDBManager = new DBManager();
        for (LakeSoulMultiTableSinkCommittable committable : committables) {
            LOG.info("Commtting {}", committable);
            if (committable.hasPendingFile()) {
                assert committable.getPendingFiles() != null;
                LOG.info("PendingFiles to commit {}", committable.getPendingFiles().size());
                if (committable.getPendingFiles().isEmpty()) {
                    continue;
                }

                // pending files to commit
                List<String> files = new ArrayList<>();
                for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable :
                        committable.getPendingFiles()) {
                    if (pendingFileRecoverable instanceof NativeParquetWriter.NativeWriterPendingFileRecoverable) {
                        NativeParquetWriter.NativeWriterPendingFileRecoverable recoverable =
                                (NativeParquetWriter.NativeWriterPendingFileRecoverable) pendingFileRecoverable;
                        files.add(recoverable.path);
                    }
                }

                LOG.info("Files to commit {}", String.join("; ", files));

                if (files.isEmpty()) continue;

                // commit LakeSoul Meta
                TableSchemaIdentity identity = committable.getIdentity();
                List<DataFileOp> dataFileOpList = new ArrayList<>();
                String fileExistCols =
                        identity.rowType.getFieldNames().stream().filter(name -> !name.equals(SORT_FIELD))
                                .collect(Collectors.joining(LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER));
                for (String file : files) {
                    DataFileOp.Builder dataFileOp = DataFileOp.newBuilder();
                    dataFileOp.setFileOp(FileOp.add);
                    dataFileOp.setPath(file);
                    Path path = new Path(file);
                    FileStatus fileStatus = FileSystem.get(path.toUri()).getFileStatus(path);
                    dataFileOp.setSize(fileStatus.getLen());
                    dataFileOp.setFileExistCols(fileExistCols);
                    dataFileOpList.add(dataFileOp.build());
                }
                String partition = committable.getBucketId();

                TableNameId tableNameId =
                        lakeSoulDBManager.shortTableName(identity.tableId.table(), identity.tableId.schema());

                DataCommitInfo.Builder dataCommitInfo = DataCommitInfo.newBuilder();
                dataCommitInfo.setTableId(tableNameId.getTableId());
                dataCommitInfo.setPartitionDesc(partition.isEmpty() ? LAKESOUL_NON_PARTITION_TABLE_PART_DESC :
                        partition.replaceAll("/", LAKESOUL_RANGE_PARTITION_SPLITTER));
                dataCommitInfo.addAllFileOps(dataFileOpList);
                if(LakeSoulSinkOptions.DELETE.equals(committable.getDmlType())){
                    dataCommitInfo.setCommitOp(CommitOp.UpdateCommit);
                }else{
                    dataCommitInfo.setCommitOp(CommitOp.AppendCommit);
                }
                dataCommitInfo.setTimestamp(System.currentTimeMillis());
                assert committable.getCommitId() != null;
                dataCommitInfo.setCommitId(DBUtil.toProtoUuid(UUID.fromString(committable.getCommitId())));

                if (LOG.isInfoEnabled()) {
                    String fileOpStr = dataFileOpList.stream()
                            .map(op -> String.format("%s,%s,%d,%s", op.getPath(), op.getFileOp(), op.getSize(),
                                    op.getFileExistCols())).collect(Collectors.joining("\n\t"));
                    LOG.info("Commit to LakeSoul: Table={}, TableId={}, Partition={}, Files:\n\t{}, " +
                                    "CommitOp={}, Timestamp={}, UUID={}", identity.tableId.identifier(),
                            tableNameId.getTableId(), partition, fileOpStr, dataCommitInfo.getCommitOp(),
                            dataCommitInfo.getTimestamp(), dataCommitInfo.getCommitId().toString());
                }

                lakeSoulDBManager.commitDataCommitInfo(dataCommitInfo.build());
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        // Do nothing.
    }
}
