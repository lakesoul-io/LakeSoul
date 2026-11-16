// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.dao.DataCommitInfoDao;
import com.dmetasoul.lakesoul.meta.dao.PartitionInfoDao;
import com.dmetasoul.lakesoul.meta.entity.CommitOp;
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo;
import com.dmetasoul.lakesoul.meta.entity.MetaInfo;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.entity.Uuid;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

/**
 * Trace conformance for the JDBC metadata commit coordinator.
 *
 * <p>These {@link DBManager#commitData} traces mirror the native coordinator's
 * optimistic-concurrency suite ({@code rust/lakesoul-metadata/tests/commit_occ.rs}); the same
 * logical commit must produce the same abstract state through the JDBC path. They require a running
 * PostgreSQL configured through LAKESOUL_PG_URL/LAKESOUL_PG_USERNAME/LAKESOUL_PG_PASSWORD.
 */
public class CommitConformanceTest {

    private static final String URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL",
                            "jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified");
    private static final String PARTITION = "-5";

    private DBManager dbManager;
    private final DataCommitInfoDao dataCommitInfoDao = new DataCommitInfoDao();
    private final PartitionInfoDao partitionInfoDao = new PartitionInfoDao();
    private String tableId;
    private TableInfo tableInfo;

    @BeforeClass
    public static void configureMetadata() {
        NativeUtils.NATIVE_METADATA_QUERY_ENABLED = false;
        NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
        System.setProperty(DBUtil.urlKey, URL);
        System.setProperty(
                DBUtil.usernameKey,
                System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test"));
        System.setProperty(
                DBUtil.passwordKey,
                System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test"));
    }

    @Before
    public void setupTable() {
        dbManager = new DBManager();
        tableId = "commit_conf_" + UUID.randomUUID().toString().replace("-", "");
        String tableName = "commit_conf_" + UUID.randomUUID().toString().replace("-", "");
        dbManager.createTable(
                tableId, "default", tableName, "file:///tmp/" + tableName, "[]", "{}", ";");
        tableInfo = dbManager.getTableInfoByTableId(tableId);
    }

    @After
    public void dropTable() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            for (String table :
                    new String[] {
                        "data_commit_info",
                        "partition_info",
                        "table_path_id",
                        "table_name_id",
                        "table_info"
                    }) {
                st.executeUpdate("delete from " + table + " where table_id = '" + tableId + "'");
            }
        }
    }

    private static Uuid uuid(UUID identifier) {
        return Uuid.newBuilder()
                .setHigh(identifier.getMostSignificantBits())
                .setLow(identifier.getLeastSignificantBits())
                .build();
    }

    /** Stages one pending data commit like a writer flush would. */
    private DataCommitInfo stagePendingCommit(String partitionDesc, UUID identifier) {
        DataCommitInfo staged =
                DataCommitInfo.newBuilder()
                        .setTableId(tableId)
                        .setPartitionDesc(partitionDesc)
                        .setCommitId(uuid(identifier))
                        .setCommitOp(CommitOp.AppendCommit)
                        .setTimestamp(System.currentTimeMillis())
                        .setCommitted(false)
                        .setDomain("public")
                        .build();
        dataCommitInfoDao.insert(staged);
        return staged;
    }

    private boolean commitAppend(Uuid commitId) {
        return dbManager.commitData(
                MetaInfo.newBuilder()
                        .setTableInfo(tableInfo)
                        .addListPartition(
                                PartitionInfo.newBuilder()
                                        .setTableId(tableId)
                                        .setPartitionDesc(PARTITION)
                                        .setCommitOp(CommitOp.AppendCommit)
                                        .setDomain("public")
                                        .addSnapshot(commitId)
                                        .build())
                        .build(),
                false,
                CommitOp.AppendCommit);
    }

    private long partitionRowCount() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "select count(*) from partition_info where table_id = '"
                                        + tableId
                                        + "'")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    public void firstPartitionPublishesVersionZero() throws Exception {
        UUID identifier = UUID.randomUUID();
        stagePendingCommit(PARTITION, identifier);
        Uuid commitId = uuid(identifier);

        assertTrue(commitAppend(commitId));

        PartitionInfo head = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertEquals(0, head.getVersion());
        assertTrue(head.getSnapshotList().contains(commitId));
        DataCommitInfo published =
                dataCommitInfoDao.selectByPrimaryKey(tableId, PARTITION, identifier.toString());
        assertTrue(published.getCommitted());
    }

    @Test
    public void replayedAppendDoesNotMintANewVersion() throws Exception {
        UUID identifier = UUID.randomUUID();
        Uuid commitId = stagePendingCommit(PARTITION, identifier).getCommitId();

        assertTrue(commitAppend(commitId));

        // a replay carrying already-published commits cannot mint a second version
        assertTrue(commitAppend(commitId));
        assertEquals(1, partitionRowCount());
    }

    @Test
    public void staleCompactionIsSkippedWhenUpdateLanded() throws Exception {
        UUID appendIdentifier = UUID.randomUUID();
        Uuid appendId = stagePendingCommit(PARTITION, appendIdentifier).getCommitId();
        assertTrue(commitAppend(appendId));

        UUID updateIdentifier = UUID.randomUUID();
        stagePendingCommit(PARTITION, updateIdentifier);
        Uuid updateId = uuid(updateIdentifier);
        PartitionInfo read = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc(PARTITION)
                                                .setCommitOp(CommitOp.UpdateCommit)
                                                .setDomain("public")
                                                .addSnapshot(updateId)
                                                .build())
                                .addReadPartitionInfo(read)
                                .build(),
                        false,
                        CommitOp.UpdateCommit));

        UUID compactionIdentifier = UUID.randomUUID();
        stagePendingCommit(PARTITION, compactionIdentifier);
        Uuid compactionId = uuid(compactionIdentifier);
        PartitionInfo beforeCompaction =
                partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc(PARTITION)
                                                .setCommitOp(CommitOp.CompactionCommit)
                                                .setDomain("public")
                                                .addSnapshot(compactionId)
                                                .build())
                                .addReadPartitionInfo(read)
                                .build(),
                        false,
                        CommitOp.CompactionCommit));

        // the stale compaction was skipped: the head the update published stays
        PartitionInfo afterCompaction =
                partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertEquals(beforeCompaction.getVersion(), afterCompaction.getVersion());
        assertEquals(beforeCompaction.getSnapshotList(), afterCompaction.getSnapshotList());
        assertEquals(2, partitionRowCount());

        // and its skipped data commit must not be marked published
        DataCommitInfo skipped =
                dataCommitInfoDao.selectByPrimaryKey(
                        tableId, PARTITION, compactionIdentifier.toString());
        assertFalse(skipped.getCommitted());
    }

    @Test
    public void multiPartitionPublicationMarksEveryCommitPublished() throws Exception {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        stagePendingCommit("-5", first);
        stagePendingCommit("-6", second);

        // two partitions published in one logical commit
        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc("-5")
                                                .setCommitOp(CommitOp.AppendCommit)
                                                .setDomain("public")
                                                .addSnapshot(uuid(first))
                                                .build())
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc("-6")
                                                .setCommitOp(CommitOp.AppendCommit)
                                                .setDomain("public")
                                                .addSnapshot(uuid(second))
                                                .build())
                                .build(),
                        false,
                        CommitOp.AppendCommit));

        for (String partitionDesc : new String[] {"-5", "-6"}) {
            PartitionInfo head = partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
            assertEquals(0, head.getVersion());
        }
        assertEquals(2, partitionRowCount());
        assertTrue(
                dataCommitInfoDao
                        .selectByPrimaryKey(tableId, "-5", first.toString())
                        .getCommitted());
        assertTrue(
                dataCommitInfoDao
                        .selectByPrimaryKey(tableId, "-6", second.toString())
                        .getCommitted());
    }

    @Test
    public void multiPartitionAppendSharesOneCommitId() throws Exception {
        // Spark streaming and Flink stage every partition entry of a batch
        // with the same commit id; the publication must scope marks per
        // partition instead of matching a commit id table-wide.
        UUID sharedIdentifier = UUID.randomUUID();
        stagePendingCommit("-5", sharedIdentifier);
        stagePendingCommit("-6", sharedIdentifier);
        Uuid sharedId = uuid(sharedIdentifier);

        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc("-5")
                                                .setCommitOp(CommitOp.AppendCommit)
                                                .setDomain("public")
                                                .addSnapshot(sharedId)
                                                .build())
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc("-6")
                                                .setCommitOp(CommitOp.AppendCommit)
                                                .setDomain("public")
                                                .addSnapshot(sharedId)
                                                .build())
                                .build(),
                        false,
                        CommitOp.AppendCommit));

        for (String partitionDesc : new String[] {"-5", "-6"}) {
            PartitionInfo head = partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
            assertEquals(0, head.getVersion());
            assertTrue(head.getSnapshotList().contains(sharedId));
        }
        assertTrue(
                dataCommitInfoDao
                        .selectByPrimaryKey(tableId, "-5", sharedIdentifier.toString())
                        .getCommitted());
        assertTrue(
                dataCommitInfoDao
                        .selectByPrimaryKey(tableId, "-6", sharedIdentifier.toString())
                        .getCommitted());
    }

    @Test
    public void updateReassertionOnCurrentHeadMintsVersion() throws Exception {
        UUID appendIdentifier = UUID.randomUUID();
        Uuid appendId = stagePendingCommit(PARTITION, appendIdentifier).getCommitId();
        commitAppend(appendId);
        PartitionInfo head = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);

        // A rewrite re-asserting ids the current head already holds reads the
        // current version on purpose: it must mint a version, not be treated
        // as a replay.
        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc(PARTITION)
                                                .setCommitOp(CommitOp.UpdateCommit)
                                                .setDomain("public")
                                                .addAllSnapshot(head.getSnapshotList())
                                                .build())
                                .addReadPartitionInfo(head)
                                .build(),
                        false,
                        CommitOp.UpdateCommit));

        PartitionInfo after = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertEquals(head.getVersion() + 1, after.getVersion());
        assertEquals(CommitOp.UpdateCommit, after.getCommitOp());
        assertEquals(head.getSnapshotList(), after.getSnapshotList());
    }

    @Test
    public void staleUpdateConflictingWithSisterUpdateIsRejected() throws Exception {
        UUID appendIdentifier = UUID.randomUUID();
        Uuid appendId = stagePendingCommit(PARTITION, appendIdentifier).getCommitId();
        assertTrue(commitAppend(appendId));

        UUID firstUpdateIdentifier = UUID.randomUUID();
        stagePendingCommit(PARTITION, firstUpdateIdentifier);
        Uuid firstUpdateId = uuid(firstUpdateIdentifier);
        PartitionInfo read = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc(PARTITION)
                                                .setCommitOp(CommitOp.UpdateCommit)
                                                .setDomain("public")
                                                .addSnapshot(firstUpdateId)
                                                .build())
                                .addReadPartitionInfo(read)
                                .build(),
                        false,
                        CommitOp.UpdateCommit));

        // a second update still reading the head before the first update can
        // never overwrite it: it must be rejected with an explicit conflict
        UUID secondUpdateIdentifier = UUID.randomUUID();
        stagePendingCommit(PARTITION, secondUpdateIdentifier);
        Uuid secondUpdateId = uuid(secondUpdateIdentifier);
        org.junit.Assert.assertThrows(
                IllegalStateException.class,
                () ->
                        dbManager.commitData(
                                MetaInfo.newBuilder()
                                        .setTableInfo(tableInfo)
                                        .addListPartition(
                                                PartitionInfo.newBuilder()
                                                        .setTableId(tableId)
                                                        .setPartitionDesc(PARTITION)
                                                        .setCommitOp(CommitOp.UpdateCommit)
                                                        .setDomain("public")
                                                        .addSnapshot(secondUpdateId)
                                                        .build())
                                        .addReadPartitionInfo(read)
                                        .build(),
                                false,
                                CommitOp.UpdateCommit));

        PartitionInfo head = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
        assertEquals(read.getVersion() + 1, head.getVersion());
        assertTrue(head.getSnapshotList().contains(firstUpdateId));
        assertFalse(head.getSnapshotList().contains(secondUpdateId));
        assertFalse(
                dataCommitInfoDao
                        .selectByPrimaryKey(tableId, PARTITION, secondUpdateIdentifier.toString())
                        .getCommitted());
    }

    @Test
    public void deleteWithoutPartitionSnapshotKeepsNothingBehind() throws Exception {
        UUID deleteIdentifier = UUID.randomUUID();
        stagePendingCommit(PARTITION, deleteIdentifier);

        // the delete for a missing partition is skipped entirely
        assertTrue(
                dbManager.commitData(
                        MetaInfo.newBuilder()
                                .setTableInfo(tableInfo)
                                .addListPartition(
                                        PartitionInfo.newBuilder()
                                                .setTableId(tableId)
                                                .setPartitionDesc(PARTITION)
                                                .setCommitOp(CommitOp.DeleteCommit)
                                                .setDomain("public")
                                                .addSnapshot(uuid(deleteIdentifier))
                                                .build())
                                .build(),
                        false,
                        CommitOp.DeleteCommit));
        assertEquals(0, partitionRowCount());
    }
}
