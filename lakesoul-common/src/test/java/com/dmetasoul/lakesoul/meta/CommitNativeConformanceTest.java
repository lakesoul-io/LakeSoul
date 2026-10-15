// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.dmetasoul.lakesoul.meta.dao.DataCommitInfoDao;
import com.dmetasoul.lakesoul.meta.dao.PartitionInfoDao;
import com.dmetasoul.lakesoul.meta.entity.CommitOp;
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo;
import com.dmetasoul.lakesoul.meta.entity.MetaInfo;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.entity.Uuid;
import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

/**
 * Drives {@link DBManager#commitData} through the native coordinator.
 *
 * <p>The table is created through JDBC, the commits are coordinated by the native metadata client
 * (the Rust state machine behind the {@code commit_data} FFI entry), and the resulting abstract
 * state is verified through the JDBC DAO, so one dead lock between the two clients cannot hide
 * behind either language. Requires the native library on the classpath and a running PostgreSQL
 * configured through LAKESOUL_PG_URL/LAKESOUL_PG_USERNAME/LAKESOUL_PG_PASSWORD; skipped otherwise.
 */
public class CommitNativeConformanceTest {

    private static final String PARTITION = "-5";

    private DBManager dbManager;
    private final DataCommitInfoDao dataCommitInfoDao = new DataCommitInfoDao();
    private final PartitionInfoDao partitionInfoDao = new PartitionInfoDao();
    private String tableId;

    @BeforeClass
    public static void configureMetadata() {
        NativeUtils.NATIVE_METADATA_QUERY_ENABLED = false;
        NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
        System.setProperty(
                DBUtil.urlKey,
                System.getenv()
                        .getOrDefault(
                                "LAKESOUL_PG_URL",
                                "jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified"));
        System.setProperty(
                DBUtil.usernameKey,
                System.getenv().getOrDefault("LAKESOUL_PG_USERNAME", "lakesoul_test"));
        System.setProperty(
                DBUtil.passwordKey,
                System.getenv().getOrDefault("LAKESOUL_PG_PASSWORD", "lakesoul_test"));
    }

    @AfterClass
    public static void releaseNative() {
        NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
    }

    private static boolean nativeLibraryAvailable() {
        try {
            com.dmetasoul.lakesoul.meta.jnr.JnrLoader.get();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Before
    public void setupTable() {
        assumeTrue(nativeLibraryAvailable());
        dbManager = new DBManager();
        tableId = "commit_native_" + UUID.randomUUID().toString().replace("-", "");
        String tableName = "commit_native_" + UUID.randomUUID().toString().replace("-", "");
        dbManager.createTable(
                tableId, "default", tableName, "file:///tmp/" + tableName, "[]", "{}", ";");
    }

    private static Uuid uuid(UUID identifier) {
        return Uuid.newBuilder()
                .setHigh(identifier.getMostSignificantBits())
                .setLow(identifier.getLeastSignificantBits())
                .build();
    }

    /**
     * Stages one pending data commit with the Java DAO, before the native coordinator publishes it.
     */
    private DataCommitInfo stagePendingCommit(UUID identifier) {
        DataCommitInfo staged =
                DataCommitInfo.newBuilder()
                        .setTableId(tableId)
                        .setPartitionDesc(PARTITION)
                        .setCommitId(uuid(identifier))
                        .setCommitOp(CommitOp.AppendCommit)
                        .setTimestamp(System.currentTimeMillis())
                        .setCommitted(false)
                        .setDomain("public")
                        .build();
        dataCommitInfoDao.insert(staged);
        return staged;
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

    @AfterClass
    public static void tearDown() {
        // intentionally empty; per-test rows are unique
    }

    @Test
    public void nativeCoordinatorPublishesAndReplaysIdempotently() throws Exception {
        NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = true;
        NativeMetadataJavaClient.getInstance();

        UUID identifier = UUID.randomUUID();
        Uuid commitId = stagePendingCommit(identifier).getCommitId();

        try {
            TableInfo nativeTableInfo = dbManager.getTableInfoByTableId(tableId);
            MetaInfo metaInfo =
                    MetaInfo.newBuilder()
                            .setTableInfo(nativeTableInfo)
                            .addListPartition(
                                    PartitionInfo.newBuilder()
                                            .setTableId(tableId)
                                            .setPartitionDesc(PARTITION)
                                            .setCommitOp(CommitOp.AppendCommit)
                                            .setDomain("public")
                                            .addSnapshot(commitId)
                                            .build())
                            .build();
            assertTrue(dbManager.commitData(metaInfo, false, CommitOp.AppendCommit));

            NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
            PartitionInfo head = partitionInfoDao.selectLatestPartitionInfo(tableId, PARTITION);
            assertEquals(0, head.getVersion());
            assertTrue(head.getSnapshotList().contains(commitId));
            assertTrue(
                    dataCommitInfoDao
                            .selectByPrimaryKey(tableId, PARTITION, identifier.toString())
                            .getCommitted());

            // the replay is idempotent: the native coordinator recognizes the
            // already-published data commit and publishes nothing
            NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = true;
            assertTrue(dbManager.commitData(metaInfo, false, CommitOp.AppendCommit));
            NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
            assertEquals(1, partitionRowCount());
        } finally {
            NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
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
                    st.executeUpdate(
                            "delete from " + table + " where table_id = '" + tableId + "'");
                }
            }
        }
    }

    @Test
    public void nativeMultiPartitionAppendSharesOneCommitId() throws Exception {
        NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = true;
        NativeMetadataJavaClient.getInstance();

        // Flink stages every partition entry of one commit with the same id;
        // the native publication must scope marks per partition.
        UUID sharedIdentifier = UUID.randomUUID();
        Uuid sharedId = uuid(sharedIdentifier);
        try {
            for (String partitionDesc : new String[] {"-5", "-6"}) {
                dataCommitInfoDao.insert(
                        DataCommitInfo.newBuilder()
                                .setTableId(tableId)
                                .setPartitionDesc(partitionDesc)
                                .setCommitId(sharedId)
                                .setCommitOp(CommitOp.AppendCommit)
                                .setTimestamp(System.currentTimeMillis())
                                .setCommitted(false)
                                .setDomain("public")
                                .build());
            }
            TableInfo nativeTableInfo = dbManager.getTableInfoByTableId(tableId);
            MetaInfo.Builder metaInfo = MetaInfo.newBuilder().setTableInfo(nativeTableInfo);
            for (String partitionDesc : new String[] {"-5", "-6"}) {
                metaInfo.addListPartition(
                        PartitionInfo.newBuilder()
                                .setTableId(tableId)
                                .setPartitionDesc(partitionDesc)
                                .setCommitOp(CommitOp.AppendCommit)
                                .setDomain("public")
                                .addSnapshot(sharedId)
                                .build());
            }
            assertTrue(dbManager.commitData(metaInfo.build(), false, CommitOp.AppendCommit));

            NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
            for (String partitionDesc : new String[] {"-5", "-6"}) {
                PartitionInfo head =
                        partitionInfoDao.selectLatestPartitionInfo(tableId, partitionDesc);
                assertEquals(0, head.getVersion());
                assertTrue(head.getSnapshotList().contains(sharedId));
                assertTrue(
                        dataCommitInfoDao
                                .selectByPrimaryKey(
                                        tableId, partitionDesc, sharedIdentifier.toString())
                                .getCommitted());
            }
        } finally {
            NativeUtils.NATIVE_METADATA_UPDATE_ENABLED = false;
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
                    st.executeUpdate(
                            "delete from " + table + " where table_id = '" + tableId + "'");
                }
            }
        }
    }
}
