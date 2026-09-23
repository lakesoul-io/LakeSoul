// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * PostgreSQL-backed checks that the clean-up statements honour the pin flags.
 *
 * <p>The metadata schema and a running PostgreSQL are required, configured through
 * LAKESOUL_PG_URL/LAKESOUL_PG_USERNAME/LAKESOUL_PG_PASSWORD like the other tests.
 */
public class PinnedCleanupTest {

    private static final String URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL", "jdbc:postgresql://127.0.0.1:5432/lakesoul_test");
    private static final String PARTITION = "-5";

    private final String tableId =
            "pinned_cleanup_" + UUID.randomUUID().toString().replace("-", "");
    private final String pinnedCommit = UUID.randomUUID().toString();
    private final String plainCommit = UUID.randomUUID().toString();
    private final String pinnedFile = "/tmp/" + tableId + "/pinned.parquet";
    private final String plainFile = "/tmp/" + tableId + "/plain.parquet";

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
    public void insertMeta() throws Exception {
        long now = System.currentTimeMillis();
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    String.format(
                            "insert into table_info(table_id, table_namespace, table_name,"
                                + " table_path, table_schema, properties, partitions, domain)"
                                + " values"
                                + " ('%s','default','%s','file:///tmp/%s','[]','{}',';','public')",
                            tableId, tableId, tableId));
            st.executeUpdate(
                    String.format(
                            "insert into partition_info(table_id, partition_desc, version,"
                                + " commit_op, snapshot, timestamp, domain, pinned) values"
                                + " ('%s','%s',0,'AppendCommit',ARRAY['%s'::uuid],%d,'public',true)",
                            tableId, PARTITION, pinnedCommit, now));
            st.executeUpdate(
                    String.format(
                            "insert into partition_info(table_id, partition_desc, version,"
                                + " commit_op, snapshot, timestamp, domain, pinned) values"
                                + " ('%s','%s',1,'AppendCommit',ARRAY['%s'::uuid,'%s'::uuid],%d,'public',false)",
                            tableId, PARTITION, pinnedCommit, plainCommit, now + 1));
            st.executeUpdate(
                    String.format(
                            "insert into data_commit_info(table_id, partition_desc, commit_id,"
                                    + " file_ops, commit_op, committed, timestamp, domain, pinned)"
                                    + " values ('%s','%s','%s'::uuid,"
                                    + " ARRAY[ROW('%s','add',10,'id')::data_file_op],"
                                    + " 'AppendCommit',true,%d,'public',true)",
                            tableId, PARTITION, pinnedCommit, pinnedFile, now));
            st.executeUpdate(
                    String.format(
                            "insert into data_commit_info(table_id, partition_desc, commit_id,"
                                    + " file_ops, commit_op, committed, timestamp, domain, pinned)"
                                    + " values ('%s','%s','%s'::uuid,"
                                    + " ARRAY[ROW('%s','add',10,'id')::data_file_op],"
                                    + " 'AppendCommit',true,%d,'public',false)",
                            tableId, PARTITION, plainCommit, plainFile, now + 1));
        }
    }

    @After
    public void dropMeta() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            st.executeUpdate("delete from data_commit_info where table_id = '" + tableId + "'");
            st.executeUpdate("delete from partition_info where table_id = '" + tableId + "'");
            st.executeUpdate("delete from table_info where table_id = '" + tableId + "'");
        }
    }

    @Test
    public void pinnedFilePathsAreReported() {
        DBManager dbManager = new DBManager();
        List<String> pinned = dbManager.getPinnedFilePaths(Arrays.asList(pinnedFile, plainFile));
        assertTrue(pinned.contains(pinnedFile));
        assertFalse(pinned.contains(plainFile));
    }

    @Test
    public void pinnedPartitionIsNotDropped() throws Exception {
        DBManager dbManager = new DBManager();
        assertEquals(0, dbManager.deleteMetaPartitionInfo(tableId, PARTITION).size());
        assertEquals(2, count("partition_info"));
        assertEquals(2, count("data_commit_info"));
    }

    @Test
    public void unpinnedPartitionIsDropped() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            st.executeUpdate(
                    "update partition_info set pinned = false where table_id = '" + tableId + "'");
            st.executeUpdate(
                    "update data_commit_info set pinned = false where table_id = '"
                            + tableId
                            + "'");
        }
        DBManager dbManager = new DBManager();
        List<String> files = dbManager.deleteMetaPartitionInfo(tableId, PARTITION);
        assertTrue(files.contains(pinnedFile));
        assertTrue(files.contains(plainFile));
        assertEquals(0, count("partition_info"));
        assertEquals(0, count("data_commit_info"));
    }

    private long count(String table) throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement();
                ResultSet rs =
                        st.executeQuery(
                                "select count(*) from "
                                        + table
                                        + " where table_id = '"
                                        + tableId
                                        + "'")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
