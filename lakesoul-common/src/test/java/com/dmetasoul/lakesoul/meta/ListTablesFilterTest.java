// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dmetasoul.lakesoul.meta.dao.TableNameIdDao;
import com.dmetasoul.lakesoul.meta.dao.TablePathIdDao;
import com.dmetasoul.lakesoul.meta.jnr.NativeUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * PostgreSQL-backed checks that the table listings hide internal IVM tables
 * (`lakesoul.ivm.internal=true`).
 *
 * <p>The metadata schema and a running PostgreSQL are required, configured through
 * LAKESOUL_PG_URL/LAKESOUL_PG_USERNAME/LAKESOUL_PG_PASSWORD like the other tests. The JDBC listings
 * are exercised here; the native listings are covered by the Rust tests.
 */
public class ListTablesFilterTest {

    private static final String URL =
            System.getenv()
                    .getOrDefault(
                            "LAKESOUL_PG_URL", "jdbc:postgresql://127.0.0.1:5432/lakesoul_test");

    private static final String NAMESPACE = "default";
    private static final String DOMAIN = "public";

    private final String suffix = UUID.randomUUID().toString().replace("-", "");
    private final String normalId = "list_filter_normal_" + suffix;
    private final String internalId = "list_filter_internal_" + suffix;
    private final String normalName = normalId;
    private final String internalName = internalId;
    private final String normalPath = "file:///tmp/" + normalId;
    private final String internalPath = "file:///tmp/" + internalId;

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
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            insertTable(st, normalId, normalName, normalPath, "{}");
            insertTable(
                    st,
                    internalId,
                    internalName,
                    internalPath,
                    "{\"lakesoul.ivm.internal\":\"true\"}");
        }
    }

    private static void insertTable(
            Statement st, String tableId, String name, String path, String properties)
            throws Exception {
        st.executeUpdate(
                String.format(
                        "insert into table_info(table_id, table_namespace, table_name,"
                                + " table_path, table_schema, properties, partitions, domain)"
                                + " values ('%s','%s','%s','%s','[]','%s',';','%s')",
                        tableId, NAMESPACE, name, path, properties, DOMAIN));
        st.executeUpdate(
                String.format(
                        "insert into table_name_id(table_name, table_id, table_namespace, domain)"
                                + " values ('%s','%s','%s','%s')",
                        name, tableId, NAMESPACE, DOMAIN));
        st.executeUpdate(
                String.format(
                        "insert into table_path_id(table_path, table_id, table_namespace, domain)"
                                + " values ('%s','%s','%s','%s')",
                        path, tableId, NAMESPACE, DOMAIN));
    }

    @After
    public void dropMeta() throws Exception {
        try (Connection conn = DBConnector.getConn();
                Statement st = conn.createStatement()) {
            for (String tableId : new String[] {normalId, internalId}) {
                st.executeUpdate("delete from table_name_id where table_id = '" + tableId + "'");
                st.executeUpdate("delete from table_path_id where table_id = '" + tableId + "'");
                st.executeUpdate("delete from table_info where table_id = '" + tableId + "'");
            }
        }
    }

    @Test
    public void namesByNamespaceHideInternalTables() {
        List<String> names = new TableNameIdDao().listAllNameByNamespace(NAMESPACE);
        assertTrue(names.contains(normalName));
        assertFalse(names.contains(internalName));
    }

    @Test
    public void pathsByNamespaceHideInternalTables() {
        List<String> paths = new TablePathIdDao().listAllPathByNamespace(NAMESPACE);
        assertTrue(paths.contains(normalPath));
        assertFalse(paths.contains(internalPath));
    }

    @Test
    public void allPathsHideInternalTables() {
        List<String> paths = new TablePathIdDao().listAllPath();
        assertTrue(paths.contains(normalPath));
        assertFalse(paths.contains(internalPath));
    }

    @Test
    public void namesByDomainHideInternalTables() {
        List<String> names =
                new TableNameIdDao()
                        .listAllNamesByDomain(DOMAIN).stream()
                                .map(NamespaceTableName::getTableName)
                                .collect(Collectors.toList());
        assertTrue(names.contains(normalName));
        assertFalse(names.contains(internalName));
    }
}
