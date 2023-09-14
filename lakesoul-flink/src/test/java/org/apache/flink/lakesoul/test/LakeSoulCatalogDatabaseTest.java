// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.entity.Namespace;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LakeSoulCatalogDatabaseTest extends LakeSoulCatalogTestBase {

    public LakeSoulCatalogDatabaseTest(String catalogName, Namespace baseNamepace) {
        super(catalogName, baseNamepace);
    }

    @After
    @Override
    public void clean() {
        sql("DROP TABLE IF EXISTS %s.%s", DATABASE, flinkTable);
        sql("DROP DATABASE IF EXISTS %s", DATABASE);
        super.clean();
    }


    @Test
    public void testDefaultDatabase() {
        sql("USE CATALOG %s", catalogName);
        sql("SHOW TABLES");

        Assert.assertEquals(
                "Should use the current catalog", getTableEnv().getCurrentCatalog(), catalogName);
        Assert.assertEquals(
                "Should use the configured default namespace",
                "default",
                getTableEnv().getCurrentDatabase()
                           );
    }

    @Test
    public void testDropEmptyDatabase() {
        Assert.assertFalse(
                "Namespace should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s", DATABASE);

        Assert.assertTrue(
                "Namespace should exist", catalog.databaseExists(DATABASE));

        sql("DROP DATABASE %s", DATABASE);

        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));
    }

    @Test
    public void testCreateNamespace() {
        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s", DATABASE);

        Assert.assertTrue(
                "Database should exist", validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE IF NOT EXISTS %s", DATABASE);
        Assert.assertTrue(
                "Database should still exist",
                validationCatalog.databaseExists(DATABASE));

        sql("DROP DATABASE IF EXISTS %s", DATABASE);
        Assert.assertFalse(
                "Database should be dropped", validationCatalog.databaseExists(DATABASE));
    }


    @Test
    public void testDropNonEmptyNamespace() {
//        Assume.assumeFalse(
//                "Hadoop catalog throws IOException: Directory is not empty.", isHadoopCatalog);

        Assert.assertFalse(
                "Namespace should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s", DATABASE);


//        sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, flinkTable);

        sql("CREATE TABLE %s.%s ( id bigint, name string, dt string, primary key (id) NOT ENFORCED ) PARTITIONED BY (dt)" +
            " " +
            "with ('connector' = 'lakeSoul', 'path'='%s', 'hashBucketNum'='2')", DATABASE, flinkTable, flinkTablePath);

        Assert.assertTrue(
                "databases should exist", validationCatalog.databaseExists(DATABASE));
        Assert.assertTrue(
                "Table should exist",
                validationCatalog.tableExists(new ObjectPath(DATABASE, flinkTable)));


        sql("DROP TABLE %s.%s", flinkDatabase, flinkTable);
        Assert.assertFalse(
                "Table should not exist",
                validationCatalog.tableExists(new ObjectPath(DATABASE, flinkTable)));
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testListTables() {
        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s", DATABASE);
        sql("USE CATALOG %s", catalogName);
        Assert.assertEquals("Database default should be in use", "default", validationCatalog.getDefaultDatabase());
        sql("USE %s", DATABASE);
        Assert.assertEquals("Database " + DATABASE + " should be in use", DATABASE, getTableEnv().getCurrentDatabase());


        Assert.assertTrue(
                "Database should exist", validationCatalog.databaseExists(DATABASE));

        Assert.assertEquals("Should not list any tables", 0, sql("SHOW TABLES").size());

        sql("CREATE TABLE %s ( id bigint, name string, dt string, primary key (id) NOT ENFORCED ) PARTITIONED BY (dt)" +
            " " +
            "with ('connector' = 'lakeSoul', 'path'='%s', 'hashBucketNum'='2')", flinkTable, flinkTablePath);

        List<Row> tables = sql("SHOW TABLES");
        Assert.assertEquals("Only 1 table", 1, tables.size());
        Assert.assertEquals("Table name should match", flinkTable, tables.get(0).getField(0));

        Assert.assertThrows(ValidationException.class, () -> sql("DROP DATABASE %s", DATABASE));
        Assert.assertTrue(
                "Namespace should not be dropped",
                validationCatalog.databaseExists(DATABASE));

        sql("DROP DATABASE %s CASCADE", DATABASE);
        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));
    }

    @Test
    public void testListNamespace() {
        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s", flinkDatabase);
        sql("USE CATALOG %s", catalogName);

        Assert.assertTrue(
                "Database should exist", validationCatalog.databaseExists(DATABASE));

        List<Row> databases = sql("SHOW DATABASES");


        Assert.assertTrue(
                "Should have db database",
                databases.stream().anyMatch(d -> Objects.equals(d.getField(0), DATABASE)));

        sql("DROP DATABASE %s", DATABASE);

        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));

    }

    @Test
    public void testCreateNamespaceWithMetadata() {

        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase);

        Assert.assertTrue(
                "Namespace should exist", validationCatalog.databaseExists(DATABASE));


        try {
            Map<String, String> metaData = validationCatalog.getDatabase(DATABASE).getProperties();
            Assert.assertEquals(
                    "Namespace should have expected prop value", "value", metaData.get("prop"));
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }

        sql("DROP DATABASE %s", DATABASE);

        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));


    }

    @Test
    public void testCreateNamespaceWithComment() {

        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s COMMENT 'namespace doc'", flinkDatabase);

        Assert.assertTrue(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        try {
            String comment = validationCatalog.getDatabase(DATABASE).getComment();
            Assert.assertEquals(
                    "Namespace should have expected comment", "namespace doc", comment);
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }

        sql("DROP DATABASE %s", DATABASE);

        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));

    }

    @Test
    public void testCreateNamespaceWithLocation() throws Exception {

        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        File location = TEMPORARY_FOLDER.newFile();
        Assert.assertTrue(location.delete());

        sql("CREATE DATABASE %s WITH ('location'='%s')", flinkDatabase, location);

        Assert.assertTrue(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        try {
            Map<String, String> nsMetadata =
                    validationCatalog.getDatabase(DATABASE).getProperties();

            Assert.assertEquals(
                    "Namespace should have expected location",
                    location.getPath(),
                    nsMetadata.get("location"));
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }

        sql("DROP DATABASE %s", DATABASE);

        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));

    }

    @Test
    public void testSetProperties() {

        Assert.assertFalse(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));

        sql("CREATE DATABASE %s", flinkDatabase);

        Assert.assertTrue(
                "Database should not already exist",
                validationCatalog.databaseExists(DATABASE));


        try {
            Map<String, String> defaultMetadata = validationCatalog.getDatabase(DATABASE).getProperties();
            Assert.assertFalse(
                    "Default metadata should not have custom property", defaultMetadata.containsKey("prop"));
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }


        sql("ALTER DATABASE %s SET ('prop'='value')", flinkDatabase);

        try {
            Map<String, String> nsMetadata = validationCatalog.getDatabase(DATABASE).getProperties();
            Assert.assertEquals(
                    "Namespace should have expected prop value", "value", nsMetadata.get("prop"));
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }

        sql("DROP DATABASE %s", DATABASE);

        Assert.assertFalse(
                "Namespace should have been dropped",
                validationCatalog.databaseExists(DATABASE));

    }

}
