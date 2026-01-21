/*
 * SPDX-FileCopyrightText: 2026 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.flink.lakesoul.entry;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.mysql.database.catalog.MySqlCatalog;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

/**
 * @author mag1cian
 */
public class JdbcIngest {
    public static void main(String[] args) throws ExecutionException, InterruptedException, TableNotExistException {
        ParameterTool params = ParameterTool.fromArgs(args);
        String tableName = params.getRequired("tableName");
        String defaultDatabase = params.getRequired("defaultDatabase");
        String username = params.getRequired("username");
        String password = params.getRequired("password");
        String baseUrl = params.getRequired("baseUrl");
        String lakesoulDB = params.getRequired("lakesoulDB");
        String partitionColumn = params.getRequired("partitionColumn");
        int parallelism = Integer.parseInt(params.getRequired("parallelism"));
        String userStart = params.get("userStart");
        String userEnd = params.get("userEnd");


        EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
        builder.inBatchMode();
        EnvironmentSettings settings = builder.build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig().getConfiguration().setString("pipeline.name", defaultDatabase + "-lakesoul-" + tableName);
        tEnv.getConfig().getConfiguration()
                .setString("table.exec.resource.default-parallelism", "1");
        tEnv.getConfig().getConfiguration()
                .setString("parallelism.default", "1");

        String catalogName = "my_mysql_catalog";

        Catalog mysqlCatalog = new MySqlCatalog(
                Thread.currentThread().getContextClassLoader(),
                catalogName,
                defaultDatabase,
                username,
                password,
                baseUrl
        );
        tEnv.registerCatalog(catalogName, mysqlCatalog);
        // single table
        CatalogBaseTable table = mysqlCatalog.getTable(ObjectPath.fromString(defaultDatabase + "." + tableName));
        Schema sourceSchema = table.getUnresolvedSchema();
        System.out.println("Source schema: " + sourceSchema);

        String jdbcTableName = "default_catalog.default_database." + tableName;
        tEnv.createTable(jdbcTableName,
                TableDescriptor.forConnector("jdbc")
                        .schema(sourceSchema)
                        .option("url", baseUrl + "/" + defaultDatabase)
                        .option("table-name", tableName)
                        .option("username", username)
                        .option("password", password)
                        .option("scan.fetch-size", "-2147483648")
                        .build());

        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnv.registerCatalog("lakesoul", lakesoulCatalog);


        // sink table
        String formatedCol = "pt_" + partitionColumn + "_dt";

        // query parCol min,max

        LocalDateTime min = null;
        LocalDateTime max = null;
        String query = String.format("SELECT MIN(%s), MAX(%s) FROM %s", partitionColumn, partitionColumn, tableName);
        try (Connection conn = DriverManager.getConnection(baseUrl + "/" + defaultDatabase, username, password); PreparedStatement ps = conn.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                min = rs.getObject(1, LocalDateTime.class);
                max = rs.getObject(2, LocalDateTime.class);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        String lakesoulTable = "s_" + defaultDatabase + "_" + tableName;
        String lakesoulDBTable = lakesoulDB + "." + lakesoulTable;
        String lakesoulCatalogDBTable = "lakesoul." + lakesoulDBTable;

        if (!lakesoulCatalog.tableExists(ObjectPath.fromString(lakesoulDBTable))) {
            // create sink table
            Schema sinkSchema = Schema.newBuilder()
                    .fromSchema(sourceSchema)
                    .column(formatedCol, DataTypes.STRING())
                    .build();
            System.out.println("Sink schema: " + sinkSchema);

            tEnv.createTable(lakesoulCatalogDBTable,
                    TableDescriptor.forConnector("lakesoul")
                            .schema(sinkSchema)
                            .partitionedBy(formatedCol)
                            .option("hashBucketNum", "8")
                            .option("use_cdc", "true")
                            .build());
        }

        LocalDate dbStart = min.toLocalDate();
        LocalDate dbEnd = max.toLocalDate();
        LocalDate start = dbStart;
        LocalDate end = dbEnd;

        if (!userStart.isEmpty() && !userEnd.isEmpty()) {
            LocalDate userStartDate = LocalDate.parse(userStart);
            LocalDate userEndDate = LocalDate.parse(userEnd);
            start = dbStart.isAfter(userStartDate) ? dbStart : userStartDate;
            end = dbEnd.isBefore(userEndDate) ? dbEnd : userEndDate;
        }

        System.out.println("start: " + start + " end: " + end);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        int cnt = 0;
        StatementSet stmtSet = tEnv.createStatementSet();
        for (LocalDate date = start;
             !date.isAfter(end);
             date = date.plusDays(1)) {
            String dayStart = date.atStartOfDay().format(formatter);
            String dayEnd = date.plusDays(1).atStartOfDay().format(formatter);
            System.out.println("Submitting date: " + formatedCol + "=" + date);
            // 生成 SQL
            String sql = String.format(
                    "INSERT INTO lakesoul.ods_qimai.s_qimai_%s " +
                            "SELECT *, date_format(%s,'yyyy-MM') as %s " +
                            "FROM %s " +
                            "WHERE %s >= '%s' AND %s < '%s'",
                    tableName, partitionColumn, formatedCol,
                    jdbcTableName,
                    partitionColumn, dayStart, partitionColumn, dayEnd
            );

            System.out.println("add sql: " + sql);

            stmtSet.addInsertSql(sql);
            cnt += 1;
            if (cnt >= parallelism) {
                System.out.println("Submitting a batch job for " + cnt + " days...");
                stmtSet.execute().await();
                stmtSet = tEnv.createStatementSet();
                cnt = 0;
            }

        }
        if (cnt > 0) {
            stmtSet.execute().await();
        }

        System.out.println("insert into " + lakesoulCatalogDBTable + " from " + start + " to " + end + " finish");

    }
}