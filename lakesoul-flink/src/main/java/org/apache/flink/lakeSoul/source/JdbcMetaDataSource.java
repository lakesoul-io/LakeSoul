package org.apache.flink.lakeSoul.source;

import java.sql.Connection;
import java.util.List;

public  interface JdbcMetaDataSource {
    public  DatabaseSchemaedTables getDatabaseAndTablesWithSchema();
}
