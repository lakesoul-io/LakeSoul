// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.metadata;

public interface JdbcMetaDataSource {
    DatabaseSchemaedTables getDatabaseAndTablesWithSchema();
}
