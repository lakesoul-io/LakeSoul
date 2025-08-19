// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

public class NamespaceTableName {
    private final String namespace;
    private final String tableName;

    public NamespaceTableName(String namespace, String tableName) {
        this.namespace = namespace;
        this.tableName = tableName;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getTableName() {
        return tableName;
    }
}
