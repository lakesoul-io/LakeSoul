// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.external;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.JsonUtils;

public class NameSpaceManager implements ExternalDBManager {
    private final DBManager lakesoulDBManager = new DBManager();

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null) {
            return;
        }
        lakesoulDBManager.createNewNamespace(
                namespace, JsonUtils.createObjectNode().toString(), "");
    }
}
