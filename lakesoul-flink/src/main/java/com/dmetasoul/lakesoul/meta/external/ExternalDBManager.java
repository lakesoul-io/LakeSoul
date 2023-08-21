// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.external;

import java.io.IOException;
import java.util.List;

public interface ExternalDBManager {

    List<String> listTables();


    void importOrSyncLakeSoulTable(String tableName) throws IOException;

    void importOrSyncLakeSoulNamespace(String namespace);
}
