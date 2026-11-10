// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.external;

public interface ExternalDBManager {
    void importOrSyncLakeSoulNamespace(String namespace);
}
