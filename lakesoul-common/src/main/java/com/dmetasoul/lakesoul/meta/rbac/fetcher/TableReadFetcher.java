// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac.fetcher;

import com.dmetasoul.lakesoul.meta.entity.TableInfo;

import java.util.LinkedList;
import java.util.List;

public class TableReadFetcher implements AuthZFetcher<TableInfo>{
    @Override
    public List<String> getObject(TableInfo object) {
        List<String> list = new LinkedList<>();
        list.add(object.getTableNamespace());
        return list;
    }
}


