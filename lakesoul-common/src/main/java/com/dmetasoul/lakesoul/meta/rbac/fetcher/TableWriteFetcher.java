// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac.fetcher;

import com.dmetasoul.lakesoul.meta.entity.MetaInfo;
import org.aspectj.lang.JoinPoint;

import java.util.LinkedList;
import java.util.List;

public class TableWriteFetcher implements AuthZFetcher<JoinPoint> {
    @Override
    public List<String> getObject(JoinPoint point) {
        MetaInfo metaInfo = (MetaInfo) point.getArgs()[0];
        String namespace = metaInfo.getTableInfo().getTableNamespace();
        LinkedList<String> objects = new LinkedList<>();
        objects.add(namespace);
        return objects;
    }

}


