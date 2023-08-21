// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac.fetcher;

import java.util.LinkedList;
import java.util.List;

public class AuthZIdentFetcher implements AuthZFetcher<Object> {

    @Override
    public List<String> getObject(Object object) {
        List<String> objects = new LinkedList<>();
        objects.add((String) object);
        return objects;
    }
}
