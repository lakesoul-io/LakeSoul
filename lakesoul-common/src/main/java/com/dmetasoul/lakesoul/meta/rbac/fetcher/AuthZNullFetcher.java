// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac.fetcher;

import org.aspectj.lang.JoinPoint;

import java.util.LinkedList;
import java.util.List;

public class AuthZNullFetcher implements AuthZFetcher<JoinPoint> {
    @Override
    public List<String> getObject(JoinPoint point) {
        return new LinkedList<>();
    }
}





