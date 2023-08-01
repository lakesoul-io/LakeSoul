// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac.fetcher;

import org.aspectj.lang.JoinPoint;

import java.util.List;

/**
 * Fetch a object from cut point
 */
public interface AuthZFetcher<T> {
    List<String> getObject(T object);
}


