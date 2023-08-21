// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZFetcher;
import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZNullFetcher;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthZBefore {
    int index() default 0;
    String name() default "";

    Class<? extends AuthZFetcher> fetcher() default AuthZNullFetcher.class;
}




