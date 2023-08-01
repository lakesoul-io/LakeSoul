// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZFetcher;
import com.dmetasoul.lakesoul.meta.rbac.fetcher.AuthZIdentFetcher;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AuthZAfter {

    Class<? extends AuthZFetcher> fetcher() default AuthZIdentFetcher.class;
}





