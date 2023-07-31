// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.dmetasoul.lakesoul.meta.GlobalConfig;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import org.junit.After;
import org.junit.Test;

//public class AuthZDomainTestNamespaceTest extends AuthZDomainTest {
//
//
//    protected final String TEST_NAME_SPACE = "authz_test_name_space_with_domain";
//
//    @Test
//    public void test(){
//        GlobalConfig.get().setAuthZEnabled(true);
//        AuthZContext.getInstance().setDomain(TEST_DOMAIN);
//        dbManager.createNewNamespace(TEST_NAME_SPACE, "{}", "");
//        Namespace namespaceByNamespace = dbManager.getNamespaceByNamespace(TEST_NAME_SPACE);
//        assert namespaceByNamespace.getNamespace().equals(TEST_NAME_SPACE) && namespaceByNamespace.getDomain().equals(TEST_DOMAIN);
//    }
//
//    @Test
//    public void test2(){
//        GlobalConfig.get().setAuthZEnabled(true);
//        AuthZContext.getInstance().setDomain(TEST_DOMAIN);
//        dbManager.createNewNamespace(TEST_NAME_SPACE, "{}", "");
//        Namespace namespaceByNamespace = dbManager.getNamespaceByNamespace(TEST_NAME_SPACE);
//        assert namespaceByNamespace.getNamespace().equals(TEST_NAME_SPACE) && namespaceByNamespace.getDomain().equals(TEST_DOMAIN);
//    }
//
//    @After
//    public void clean(){
//        dbManager.deleteNamespace(TEST_NAME_SPACE);
//    }
//}
