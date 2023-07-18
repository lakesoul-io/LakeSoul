// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.GlobalConfig;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.junit.After;
import org.junit.Test;

public class AuthZDomainTestTablePublicTest extends AuthZDomainTest{

    protected final String TEST_NAME_SPACE = "authz_test_name_space_test_table_with_public";


    protected final String TEST_TABLE_ID = "2C4F7D0D-C507-C882-F05D-9459C00DC638";


    protected final String TEST_TABLE_NAME = "2C4F7D0D-C507-C882-F05D-9459C00DC638";

    @Test
    public void test(){
        GlobalConfig.get().setAuthZEnabled(false);
        dbManager.createNewTable(
                TEST_TABLE_ID,
                TEST_NAME_SPACE,
                TEST_TABLE_NAME,
                TEST_TABLE_PATH,
                "schema",
                new JSONObject(),
                "");
        TableInfo tableInfoByTableId = dbManager.getTableInfoByTableId(TEST_TABLE_ID);
        assert tableInfoByTableId.getDomain().equals("public");

    }

    @After
    public void clean(){
        dbManager.deleteTableInfo(TEST_TABLE_PATH, TEST_TABLE_ID, TEST_NAME_SPACE);
    }
}
