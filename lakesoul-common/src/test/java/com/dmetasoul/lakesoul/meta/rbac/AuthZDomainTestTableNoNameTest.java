// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.GlobalConfig;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.entity.TableNameId;
import org.junit.After;
import org.junit.Test;

public class AuthZDomainTestTableNoNameTest extends AuthZDomainTest{

    protected final String TEST_NAME_SPACE = "authz_test_name_space_test_table_with_noname";

    protected final String TEST_TABLE_ID = "2789921C-4597-9688-AF37-A1BA7043BBC3";


    protected final String TEST_TABLE_NAME = "2789921C-4597-9688-AF37-A1BA7043BBC3";
    @Test
    public void test(){
        GlobalConfig.get().setAuthZEnabled(true);
        AuthZContext.getInstance().setDomain(TEST_DOMAIN);
        dbManager.createNewTable(
                TEST_TABLE_ID,
                TEST_NAME_SPACE,
                "",
                TEST_TABLE_PATH,
                "schema",
                new JSONObject(),
                "");
        dbManager.updateTableShortName(TEST_TABLE_PATH, TEST_TABLE_ID, TEST_TABLE_NAME, TEST_NAME_SPACE);
        TableInfo tableInfoByTableId = dbManager.getTableInfoByTableId(TEST_TABLE_ID);
        TableNameId tableNameId = dbManager.shortTableName(TEST_TABLE_NAME, TEST_NAME_SPACE);
        assert tableInfoByTableId.getTableName().equals(TEST_TABLE_NAME);
        assert tableInfoByTableId.getDomain().equals(TEST_DOMAIN);
        assert tableNameId.getDomain().equals(TEST_DOMAIN);
    }

    @After
    public void clean(){
        dbManager.deleteTableInfo(TEST_TABLE_PATH, TEST_TABLE_ID, TEST_NAME_SPACE);
    }
}
