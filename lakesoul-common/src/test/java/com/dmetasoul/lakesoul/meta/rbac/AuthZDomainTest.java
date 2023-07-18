// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.GlobalConfig;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

public class AuthZDomainTest {

    DBManager dbManager;

    protected  final String TEST_DOMAIN = "test_domain";

    protected final String TEST_TABLE_PATH = "/root/test_path";

    public AuthZDomainTest(){
        dbManager = new DBManager();
    }
}





