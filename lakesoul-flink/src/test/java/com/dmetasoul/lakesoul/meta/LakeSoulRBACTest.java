package com.dmetasoul.lakesoul.meta;

import org.apache.flink.lakesoul.test.LakeSoulFlinkTestBase;
import org.junit.Test;

public class LakeSoulRBACTest extends LakeSoulFlinkTestBase {
    final String ADMIN1  = "admin1";
    final String ADMIN1_PASS = "admin1";
    final String ADMIN2 = "admin2";
    final String ADMIN2_PASS = "admin2";
    final String USER1 = "user1";
    final String USER1_PASS = "user1";
    final String DOMAIN1 = "domain1";
    final String DOMAIN2 = "domain2";

    private void login(String username , String password, String domain)  {
        System.setProperty(DBUtil.usernameKey, username);
        System.setProperty(DBUtil.passwordKey, password);
        System.setProperty(DBUtil.domainKey, domain);
        DBConnector.closeAllConnections();
    }

    @Test
    public void testDifferentDomain(){
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);
        System.out.println(
                sql("show databases").size());

    }

    @Test
    public void testDifferentRole(){

    }


}
