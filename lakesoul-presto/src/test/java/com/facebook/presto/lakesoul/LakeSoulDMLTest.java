// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;


import com.facebook.presto.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.util.List;

public class LakeSoulDMLTest extends LakeSoulSmokeTest{

    @Test
    public void testStar(){
        List<MaterializedRow> rows = sql("select * from table1 where id = 0 or id >= 2");
        int count = rows.size();
         assert count == 2;
    }

    /* without pks */
    public void testPrimaryKey1(){
        List<MaterializedRow> rows = sql("select name, id from table1 where id >= 2");
        System.out.println(rows);
    }

    /* defined by pks, and projected without pks */
    public void testPrimaryKey2(){
        List<MaterializedRow> rows = sql("select math from table4 where id > 2");
        System.out.println(rows);
        assert rows.size() == 1 && rows.get(0).getField(0).equals(95.2);
    }


    /* defined by pks, and projected with pks */
    public void testPrimaryKey3(){
        List<MaterializedRow> rows = sql("select name, id from table4 where math > 97");
        System.out.println(rows);
        assert rows.size() == 3;
    }

    /* defined by pks, and projected with part of pks */
    public void testPrimaryKey4(){
        List<MaterializedRow> rows = sql("select id, math from table4 where name = 'xiao li'");
        System.out.println(rows);
        assert rows.size() == 2;
    }

    /* all */
    public void testPartition0(){
        List<MaterializedRow> rows = sql("select * from table2 ");
        System.out.println(rows);
        assert rows.size() == 4;
    }

    /* defined by rks, and projected without rks */
    public void testPartition1(){
        List<MaterializedRow> rows = sql("select math from table2 where id > 2");
        System.out.println(rows);
        assert rows.size() == 1 && rows.get(0).getField(0).equals(95.2);
    }

    /* defined by rks, and projected with rks */
    public void testPartition2(){
        List<MaterializedRow> rows = sql("select name, id from table4 where math > 97");
        System.out.println(rows);
        assert rows.size() == 3;
    }

    /* defined by rks, and projected with part of rks */
    public void testPartition3(){
        List<MaterializedRow> rows = sql("select id, math from table2 where name = 'xiaoli'");
        System.out.println(rows);
        assert rows.size() == 2;
    }

    public void testWhere1(){
        // coming soon

        List<MaterializedRow> rows = sql("select * from test_cdc.table2");
        System.out.println(rows);
        assert rows.size() == 5;
    }


    public void testGroupBy1(){
        // coming soon
    }

    public void testJoin1(){
        // coming soon
    }


}
