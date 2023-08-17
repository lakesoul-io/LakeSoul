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
        List<MaterializedRow> rows = sql("select name, id from table1");
        System.out.println(rows);
    }

    /* defined by pks, and projected without pks */
    public void testPrimaryKey2(){
        List<MaterializedRow> rows = sql("select score from table2");
        System.out.println(rows);
    }


    /* defined by pks, and projected with pks */
    public void testPrimaryKey3(){
        List<MaterializedRow> rows = sql("select name, id from table2");
        System.out.println(rows);
    }

    /* defined by pks, and projected with part of pks */
    public void testPrimaryKey4(){
        List<MaterializedRow> rows = sql("select id, score from table2");
        System.out.println(rows);
    }

    public void testPartition1(){
        // coming soon
    }

    public void testWhere1(){
        // coming soon
    }



    public void testGroupBy1(){
        // coming soon
    }

    public void testJoin1(){
        // coming soon
    }


}
