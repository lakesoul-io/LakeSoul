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
        List<MaterializedRow> rows1 = sql("select * from pks.table1");
        List<MaterializedRow> rows2 = sql("select * from rks.table1");
        assert rows1.size() == 5;
        assert rows2.size() == 5;
    }

    /* defined by pks, and projected without pks */
    public void testPrimaryKey2(){
        List<MaterializedRow> rows = sql("select col1, col2 from pks.table1 where col3 > 2");
        System.out.println(rows);
        assert rows.size() == 3;
    }


    /* defined by pks, and projected with pks */
    public void testPrimaryKey3(){
        List<MaterializedRow> rows = sql("select id1, id2 from pks.table1 where col3 > 2");
        System.out.println(rows);
        assert rows.size() == 3;
    }

    /* defined by pks, and projected with part of pks */
    public void testPrimaryKey4(){
        List<MaterializedRow> rows = sql("select id1, col2 from pks.table1 where col3 > 2");
        System.out.println(rows);
        assert rows.size() == 3;
    }


    /* defined by rks, and projected without rks */
    public void testPartition1(){
        List<MaterializedRow> rows = sql("select col1, col2 from rks.table1 where col3 > 2");
        System.out.println(rows);
        assert rows.size() == 3;
    }

    /* defined by rks, and projected with rks */
    public void testPartition2(){
        List<MaterializedRow> rows = sql("select id1, id2 from rks.table1 where col3 > 2");
        System.out.println(rows);
        assert rows.size() == 3;
    }

    /* defined by rks, and projected with part of rks */
    public void testPartition3(){
        List<MaterializedRow> rows = sql("select id1, col2 from rks.table1 where col3 > 2");
        System.out.println(rows);
        assert rows.size() == 3;
    }


}
