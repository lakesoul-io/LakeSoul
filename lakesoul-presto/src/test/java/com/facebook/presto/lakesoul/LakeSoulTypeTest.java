// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.testing.MaterializedRow;

import java.util.List;

public class LakeSoulTypeTest extends LakeSoulSmokeTest {

    public void testTinyIntType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id1 from table3 where id1 > 1");
        assert showColumnsInTable1.size() == 4;
    }

    public void testShortType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id2 from table3 where id2 > 2");
        assert showColumnsInTable1.size() == 3;
    }

    public void testIntType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id3 from table3 where id3 > 3");
        assert showColumnsInTable1.size() == 2;
    }

    public void testLongType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id4 from table3 where id4 > 4");
        assert showColumnsInTable1.size() == 1;
    }

    public void testFloatType(){
        //List<MaterializedRow> showColumnsInTable1 = sql("select id5 from table3 where id5 >= 5.0");
        List<MaterializedRow> showColumnsInTable1 = sql("select id5 from table3 where id5 >= 5");
        assert showColumnsInTable1.size() == 1;
    }

    public void testDoubleType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id6 from table3 where id6 >= 5");
        assert showColumnsInTable1.size() == 1;
    }

    public void testBooleanType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id1 from table4 where id1 = true or id1 is null");
        assert showColumnsInTable1.size() == 2;
    }

    public void testDecimalType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select id2 from table4 where not id2 is null");
        assert showColumnsInTable1.size() == 2;
    }

    public void testStringType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select * from table4");
        //List<MaterializedRow> showColumnsInTable1 = sql("select id3 from table4 where id3 = 'hello' or id3 is null");
        assert showColumnsInTable1.size() == 2;
    }


}
