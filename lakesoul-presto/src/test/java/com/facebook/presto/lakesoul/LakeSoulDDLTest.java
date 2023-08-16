// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0


package com.facebook.presto.lakesoul;

import com.facebook.presto.testing.MaterializedRow;

import java.util.List;

public class LakeSoulDDLTest extends LakeSoulSmokeTest{

    public void test(){
        List<MaterializedRow> showColumnsInTable1 = sql("show schemas");
        List<MaterializedRow> showColumnsInTable2 = sql("show tables in default");
        List<MaterializedRow> showColumnsInTable3 = sql("show columns in table1");
        System.out.println(showColumnsInTable1);
        System.out.println(showColumnsInTable2);
        System.out.println(showColumnsInTable3);
    }
}
