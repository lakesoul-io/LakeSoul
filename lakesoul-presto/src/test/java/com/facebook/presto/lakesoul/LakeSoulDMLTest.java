// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;


import com.dmetasoul.lakesoul.meta.DBUtil;
import com.facebook.presto.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.util.List;

public class LakeSoulDMLTest extends LakeSoulSmokeTest{

    @Test
    public void test(){
        List<MaterializedRow> showColumnsInTable1 = sql("show schemas");
        List<MaterializedRow> showColumnsInTable2 = sql("show tables");
        List<MaterializedRow> showColumnsInTable3 = sql("show columns in table1");
        int count = sql("select * from table1").size();
        assert count == 2;
    }


}
