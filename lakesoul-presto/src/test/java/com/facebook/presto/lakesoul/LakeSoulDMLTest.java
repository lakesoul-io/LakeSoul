// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;


import com.facebook.presto.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.util.List;

public class LakeSoulDMLTest extends LakeSoulSmokeTest{

    @Test
    public void test(){
        List<MaterializedRow> showColumnsInTable1 = sql("show columns in table1");
        assert sql("select * from table1").size() == 0;
    }


}
