package com.facebook.presto.lakesoul;

import com.facebook.presto.testing.MaterializedRow;

import java.util.List;

public class LakeSoulStorageTest extends LakeSoulSmokeTest{

    public void testS3(){
        sql("use test1");
        List<MaterializedRow> showColumnsInTable1 = sql("select * from test1.table1");
        assert showColumnsInTable1.size() == 2;
    }
}
