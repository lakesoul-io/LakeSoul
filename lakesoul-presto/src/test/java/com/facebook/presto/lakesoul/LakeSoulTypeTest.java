// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.testing.MaterializedRow;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LakeSoulTypeTest extends LakeSoulSmokeTest {

    public void testTinyIntType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col1 from pks.table1 where col1 > 1");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 4;
    }

    public void testShortType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col2 from pks.table1 where col2 > 1");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 4;
    }

    public void testIntType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col3 from pks.table1 where col3 > 1");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 4;
    }

    public void testLongType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col4 from pks.table1 where col4 > 4");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 1;
    }

    public void testFloatType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col5 from pks.table1 where col5 >= 5.0");
        List<MaterializedRow> showColumnsInTable2 = sql("select col5 from pks.table1 where col5 >= 5");
        List<MaterializedRow> showColumnsInTable3 = sql("select col5 from pks.table1 where col5 = 5.11");
        System.out.println(showColumnsInTable1);
        System.out.println(showColumnsInTable2);
        System.out.println(showColumnsInTable3);
        assert showColumnsInTable1.size() == 1 && showColumnsInTable1.get(0).getField(0).equals(5.11f);
        assert showColumnsInTable2.size() == 1 && showColumnsInTable2.get(0).getField(0).equals(5.11f);
        assert showColumnsInTable3.size() == 1 && showColumnsInTable3.get(0).getField(0).equals(5.11f);
    }

    public void testDoubleType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col6 from pks.table1 where col6 >= 5.0");
        List<MaterializedRow> showColumnsInTable2 = sql("select col6 from pks.table1 where col6 >= 5");
        List<MaterializedRow> showColumnsInTable3 = sql("select col6 from pks.table1 where col6 = 5.111");
        System.out.println(showColumnsInTable1);
        System.out.println(showColumnsInTable2);
        System.out.println(showColumnsInTable3);
        assert showColumnsInTable1.size() == 1 && showColumnsInTable1.get(0).getField(0).equals(5.111);
        assert showColumnsInTable2.size() == 1 && showColumnsInTable2.get(0).getField(0).equals(5.111);
        assert showColumnsInTable3.size() == 1 && showColumnsInTable3.get(0).getField(0).equals(5.111);
    }

    public void testBooleanType(){
        //List<MaterializedRow> showColumnsInTable1 = sql("select * from table4");
        List<MaterializedRow> showColumnsInTable1 = sql("select id1 from pks.table1 where col7 = true");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 2 ;
        Set<Object> collect = showColumnsInTable1.stream().map((item) -> item.getField(0)).collect(Collectors.toSet());
        collect.contains(1);
        collect.contains(2);
    }

    public void testDecimalType(){
        // filted upon upper layer
        List<MaterializedRow> showColumnsInTable1 = sql("select col8 from pks.table1 where col8 > 2 ");
        List<MaterializedRow> showColumnsInTable2 = sql("select col9 from pks.table1 where col9 > 2.0 ");
        System.out.println(showColumnsInTable1);
        System.out.println(showColumnsInTable2);
        assert showColumnsInTable1.size() == 4;
        assert showColumnsInTable2.size() == 4;
    }

    public void testCharsType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col10 from pks.table1 where col10 = 'a'");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 1 && showColumnsInTable1.get(0).getField(0).equals("a");
    }

    public void testStringType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col11 from pks.table1  where col11 = 'a'");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 1 && showColumnsInTable1.get(0).getField(0).equals("a");
    }

    public void testByteType(){
        // coming soon
    }

    public void testBinary(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col12 from pks.table1  where col11 = 'b'");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 1 && bytesEq((byte[]) showColumnsInTable1.get(0).getField(0), "bbb".getBytes());
    }

    public void testVarBinary(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col13 from pks.table1  where col11 = 'b'");
        System.out.println(showColumnsInTable1);
        assert showColumnsInTable1.size() == 1 && bytesEq((byte[]) showColumnsInTable1.get(0).getField(0), "bbbb".getBytes());
    }

    public void testDateType(){
        List<MaterializedRow> showColumnsInTable1 = sql("select col14 from pks.table1 where col14 = date('2023-09-03') ");
        assert showColumnsInTable1.size() == 1;
        System.out.println(showColumnsInTable1);
        LocalDate localDate = (LocalDate) showColumnsInTable1.get(0).getField(0);
        assert localDate.equals(LocalDate.of(2023, 9, 3));
    }

    public void testTimeStampWithTimeZone(){
        // coming soon
    }


    private boolean bytesEq(byte[] a, byte[] b){
        if(a.length != b.length) return false;
        for(int i = 0; i < a.length; i++){
            if(a[i] != b[i]) return false;
        }
        return true;
    }
}
