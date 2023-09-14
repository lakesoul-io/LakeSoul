// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import org.apache.flink.types.Row;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LakeSoulPrestoTest extends LakeSoulFlinkTestBase {


    @Test
    public void test1GenerateSchema(){
        getTableEnv().useCatalog("lakesoul");
        sql("create database if not exists pks");
        sql("create database if not exists rks");
        sql("create database if not exists test_cdc");
        List<Row> sql = sql("show databases;");
        System.out.println(sql);
    }

    @Test
    public void test2GenerateFullTypePrimaryKeyTable(){
        getTableEnv().useCatalog("lakesoul");
        sql("use pks");
        sql("create table if not exists table1 (id1 int, id2 string, \n"
                + "col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,\n"
                + "col7 boolean, col8 decimal(6, 3), col9 decimal(19, 7), col10 char(10),  col11 string,\n"
                + "col12 binary(10), col13 varbinary, col14 date, dt1 int, dt2 string,\n"
                + "PRIMARY KEY (`id1`, `id2`) NOT ENFORCED ) \n"
                + "PARTITIONED BY (`dt1`, `dt2`)\n"
                + "WITH ('format' = 'lakesoul', 'hashBucketNum' = '2', 'path' = '"
                + getTempDirUri("/lakeSource/pks/table1")
                + "')");

        // time type unsupported by flink
        // byte type unsupported by flink
        // timestamp type unsupported by spark now, to_timestamp('20230831152945', 'yyyyMMddHHmmss')
        // timestamp_ltz type unsupported by spark now,
        sql("insert into table1 values(1, '1', cast(1 as tinyint), cast(1 as smallint), 1, 1, 1.11, 1.111, true, 1.11, 1.111, 'a', 'a', ENCODE('aaa', 'utf-8') , ENCODE('aaaa', 'utf-8')  , TO_DATE('2023-08-31'), 1, '1' )");
        sql("insert into table1 values(2, '2', cast(2 as tinyint), cast(2 as smallint), 2, 2, 2.11, 2.111, true, 2.11, 2.111, 'b', 'b', ENCODE('bbb', 'utf-8') , ENCODE('bbbb', 'utf-8')  , TO_DATE('2023-09-01'), 2, '2' )");
        sql("insert into table1 values(3, '3', cast(3 as tinyint), cast(3 as smallint), 3, 3, 3.11, 3.111, false, 3.11, 3.111, 'c', 'c', ENCODE('ccc', 'utf-8') , ENCODE('cccc', 'utf-8')  , TO_DATE('2023-09-02'), 3, '3' )");
        sql("insert into table1 values(4, '4', cast(4 as tinyint), cast(4 as smallint), 4, 4, 4.11, 4.111, false, 4.11, 4.111, 'd', 'd', ENCODE('ddd', 'utf-8') , ENCODE('dddd', 'utf-8')  , TO_DATE('2023-09-03'), 4, '4' )");
        sql("insert into table1 values(5, '5', cast(5 as tinyint), cast(5 as smallint), 5, 5, 5.11, 5.111, false, 5.11, 5.111, 'e', 'e', ENCODE('eee', 'utf-8') , ENCODE('eeee', 'utf-8')  , TO_DATE('2023-09-04'),  5, '5' )");

    }

    @Test
    public void test3GenerateFullTypeRangeKeyTable(){
        getTableEnv().useCatalog("lakesoul");
        sql("use rks");
        sql("create table if not exists table1 (id1 int, id2 string, \n"
                + "col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,\n"
                + "col7 boolean, col8 decimal(6, 3), col9 decimal(19, 7), col10 char(10),  col11 string,\n"
                + "col12 binary(10), col13 varbinary, col14 date, dt1 int, dt2 string\n"
                + ") \n"
                + "PARTITIONED BY (`dt1`, `dt2`)\n"
                + "WITH ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/rks/table1")
                + "')");

        // time type unsupported by flink
        // byte type unsupported by flink
        // timestamp type unsupported by spark now, to_timestamp('20230831152945', 'yyyyMMddHHmmss')
        // timestamp_ltz type unsupported by spark now,
        sql("insert into table1 values(1, '1', cast(1 as tinyint), cast(1 as smallint), 1, 1, 1.11, 1.111, true, 1.11, 1.111, 'a', 'a', ENCODE('aaa', 'utf-8') , ENCODE('aaaa', 'utf-8')  , TO_DATE('2023-08-31'), 1, '1' )");
        sql("insert into table1 values(2, '2', cast(2 as tinyint), cast(2 as smallint), 2, 2, 2.11, 2.111, true, 2.11, 2.111, 'b', 'b', ENCODE('bbb', 'utf-8') , ENCODE('bbbb', 'utf-8')  , TO_DATE('2023-09-01'), 2, '2' )");
        sql("insert into table1 values(3, '3', cast(3 as tinyint), cast(3 as smallint), 3, 3, 3.11, 3.111, false, 3.11, 3.111, 'c', 'c', ENCODE('ccc', 'utf-8') , ENCODE('cccc', 'utf-8')  , TO_DATE('2023-09-02'), 3, '3' )");
        sql("insert into table1 values(4, '4', cast(4 as tinyint), cast(4 as smallint), 4, 4, 4.11, 4.111, false, 4.11, 4.111, 'd', 'd', ENCODE('ddd', 'utf-8') , ENCODE('dddd', 'utf-8')  , TO_DATE('2023-09-03'), 4, '4' )");
        sql("insert into table1 values(5, '5', cast(5 as tinyint), cast(5 as smallint), 5, 5, 5.11, 5.111, false, 5.11, 5.111, 'e', 'e', ENCODE('eee', 'utf-8') , ENCODE('eeee', 'utf-8')  , TO_DATE('2023-09-04'),  5, '5' )");

    }

    @Test
    public void test4GenerateCDCTable(){
        getTableEnv().useCatalog("lakesoul");
        sql("use test_cdc");
        sql("create table if not exists table1 ( id int primary key not enforced, name string, test string not null)"
                + " with ('format' = 'lakesoul', 'use_cdc'='true', 'lakesoul_cdc_change_column'='test', 'hashBucketNum'='2', 'path' = '"
                + getTempDirUri("/lakeSource/test_cdc/table1")
                + "')");
        sql("create table if not exists table2 ( id int primary key not enforced, math float, chinese float, english float, test string not null)"
                + " with ('format' = 'lakesoul', 'use_cdc'='true', 'lakesoul_cdc_change_column'='test', 'hashBucketNum'='2', 'path' = '"
                + getTempDirUri("/lakeSource/test_cdc/table2")
                + "')");
        sql("insert into table1 values(1, 'aaa')");
        sql("insert into table1 values(2, 'bbb')");
        sql("insert into table1 values(3, 'ccc')");
        sql("insert into table1 values(4, 'ddd')");
        sql("insert into table1 values(5, 'eee')");
        sql("insert into table2 values(1, 11.1, 111, 1.11)");
        sql("insert into table2 values(2, 22.2, 222, 2.22)");
        sql("insert into table2 values(3, 33.3, 333, 3.33)");
        sql("insert into table2 values(4, 44.4, 444, 4.44)");
        sql("insert into table2 values(5, 55.5, 555, 5.55)");
    }


}
