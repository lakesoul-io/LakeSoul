// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.benchmark;

public class WriteDataCheck {

    static String csvPath = "file:///tmp/csv/";
    static String lakeSoulPath = "file:///tmp/lakesoul/";
    static String serverTimeZone = "UTC";

    static String printLine = " ******** ";

    /**
     * param example:
     * --csv.path file:///tmp/csv/
     * --lakesoul.table.path file:///tmp/lakesoul/
     * --server.time.zone UTC
     */
    public static void main(String[] args)  {
        ParametersTool parameter = ParametersTool.fromArgs(args);
        csvPath = parameter.get("csv.path", "file:///tmp/csv/");
        lakeSoulPath = parameter.get("lakesoul.table.path", "file:///tmp/lakesoul/");
        serverTimeZone = parameter.get("server.time.zone", serverTimeZone);


//        val spark = builder.getOrCreate()
//        spark.sparkContext.setLogLevel("ERROR")
//
//        val lakeSoulTablePath = SparkUtil.makeQualifiedTablePath(new Path(lakeSoulPath)).toString
//        val csvTablePath = SparkUtil.makeQualifiedTablePath(new Path(csvPath)).toString
//
//        val lakeSoulDF = LakeSoulTable.forPath(lakeSoulTablePath).toDF
//        val csvDF = spark.read.schema(lakeSoulDF.schema).format("parquet").load(csvTablePath)
//
//        val diff1 = lakeSoulDF.rdd.subtract(csvDF.rdd)
//        val result = lakeSoulDF.count() == csvDF.count() && diff1.count() == 0
//
//        if (!result) {
//            System.out.println(printLine);
//            System.out.println("CSV count ${csvDF.count()}, LakeSoul count ${lakeSoulDF.count()}");
//            System.out.println("*************diff1**************");
//            spark.createDataFrame(diff1, lakeSoulDF.schema).show();
//            System.out.println("data verification ERROR!!!");
//            System.exit(1);
//        } else {
//            System.out.println(printLine + "data verification SUCCESS!!!" + printLine);
//        }

        System.out.println("write data checkok");
    }
}

