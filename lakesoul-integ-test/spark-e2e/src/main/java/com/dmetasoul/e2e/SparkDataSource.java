/*
 * SPDX-FileCopyrightText: 2025 LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.dmetasoul.e2e;


import org.apache.spark.sql.SparkSession;

/**
 * @author mag1cian
 */
public class SparkDataSource {
    public static void main(String[] args) {
        var spark = SparkSession.builder().config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
                .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
                .config("spark.sql.defaultCatalog", "lakesoul")
                .getOrCreate();
        var df = spark.sql("select * from lakesoul_e2e_test");
        df.show();
    }
}
