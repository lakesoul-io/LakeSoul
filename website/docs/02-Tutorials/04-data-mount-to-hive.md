# Mount LakeSoul Data to Hive Meta

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Since version 2.0, LakeSoul supports two functions: attaching the directory path after Compaction to the specified Hive table, specifying the same partition name as LakeSoul, and customizing the partition name. This function can facilitate downstream systems that can only support Hive to read LakeSoul data. It is more recommended to support Hive JDBC through Kyuubi, so that you can directly use Hive JDBC to call the Spark engine to access the LakeSoul table, including Merge on Read.

## Keep the Same Partition Name as LakeSoul Range
The user can not add the hive partition name information, which is consistent with the LakeSoul partition name by default.

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-01'", "spark_catalog.default.hive_test_table")
```

## Custom Hive Partition Name
You can also customize the hive partition name to standardize the use of data in the hive partition.

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-02'", "spark_catalog.default.hive_test_table", "date='20210102'")
```

**Note * * The function of attaching data to the hive meta needs to be used together with the compression function. Please refer to API: 6. Comparison for relevant data compression functions

