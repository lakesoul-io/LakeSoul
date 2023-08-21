# 将LakeSoul数据挂载到hive meta用法教程

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

自 2.0 版本起，LakeSoul 支持将 Compaction 后的目录路径，挂载到指定的 Hive 表，指定和 LakeSoul 分区名一致和自定义分区名两种功能。该功能可以方便下游一些只能支持访问 Hive 的系统读取到 LakeSoul 的数据。更推荐的方式是通过 Kyuubi 来支持 Hive JDBC，这样可以直接使用 Hive JDBC 调用 Spark 引擎来访问 LakeSoul 表，包括 Merge on Read 读取。

## 保持和 LakeSoul Range 分区名一致
用户可以不添加 hive 分区名信息，则默认和 LakeSoul 分区名保持一致。

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-01'", "spark_catalog.default.hive_test_table")
```

## 自定义 hive 分区名
用户也可自定义 hive 分区名，规范数据在 hive 分区使用规范，
```scala

import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-02'", "spark_catalog.default.hive_test_table", "date='20210102'")
```
**注意** 数据挂载到 hive meta 功能需要和压缩功能一起使用。相关数据压缩功能介绍请看API： 6. Compaction


