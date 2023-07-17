<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

properties中设置数据库以及表链接信息

 第一步：1_create_table.py 随机创建table_num张表，每张表主键id，除主键外还包含随机1~10个字段列，列字段类型随机，表名以`random_table_*`开头；
 第二步：2_insert_table_data.sh 向上个随机生成的表中插入数据；
 第三步：3_add_column.py 像随机生成的表中增加列；
 第四步：4_update_data.py 修改表中已存在数据；
 第五步：5_change_column_type.py 修改列字段类型；
 第六步：6_drop_column.py 随机删除表中增加的列；

 delete_data.py 随机删除表中因存在的数据
 drop_table.py 删除生成的`random_table_*`表。

以数字开头的脚本需要严格按照顺序执行，2_insert_table_data.sh 可以在第3、5、6步后再次执行插入数据。

以上为数据库自动生成数据脚本，配合org.apache.spark.sql.lakesoul.benchmark.Benchmark进行对比mysql和lakesoul数据准确性。

auto_test.sh对各个执行步骤进行了整合，执行auto_test.sh可自动化执行测试全流程。
