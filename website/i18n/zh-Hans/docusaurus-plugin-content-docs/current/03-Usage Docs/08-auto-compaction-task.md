# LakeSoul 全局自动压缩服务使用方法

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

:::tip
该功能于 2.3.0 版本起提供
:::

不管是批量还是流式任务的数据写入，因为数据多是merge方式写入，所以存在一些中间冗余数据以及大量小文件情况，为了减少此类数据造成资源的浪费以及提高数据读取的效率，需要将数据进行压缩。

传统上如果我们在写入作业（例如流作业）中执行压缩，则写任务可能会被阻塞，影响延迟和吞吐。而如果我们为每个表启动一个周期压缩任务，部署又比较繁琐。因此 LakeSoul 提供了全局自动压缩任务，可以根据数据库以及写入分区数据情况自动进行数据压缩，压缩任务支持自动弹性伸缩。

## 实现原理解析
- 依赖 PG 的 trigger-notify-listen 机制，在 PG 中的 PLSQL 中定义一个触发器函数：每当数据写入时可以触发执行一个定义函数，在函数中分析处理满足压缩条件的分区（例如，自上次压缩以来有 10 次提交），然后将信息发布出去；
- 后端启动一个实时监听任务，这是一个 Spark 任务，长期运行并监听来自 PG 发布的信息，然后对满足压缩条件的分区启动数据启动进行数据压缩。如此，这个 Spark 任务会负责全局所有表的压缩。

目前只根据写入分区 version 情况进行压缩，每提交 10 次会触发压缩任务的执行。

## 压缩任务启动方式

trigger 和 pg 函数在数据库初始化的时候已经配置，默认压缩配置，分区每插入 10 次会触发进行压缩信号，所以只需要启动 Spark 自动压缩作业就好。

通过下载 LakeSoul spark release 的 jar 包，任务提交时通过 --jars 方式添加依赖jar包，然后启动 Spark 自动压缩任务即可。

1. 为 Spark 作业配置 LakeSoul 元数据库连接，详细说明可以参考 [LakeSoul设置 Spark 工程/作业](../03-Usage%20Docs/02-setup-spark.md) ；
2. 提交作业。目前支持参数信息如下：

| 参数              | 参数说明                                          | 是否必要 | 默认值        |
|-----------------|-----------------------------------------------|------|------------|
| threadpool.size | 自动压缩任务线程池数量                                   | 不必要  | 8          |
| database        | 要压缩的 LakaSoul 库名称。若不填写，表示不区分数据库对满足条件的分区进行数据压缩 | 不必要  | 空。默认为所有库 |

本地启动 Spark 自动压缩命令：
```shell
./bin/spark-submit \
    --name auto_compaction_task \
    --master yarn  \
    --deploy-mode cluster \
    --executor-memory 3g \
    --executor-cores 1 \
    --num-executors 20 \
    --conf "spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=4G" \
    --conf "spark.executor.memoryOverhead=3g" \
    --class com.dmetasoul.lakesoul.spark.compaction.CompactionTask  \
    jars/lakesoul-spark-3.3-VAR::VERSION.jar 
    --threadpool.size=10
    --database=test
```
:::tip
因为LakeSoul默认开启native IO 需要依赖堆外内存，所以 Spark 任务需要设置堆外内存大小，否则容易出现堆外内存溢出问题。
:::

:::tip
可以为 Spark 全局压缩任务开启 [Dynamic Allocation](https://spark.apache.org/docs/3.3.1/job-scheduling.html#dynamic-resource-allocation)，使得该任务可以根据需要自动弹性伸缩资源。
:::