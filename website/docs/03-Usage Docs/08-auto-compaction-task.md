# LakeSoul Global Automatic Compaction Service Usage

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

:::tip
Since 2.3.0
:::

When the data is written in batch or streaming tasks, the data is mostly written in small batches, therefore there are some intermediate data and a large number of small files. In order to reduce the waste of resources caused by such data and improve the efficiency of data reading, compaction need to be executed periodically for all tables.

If we perform compaction from within a writing job (such as a stream job), the write process may be blocked and latency and throughput maybe impacted. If we start compaction task for each table in a separate job, it will be cumbersome to setup and deploy. Therefore, LakeSoul provides a global automatic compaction service, which can automatically compress the data according to the database and write partition data, and the compaction task can be automatically scaled.

## Implementation Details
- Depending on PG's trigger notify listen mechanism, define a trigger function in PLSQL in PG: each time data is written, it can trigger the execution of a defined function, analyze and process the partitions that meet the compaction conditions in the function (for example, there are 10 submissions since the last compaction), and then publish the information;
- The backend starts a long running Spark job that listens to the event published by PG, and then starts the Spark job to compress the data of partitions that meet the compaction conditions. So that this one Spark job would be responsible for the compaction of all tables automatically.

Currently, compaction is only performed according to the version of the written partition, and the execution of the compaction service will be triggered every 10 commits.

## Start Compaction Service

The trigger and PLSQL functions have been configured when the database is initialized, and the default compaction configuration will trigger a compaction signal every time a partition is inserted 10 times, so you only need to start the Spark automatic compaction job.

Download LakeSoul's Spark release jar file, add the dependent jar package through --jars when submitting the job, and then start the Spark automatic compaction service job.

1. Setup metadata connection for LakeSoul. For detailed documentation, please refer
   to [Setup Spark Job](../03-Usage%20Docs/02-setup-spark.md)
2. Submit the Spark job. The currently supported parameters are as follows:

| Parameter       | Meaning                                                                                                                          | required | default |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------- | -------- | ------- |
| threadpool.size | the thread pools number of automatic compaction task                                                                             | false    | 8       |
| database        | The database name to compress. If it is not filled, it means that all database partitions will compress that meet the conditions | false    | ""      |

The use the following command to start the compaction service job:
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
Because LakeSoul enables native IO by default and needs to rely on off-heap memory, the spark task needs to set the size of off-heap memory, otherwise it is prone to out-of-heap memory overflow.
:::

:::tip
Your could enable Spark's dynamic allocation to get auto-scaling for the compaction service job. Refer to Spark's doc [Dynamic Resource Allocation](https://spark.apache.org/docs/3.3.1/job-scheduling.html#dynamic-resource-allocation) on how to config.
:::