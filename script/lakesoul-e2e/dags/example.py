# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


import logging

from e2e.lakesoul import (
    SparkSQLOperator,
    DataInitOperator,
    Schema,
    Field,
    DataType,
    SparkCheckOperator,
    SparkCreateTableOperator,
    SparkRepeatSinkOperator,
    CompactionOperator,
    OSSOperator,
    CleanJobOperator,
)

from e2e.models.dag import DAG

log = logging.getLogger(__name__)


with DAG(
    dag_id="lakesoul-e2e",
) as dag:
    # comp = CompileOperator(
    #     repo_url="https://github.com/mag1c1an1/LakeSoul.git", branch="tmp_name"
    # )

    data_prefix = "jiax/lakesoul-e2e/data-init/"
    schema = Schema(
        [
            Field("f1", DataType.Int32),
            Field("f2", DataType.Float32),
            Field("f3", DataType.Date32),
        ],
    )

    o = OSSOperator(kind="delete", prefix=data_prefix)

    data_init = DataInitOperator(
        schema=schema, rows=100, times=1, dest_prefix=data_prefix
    )

    create_table = SparkCreateTableOperator(
        table_name="test_table_1",
        table_schema=schema,
        localtion="s3://dmetasoul-bucket/jiax/lakesoul-e2e/test_table_1/",
        need_drop=True,
        # partition_key="f3",
        properties={"hashPartitions": "f1", "hashBucketNum": "2"},
        is_cdc=False,
    )

    sinks = SparkRepeatSinkOperator(
        task_id="repeat_sink_1707",
        table_name="test_table_1",
        has_primary_key=True,
        data_prefix=data_prefix,
        times=1,
    )

    check = SparkCheckOperator(
        left_data_prefix=data_prefix,
        right_data_prefix="jiax/lakesoul-e2e/repeat_sink_1707/after",
        hash_key="f1",
    )

    o >> data_init >> create_table >> sinks >> check  # type: ignore


with DAG(dag_id="back_compaction") as dag:
    c = CompactionOperator(
        l0_num_limit="2",
        lx_file_num_limit="2",
        merge_num_limit="2",
        max_levels="2",
    )


with DAG(
    dag_id="clean",
) as dag:
    clean = CleanJobOperator(
        db_name="lakesoul_e2e",
        host="pgsvc.default.svc.cluster.local",
        slot_name="flink",
        plugin_name="pgoutput",
        on_timer_interval="1",
        schema_list="public",
        expired_time="60000",  # 1min
    )
