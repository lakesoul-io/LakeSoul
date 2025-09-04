# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple
import logging
import subprocess
from e2e.lakesoul.data_init import Schema
from e2e.operators.python import PythonOperator
from e2e.utils import (
    random_str,
    oss_list_prefix_dir,
    oss_create_dir,
    get_jars_by_name,
    fill_skeleton,
    build_skeleton,
    upload,
)
from e2e.variables import (
    BUCKET,
    ACCESS_KEY_ID,
    SECRET_KEY,
    END_POINT,
    j2env,
    E2E_PATH_PREFIX,
    spark_submit_common_args,
    SPARK_COMPLETED_JOB_PREFIX,
)

from multiprocessing import Pipe, Process


def spark_check_driver(stream):
    pattern = r"termination reason:\s*(\w+)"
    for line in stream:
        results = re.findall(pattern, line)
        if "Error" in results:
            raise RuntimeError("spark job failed")


def spark_check_executor(logs: List[str]):
    from e2e.utils import oss_read

    def check_single(name):
        content = oss_read(name)
        for line in content.splitlines():
            data = json.loads(line)
            if data["Event"] == "SparkListenerJobEnd":
                if data["Job Result"]["Result"] != "JobSucceeded":
                    raise RuntimeError(f"spark job failed, log in {name}")

    for log in logs:
        check_single(log)


def do_spark_simple_task(
    task_path, log_prefix: str, extra_args: List[str] | None = None
):
    args = spark_submit_common_args[:]
    if extra_args:
        args.extend(extra_args)
    args.extend(
        [
            "--class",
            "com.dmetasoul.e2e.Main",
            "--name",
            f"{Path(task_path).stem}",
            "--jars",
            get_jars_by_name("spark"),
            "--conf",
            f"spark.eventLog.dir=s3://{BUCKET}/{log_prefix}",
            task_path,
        ]
    )
    logging.info(f"spark subprocess args: {' '.join(args)}")
    oss_create_dir(log_prefix, fresh=True)
    # TODO add timeout? use flink pattern

    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    try:
        spark_check_driver(proc.stdout)
    finally:
        proc.wait()

    # after spark task finished check history
    spark_check_executor(oss_list_prefix_dir(log_prefix))


def gen_sql_template(task_id: str, sqls: List[str]):
    template = j2env.get_template("spark-sql.java.j2")
    output = template.render(appName=task_id, sqls=sqls)
    fill_skeleton(output)
    # build
    build_skeleton()
    task_path = upload(task_id, rename=task_id)
    # run
    log_prefix = f"{SPARK_COMPLETED_JOB_PREFIX}/{task_id}"
    do_spark_simple_task(task_path, log_prefix)


class SparkSQLOperator(PythonOperator):
    def __init__(self, *, task_id: str | None = None, sqls: List[str], **kwargs):
        task_id = f"SparkSQL-{random_str()}" if task_id is None else task_id
        op_args = [task_id, sqls]
        super().__init__(
            task_id=task_id, python_callable=gen_sql_template, op_args=op_args, **kwargs
        )


def create_table(
    table_name: str,
    table_schema: Schema,
    partition_key: Optional[str],
    localtion: str,
    properties: Optional[Dict[str, str]],
    is_cdc: bool,
    need_drop: bool,
):
    def gen_partition(partition_key, schema) -> str:
        if partition_key is None:
            return ""
        if all(partition_key != f.name for f in schema.fields):
            raise RuntimeError("parition key is not in schema")
        return f"PARTITIONED BY ({partition_key}) "

    def gen_props(properties, is_cdc) -> str:
        if properties is None:
            return ""
        lst = []
        for k, v in properties.items():
            lst.append(f"{repr(k)} = {repr(v)}")
        if is_cdc:
            lst.append("'lakesoul_cdc_change_column' = 'op'")
        return f"TBLPROPERTIES({','.join(lst)})"

    lst = []
    if need_drop:
        lst.append(f"DROP TABLE IF EXISTS {table_name}")
    lst.append(
        f"CREATE TABLE {table_name} {table_schema.as_spark_sql()} USING lakesoul {gen_partition(partition_key, table_schema)} LOCATION '{localtion}' {gen_props(properties, is_cdc)}"
    )

    return lst


class SparkCreateTableOperator(SparkSQLOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        table_name: str,
        table_schema: Schema,
        partition_key: Optional[str] = None,
        localtion: str,
        properties: Optional[Dict[str, str]] = None,
        is_cdc: bool = False,
        need_drop: bool = True,
        **kwargs,
    ):
        task_id = f"SparkCreateTable-{random_str()}" if task_id is None else task_id
        sqls = create_table(
            table_name,
            table_schema,
            partition_key,
            localtion,
            properties,
            is_cdc,
            need_drop,
        )
        logging.info(sqls)
        super().__init__(task_id=task_id, sqls=sqls, **kwargs)


def spark_check(task_id, left_data_prefix, right_data_prefix, hash_key):
    logging.info(f"{task_id} started")

    def row_num(data_path, hash_key) -> int:
        import pyarrow.dataset as ds
        import pyarrow.fs as fs
        import pyarrow.compute as pc
        import pyarrow as pa

        s3 = fs.S3FileSystem(
            access_key=ACCESS_KEY_ID,
            secret_key=SECRET_KEY,
            region="us-east-1",
            endpoint_override=END_POINT,
        )

        dataset = ds.dataset(data_path, format="parquet", filesystem=s3)
        if hash_key:
            scanner = dataset.scanner(columns=[hash_key])
            # 直接合并所有 batch，避免多次转换
            arrays = []
            for batch in scanner.to_batches():
                arrays.append(batch.column(0))
            all_array = pa.concat_arrays(arrays)
            # 统计唯一值
            unique = pc.unique(all_array)  # type: ignore
            return len(unique)
        else:
            scanner = dataset.scanner()
            row_num = 0
            scanner = dataset.scanner()
            for batch in scanner.to_batches():
                row_num += batch.num_rows
            return row_num

    left = row_num(f"{BUCKET}/{left_data_prefix}", hash_key)
    right = row_num(f"{BUCKET}/{right_data_prefix}", hash_key)
    if left != right:
        raise RuntimeError(f"data nums does not match, {left} != {right}")
    logging.info(f"{task_id} finished")


class SparkCheckOperator(PythonOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        left_data_prefix: str,
        right_data_prefix: str,
        hash_key: str | None = None,
        **kwargs,
    ) -> None:
        task_id = f"SparkCheck-{random_str()}" if task_id is None else task_id
        op_args = [
            task_id,
            left_data_prefix,
            right_data_prefix,
            hash_key,
        ]
        super().__init__(
            task_id=task_id,
            python_callable=spark_check,
            op_args=op_args,
            **kwargs,
        )


def repeat_sink(
    task_id: str, table_name, has_primary_key, data_prefix: str, times: int
):
    def pre_source_task() -> Tuple[str, str]:
        app_name = f"{task_id}-source_before"
        template = j2env.get_template("spark-source.java.j2")
        output = template.render(
            appName=app_name,
            dataPath=f"s3://{BUCKET}/{E2E_PATH_PREFIX}/{task_id}/before/",
            tableName=table_name,
        )
        fill_skeleton(output)
        build_skeleton()
        task_path = upload(task_id, rename="source_before.jar")
        log_prefix = f"{SPARK_COMPLETED_JOB_PREFIX}/{task_id}/source_before/"
        return (task_path, log_prefix)

    def gen_sink_task(i, data_path) -> Tuple[str, str]:
        app_name = f"{task_id}-sink_{i}"
        template = j2env.get_template("spark-sink.java.j2")
        output = template.render(
            appName=app_name,
            dataPath=f"s3://{BUCKET}/{data_path}",
            tableName=table_name,
            hasPrimaryKey=has_primary_key,
        )
        fill_skeleton(output)
        build_skeleton()
        task_path = upload(task_id, f"sink_{i}.jar")
        log_prefix = f"{SPARK_COMPLETED_JOB_PREFIX}/{task_id}/sink_{i}/"
        return (task_path, log_prefix)

    def after_source_task() -> Tuple[str, str]:
        app_name = f"{task_id}-source_after"
        template = j2env.get_template("spark-source.java.j2")
        output = template.render(
            appName=app_name,
            dataPath=f"s3://{BUCKET}/{E2E_PATH_PREFIX}/{task_id}/after/",
            tableName=table_name,
        )
        fill_skeleton(output)
        build_skeleton()
        task_path = upload(task_id, "source_after.jar")
        log_prefix = f"{SPARK_COMPLETED_JOB_PREFIX}/{task_id}/source_after/"
        return (task_path, log_prefix)

    def do_tasks(tasks, extra_args):
        for task, log_prefix in tasks:
            do_spark_simple_task(task, log_prefix, extra_args)

    lst = oss_list_prefix_dir(data_prefix)
    if len(lst) < times:
        logging.error(f"Only {len(lst)} data files but repeat {times}")
        raise RuntimeError("data not enough")
    tasks = []
    tasks.append(pre_source_task())
    for i in range(times):
        tasks.append(gen_sink_task(i, lst[i]))
    tasks.append(after_source_task())
    extra_args = ["--conf", f"spark.kubernetes.executor.label.repeatsink={task_id}"]
    do_tasks(tasks, extra_args)


class SparkRepeatSinkOperator(PythonOperator):
    """
    task-id/(before|after)-randmom
    Sink multiple times
    read table to s3 , data -> lakesoul, then read data to s3 (for check)
    s3: xxxxxx/{task-id}/(before|after)/.....  this is a contract
    input: List[data]
    """

    def __init__(
        self,
        *,
        task_id: str | None = None,
        table_name: str,
        has_primary_key: bool,
        data_prefix: str,
        times: int = 1,
        **kwargs,
    ):
        task_id = f"SparkRepeatSink-{random_str()}" if task_id is None else task_id
        op_args = [task_id, table_name, has_primary_key, data_prefix, times]
        super().__init__(
            task_id=task_id, python_callable=repeat_sink, op_args=op_args, **kwargs
        )


def check_submit_log(conn, args, task_id):
    import setproctitle

    setproctitle.setproctitle(f"{task_id}-check_submit_log")
    with subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ) as proc:
        # extract driver pod name
        not_send = True
        for line in proc.stderr:  # type: ignore
            # 逐行处理标准输出
            if "Error" in line:
                logging.error(line)
                raise RuntimeError(f"Error in {line}")
            if not_send:
                pattern = re.compile(r"compaction-[a-z0-9]+-\w+-driver")
                m = re.search(pattern, line)
                if m:
                    conn.send(m.group())
                    not_send = False


def check_driver_log(conn, task_id):
    from kubernetes import client, config
    import time
    import threading
    import setproctitle
    from collections import defaultdict

    setproctitle.setproctitle(f"{task_id}-check_driver_log")
    config.load_kube_config()
    v1 = client.CoreV1Api()
    namespace = "default"
    pod_name = conn.recv()
    # wait 20s
    time.sleep(20)  # wait pod start

    compaction_status = defaultdict(str)

    def check_log_timeout(compaction_id):
        logging.debug(f"check timeout for {compaction_id}")
        if compaction_status[compaction_id] == "ending":
            del compaction_status[compaction_id]
        else:
            raise RuntimeError(f"{compaction_id} not finished")

    stream = v1.read_namespaced_pod_log(
        name=pod_name,
        namespace=namespace,
        follow=True,  # 实时流式输出用 True
        _preload_content=False,
    )
    pattern = re.compile(r"\[compaction-([a-z0-9\-]+)\]:\s*(\w+)")
    not_triggerd = True
    not_triggerd_timeout = 60 * 10
    start_time = time.time()
    for line in stream:
        if not_triggerd and time.time() - start_time > not_triggerd_timeout:
            raise RuntimeError("Compaction Trigger Timeout")
        txt = line.decode("utf-8").strip()
        match = pattern.search(txt)
        if match:
            not_triggerd = False
            compaction_id = match.group(1)
            status = match.group(2)
            logging.debug(f"[compaction] {compaction_id} {status}")
            compaction_status[compaction_id] = status
            if status == "beging":
                t = threading.Timer(5 * 60, check_log_timeout, args=[compaction_id])
                t.start()


def run_compaction(
    task_id: str,
    l0_num_limit: str | None,
    lx_file_num_limit: str | None,
    merge_num_limit: str | None,
    max_levels: str | None,
):
    args = spark_submit_common_args[:]
    log_prefix = f"{SPARK_COMPLETED_JOB_PREFIX}/{task_id}/"
    oss_create_dir(log_prefix, fresh=True)
    args.extend(
        [
            "--conf",
            f"spark.eventLog.dir=s3://{BUCKET}/{log_prefix}",
            "--conf",
            "spark.dynamicAllocation.enabled=true",
            "--conf",
            "spark.dynamicAllocation.shuffleTracking.enabled=true",
            "--conf",
            "spark.dynamicAllocation.minExecutors=1",
            "--conf",
            "spark.dynamicAllocation.maxExecutors=2",
            "--conf",
            "spark.dynamicAllocation.initialExecutors=1",
            "--conf",
            "spark.executor.extraJavaOptions=-XX:MaxDirectMemorySize=1G",
            "--name",
            task_id,
            "--class",
            "com.dmetasoul.lakesoul.spark.compaction.NewCompactionTask",
            get_jars_by_name("spark"),
        ]
    )
    if l0_num_limit:
        args.extend(
            [
                "--conf",
                f"spark.dmetasoul.lakesoul.compaction.level0.file.number.limit={l0_num_limit}",
            ]
        )
    if lx_file_num_limit:
        args.extend(
            [
                "--conf",
                f"spark.dmetasoul.lakesoul.compaction.level.file.number.limit={lx_file_num_limit}",
            ]
        )
    if merge_num_limit:
        args.extend(
            [
                "--conf",
                f"spark.dmetasoul.lakesoul.compaction.level.file.merge.num.limit={merge_num_limit}",
            ]
        )
    if max_levels:
        args.extend(
            ["--conf", f"spark.dmetasoul.lakesoul.max.num.levels.limit={max_levels}"]
        )

    logging.info(" ".join(args))
    parent_conn, child_conn = Pipe()
    p1 = Process(target=check_submit_log, args=[child_conn, args, task_id])
    p1.start()
    p2 = Process(target=check_driver_log, args=[parent_conn, task_id])
    p2.start()
    p1.join()
    p1.close()
    p2.join()
    p2.close()


class CompactionOperator(PythonOperator):
    # need shutdown?
    def __init__(
        self,
        *,
        task_id: str | None = None,
        l0_num_limit: str | None = None,
        lx_file_num_limit: str | None = None,
        merge_num_limit: str | None = None,
        max_levels: str | None = None,
        **kwargs,
    ):
        task_id = f"Compaction-{random_str()}" if task_id is None else task_id
        op_args = [
            task_id,
            l0_num_limit,
            lx_file_num_limit,
            merge_num_limit,
            max_levels,
        ]
        super().__init__(
            task_id=task_id, python_callable=run_compaction, op_args=op_args, **kwargs
        )
