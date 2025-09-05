# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import logging
import subprocess

from e2e.operators.python import PythonOperator
from e2e.utils import (
    random_str,
    oss_create_dir,
    oss_list_prefix_dir,
    get_jars_by_name,
    oss_read,
    build_skeleton,
    upload,
)

from e2e.variables import (
    FLINK_COMPLETED_JOB_DIR,
    LAKESOUL_PG_PASSWORD,
    LAKESOUL_PG_URL,
    LAKESOUL_PG_USERNAME,
    BUCKET,
    END_POINT,
    ACCESS_KEY_ID,
    SECRET_KEY,
    E2E_PATH_PREFIX,
    flink_common_args,
    j2env,
)


def flink_check_job(log_prefix: str):
    import json

    paths = oss_list_prefix_dir(log_prefix)
    for p in paths:
        content = oss_read(p)
        data = json.loads(content)
        inner = data["archive"][1]["json"]
        res = json.loads(inner)
        failed = res["jobs"][0]["tasks"]["failed"]
        if failed != 0:
            raise Exception("Flink Job failed to execute")


def check_flink_logs(task_id, f=None, timeout=120):
    """
    make sure jobmanager and taskmanager all started
    """
    from kubernetes import client, config, watch
    import time
    import threading

    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    finished = False
    thread = None
    for e in w.stream(
        v1.list_namespaced_pod,
        namespace="default",
        label_selector=f"app={task_id}",
        timeout_seconds=timeout,
    ):
        typ = e.get("type")  # type: ignore
        obj = e.get("object")  # type: ignore
        name = obj.metadata.name  #  type: ignore
        namespace = obj.metadata.namespace  # type: ignore
        if typ == "ADDED":
            start_time = time.time()
            while time.time() - start_time < timeout:
                pod = v1.read_namespaced_pod(name=name, namespace=namespace)
                container_statuses = pod.status.container_statuses or []  # type: ignore
                if container_statuses[0].state.running is not None:
                    break
                time.sleep(1)
            # read log
            if f:
                thread = threading.Thread(target=f, args=[name, namespace])
        elif e.get("type") == "DELETED":  # type: ignore
            finished = True
            break

    if not finished:
        raise RuntimeError("Spy on k8s failed")
    if f and not thread:
        raise RuntimeError("read log failed")
    if thread:
        thread.join()


def gen_sql_template(task_id, streaming):
    template = j2env.get_template("flink-sql.java.j2")
    output = template.render(
        appName=task_id,
        streaming=streaming,
        sqls=["select * from lakesoul.`default`.test_table_1;"],
    )
    with open(
        "java-skeleton/src/main/java/com/dmetasoul/e2e/Main.java",
        "w",
        encoding="utf-8",
    ) as f:
        f.write(output)


def gen_flink_pod_template():
    files = list(
        filter(
            lambda x: "lakesoul-flink" in x,
            oss_list_prefix_dir(f"{E2E_PATH_PREFIX}/compile/"),  # this is fixed
        )
    )
    assert len(files) == 1

    template = j2env.get_template("flink-pod.yaml.j2")

    output = template.render(
        jarPath=files[0],
        accessKey=ACCESS_KEY_ID,
        secretKey=SECRET_KEY,
        endpoint=END_POINT,
        bucket=BUCKET,
    )
    with open("k8s/flink-pod.yaml", "w", encoding="utf-8") as f:
        f.write(output)


def flink_sql(task_id):
    gen_sql_template(task_id, False)
    build_skeleton()
    dest = upload(task_id)
    log_prefix = f"s3://{BUCKET}/{FLINK_COMPLETED_JOB_DIR}/{task_id}"
    oss_create_dir(log_prefix, fresh=True)

    gen_flink_pod_template()
    args = flink_common_args[:]
    extra_args = [
        "--class",
        "com.dmetasoul.e2e.Main",
        "-Dkubernetes.pod-template-file.default=k8s/flink-pod.yaml",
        f"-Dkubernetes.cluster-id={task_id.lower()}",
        f"-Djobmanager.archive.fs.dir={log_prefix}",  # static
        dest,
    ]
    args.extend(extra_args)
    logging.info(" ".join(args))
    subprocess.run(args, check=True)
    # flink client shutdown quickly
    check_flink_logs(task_id)
    flink_check_job(log_prefix)


class FlinkSQLOperator(PythonOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        **kwargs,
    ) -> None:
        task_id = f"FlinkSQL-{random_str()}" if task_id is None else task_id
        op_args = [task_id]
        super().__init__(
            task_id=task_id,
            python_callable=flink_sql,
            op_args=op_args,
            **kwargs,
        )


def clean_job(
    task_id,
    user_name,
    db_name,
    password,
    host,
    port,
    slot_name,
    plugin_name,
    split_size,
    schema_list,
    pg_url,
    source_parallelism,
    target_tables,
    expired_time,
    on_timer_interval,
):
    gen_flink_pod_template()
    log_prefix = f"s3://{BUCKET}/{FLINK_COMPLETED_JOB_DIR}/{task_id}"
    args = flink_common_args[:]
    extra_args = [
        "--class",
        "org.apache.flink.lakesoul.entry.clean.NewCleanJob",
        "-Dkubernetes.pod-template-file.default=k8s/flink-pod.yaml",
        f"-Dkubernetes.cluster-id={task_id.lower()}",
        f"-Djobmanager.archive.fs.dir={log_prefix}",  # static
        get_jars_by_name("flink"),
        "--source_db.user",
        user_name,
        "--source_db.password",
        password,
        "--source_db.port",
        port,
        "--splitSize",
        split_size,
        "--url",
        pg_url,
        "--source.parallelism",
        source_parallelism,
        "--ontimer_interval",
        on_timer_interval,
    ]
    args.extend(extra_args)

    if db_name:
        args.extend(["--source_db.dbName", db_name])
    if host:
        args.extend(["--source_db.host", host])
    if slot_name:
        args.extend(["--slotName", slot_name])
    if plugin_name:
        args.extend(["--plugName", plugin_name])
    if schema_list:
        args.extend(["--schemaList", schema_list])
    if target_tables:
        args.extend(["--targetTableName", target_tables])
    if expired_time:
        args.extend(["--dataExpiredTime", expired_time])

    logging.info(" ".join(args))
    subprocess.run(args, check=True,capture_output=True)

    def check_clean(name, namespace):
        from kubernetes import client, config
        import time
        import re
        from collections import defaultdict
        import threading

        logging.info(name)
        if "jobmanager" in name:
            return

        clean_status = defaultdict(str)
        config.load_config()
        v1 = client.CoreV1Api()
        stream = v1.read_namespaced_pod_log(
            name=name,
            namespace=namespace,
            follow=True,
            _preload_content=False,
        )

        pattern = re.compile(
            r"\[Clean-([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})\]: (\w+)"
        )
        not_triggerd = True
        not_triggerd_timeout = 60 * 10
        start_time = time.time()

        def check_log_timeout(clean_id):
            logging.debug(f"check timeout for {clean_id}")
            if clean_status[clean_id] == "success":
                del clean_status[clean_id]
            else:
                raise RuntimeError(f"{clean_id} not finished")

        for line in stream:
            if not_triggerd and time.time() - start_time > not_triggerd_timeout:
                raise RuntimeError("Clean Trigger Timeout")
            txt = line.decode("utf-8").strip()
            match = pattern.search(txt)
            if match:
                not_triggerd = False
                clean_id = match.group(1)
                status = match.group(2)
                logging.debug(f"[clean] {clean_id} {status}")
                clean_status[clean_id] = status
                if status == "begin":
                    t = threading.Timer(5 * 60, check_log_timeout, args=[clean_id])
                    t.start()

    check_flink_logs(task_id, f=check_clean, timeout=999999999)
    flink_check_job(log_prefix)


class CleanJobOperator(PythonOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        user_name: str = LAKESOUL_PG_USERNAME,
        db_name: str | None,
        password: str = LAKESOUL_PG_PASSWORD,
        host: str | None,
        port: str = "5432",
        slot_name: str | None,
        plugin_name: str | None,
        split_size: str = "1024",
        schema_list: str = "public",
        pg_url: str = LAKESOUL_PG_URL,
        source_parallelism: str = "1",
        target_tables: str | None = None,
        expired_time: str | None,
        on_timer_interval: str = "5",
        **kwargs,
    ) -> None:
        task_id = f"cleanjob-{random_str()}" if task_id is None else task_id
        op_args = [
            task_id,
            user_name,
            db_name,
            password,
            host,
            port,
            slot_name,
            plugin_name,
            split_size,
            schema_list,
            pg_url,
            source_parallelism,
            target_tables,
            expired_time,
            on_timer_interval,
        ]
        super().__init__(
            task_id=task_id,
            python_callable=clean_job,
            op_args=op_args,
            **kwargs,
        )
