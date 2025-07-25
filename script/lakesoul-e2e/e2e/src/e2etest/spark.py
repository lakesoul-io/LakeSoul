import json
import os
import subprocess
from typing import Set
from e2etest.task import SubTask
from e2etest.vars import (
    BUCKET,
    E2E_CLASSPATH,
    LAKESOUL_SPARK_PATH,
    MVN_LOCAL,
    VERSION,
    SPARK_COMPLETED_JOB_DIR,
    ACCESS_KEY_ID,
    SECRET_KEY,
    END_POINT,
    LAKESOUL_PG_DRIVER,
    LAKESOUL_PG_USERNAME,
    LAKESOUL_PG_PASSWORD,
    LAKESOUL_PG_URL,
)
import re
from e2etest.s3 import s3_list_prefix_dir, s3_read


from e2etest.k8s import api_server_address


def spark_check_driver(stdout):
    pattern = r"termination reason:\s*(\w+)"
    results = re.findall(pattern, stdout)
    if "Error" in results:
        raise RuntimeError("spark job failed")


def check_log(content: str):
    for line in content.splitlines():
        data = json.loads(line)
        if data["Event"] == "SparkListenerJobEnd":
            if data["Job Result"]["Result"] != "JobSucceeded":
                raise RuntimeError("spark job failed")


def spark_check_executor(logs: Set[str]):
    def check_single(name):
        content = s3_read(name)
        check_log(content)

    for log in logs:
        check_single(log)


class SparkSubTask(SubTask):
    """Act as a spark task"""

    def __init__(self, name, entry, mode, conf):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.deploy = conf["deploy"]
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/spark-e2e/{VERSION}/spark-e2e-{VERSION}.jar"
        )
        self.lib = f"s3://{BUCKET}/{E2E_CLASSPATH}/{LAKESOUL_SPARK_PATH.name}"

    def run(self, **conf):
        args = [
            "spark-submit",
            "--jars",
            self.lib,
            "--class",
            self.entry,
            "--master",
            "local[*]",
            self.target,
        ]
        subprocess.run(args, check=True)

    def __repr__(self):
        return f"SparkSubTask {{ {self.mode} {self.name} {self.entry} {self.target}}}"


class SparkK8SClusterTask(SubTask):
    """Act as a spark task in cluster mode on k8s"""

    def __init__(self, name, entry, mode, conf):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.deploy = conf["deploy"]
        self.target = f"s3://{BUCKET}/{E2E_CLASSPATH}/spark-e2e-{VERSION}.jar"
        self.lib = f"s3://{BUCKET}/{E2E_CLASSPATH}/{LAKESOUL_SPARK_PATH.name}"
        self.image = conf["image"]

    def run(self, **conf):
        args = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            "--master",
            f"k8s://{api_server_address()}",
            "--jars",
            self.lib,
            "--name",
            f"lakesoul-e2e-{self.name}",
            "--class",
            self.entry,
            "--conf",
            "spark.executor.instances=1",
            "--conf",
            "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            "--conf",
            f"spark.kubernetes.container.image={self.image}",
            "--conf",
            f"spark.hadoop.fs.s3a.access.key={ACCESS_KEY_ID}",
            "--conf",
            f"spark.hadoop.fs.s3a.secret.key={SECRET_KEY}",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint={END_POINT}",
            "--conf",
            "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            "spark.kubernetes.container.image.pullPolicy=Always",
            "--conf",
            f"spark.kubernetes.driverEnv.LAKESOUL_PG_DRIVER={LAKESOUL_PG_DRIVER}",
            "--conf",
            f"spark.kubernetes.driverEnv.LAKESOUL_PG_USERNAME={LAKESOUL_PG_USERNAME}",
            "--conf",
            f"spark.kubernetes.driverEnv.LAKESOUL_PG_PASSWORD={LAKESOUL_PG_PASSWORD}",
            "--conf",
            f"spark.kubernetes.driverEnv.LAKESOUL_PG_URL={LAKESOUL_PG_URL}",
            "--conf",
            f"spark.executorEnv.LAKESOUL_PG_DRIVER={LAKESOUL_PG_DRIVER}",
            "--conf",
            f"spark.executorEnv.LAKESOUL_PG_USERNAME={LAKESOUL_PG_USERNAME}",
            "--conf",
            f"spark.executorEnv.LAKESOUL_PG_PASSWORD={LAKESOUL_PG_PASSWORD}",
            "--conf",
            f"spark.executorEnv.LAKESOUL_PG_URL={LAKESOUL_PG_URL}",
            "--conf",
            "spark.eventLog.enabled=true",
            "--conf",
            f"spark.eventLog.dir=s3://{BUCKET}/{SPARK_COMPLETED_JOB_DIR}",
            self.target,
        ]
        before = set(s3_list_prefix_dir(SPARK_COMPLETED_JOB_DIR))
        res = subprocess.run(args, check=True, capture_output=True, text=True)
        print(res.stdout)
        spark_check_driver(res.stdout)
        after = set(s3_list_prefix_dir(SPARK_COMPLETED_JOB_DIR))
        logs = after - before
        spark_check_executor(logs)

    def __repr__(self):
        return f"SparkK8SClusterTask {{ {self.mode} {self.name} {self.entry} {self.target}}}"
