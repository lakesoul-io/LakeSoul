import subprocess
from e2etest.k8s import flink_wait_until_finish
from e2etest.task import SubTask
import json
import os

from e2etest.vars import (
    BUCKET,
    E2E_CLASSPATH,
    END_POINT,
    LAKESOUL_FLINK_PATH,
    FLINK_COMPLETED_JOB_DIR,
    LAKESOUL_PG_DRIVER,
    LAKESOUL_PG_PASSWORD,
    LAKESOUL_PG_URL,
    LAKESOUL_PG_USERNAME,
    MVN_LOCAL,
    VERSION,
)

from e2etest.s3 import s3_read


def flink_check_job(key: str):
    content = s3_read(key)
    data = json.loads(content)
    inner = data["archive"][1]["json"]
    res = json.loads(inner)
    failed = res["jobs"][0]["tasks"]["failed"]
    if failed != 0:
        raise Exception("Flink Job failed to execute")


class FlinkSubTask(SubTask):
    """Act as a flink task in session mode"""

    def __init__(self, name, entry, mode, conf):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.depoly = conf["deploy"]
        self.jobmanager = conf["jobmanager"]
        self.target = os.path.expanduser(
            f"{MVN_LOCAL}/flink-e2e/{VERSION}/flink-e2e-{VERSION}.jar"
        )

        self.lib = f"{END_POINT}/{BUCKET}/{E2E_CLASSPATH}/{LAKESOUL_FLINK_PATH.name}"

    def run(self, **conf):
        args = ["flink", "run", "--classpath", self.lib, "-c", self.entry, self.target]
        subprocess.run(args, check=True)

    def __repr__(self):
        return f"FlinkSubTask {{ {self.mode} {self.name} {self.entry} {self.target} {self.depoly} {self.jobmanager}}}"


class FlinkK8SApplicationTask(SubTask):
    """Act as a flink task in application mode deployed by k8s"""

    def __init__(self, name, entry, mode, conf):
        self.name = name
        self.entry = entry
        self.mode = mode
        self.depoly = conf["deploy"]
        self.template = conf["template"]
        self.image = conf["image"]
        self.timeout=  conf['timeout']
        self.target = f"s3://{BUCKET}/{E2E_CLASSPATH}/flink-e2e-{VERSION}.jar"
        self.lib = f"{END_POINT}/{BUCKET}/{E2E_CLASSPATH}/{LAKESOUL_FLINK_PATH.name}"

    def run(self, **conf):
        args = [
            "flink",
            "run-application",
            "--target",
            "kubernetes-application",
            "--class",
            self.entry,
            "--classpath",
            self.lib,
            f"-Djobmanager.archive.fs.dir=s3://{BUCKET}/{FLINK_COMPLETED_JOB_DIR}",  # static
            f"-Dkubernetes.pod-template-file.default={self.template}",
            f"-Dcontainerized.master.env.LAKESOUL_PG_DRIVER={LAKESOUL_PG_DRIVER}",
            f"-Dcontainerized.master.env.LAKESOUL_PG_USERNAME={LAKESOUL_PG_USERNAME}",
            f"-Dcontainerized.master.env.LAKESOUL_PG_PASSWORD={LAKESOUL_PG_PASSWORD}",
            f"-Dcontainerized.master.env.LAKESOUL_PG_URL={LAKESOUL_PG_URL}",
            f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_DRIVER={LAKESOUL_PG_DRIVER}",
            f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_USERNAME={LAKESOUL_PG_USERNAME}",
            f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_PASSWORD={LAKESOUL_PG_PASSWORD}",
            f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_URL={LAKESOUL_PG_URL}",
            "-Dkubernetes.artifacts.local-upload-enabled=true",
            "-Dkubernetes.artifacts.local-upload-overwrite=true",
            f"-Dkubernetes.artifacts.local-upload-target=s3://{BUCKET}/{E2E_CLASSPATH}/",
            "-Dkubernetes.cluster-id=lakesoul-e2e-flink",
            f"-Dkubernetes.container.image.ref={self.image}",  # static
            self.target,
        ]
        # need run k8s and check its status
        subprocess.run(args, check=True)
        # wait until finish
        ids = flink_wait_until_finish(
            "app=lakesoul-e2e-flink,component=jobmanager", self.timeout
        )
        # check flink history
        for id in ids:
            print(id)
            flink_check_job(f"{FLINK_COMPLETED_JOB_DIR}/{id}")

    def __repr__(self):
        return f"FlinkK8SApplicationTask {{ {self.mode} {self.name} {self.entry} {self.target} {self.depoly} }}"
