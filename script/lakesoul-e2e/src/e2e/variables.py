# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import os
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

from .utils.k8s import api_server_address

E2E_PATH_PREFIX = "jiax/lakesoul-e2e"

# oss
END_POINT = os.getenv("AWS_ENDPOINT", "")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
BUCKET = os.getenv("AWS_BUCKET", "")
REGION = os.getenv("AWS_REGION", "auto")

# pg
LAKESOUL_PG_URL = os.getenv(
    "LAKESOUL_PG_URL",
    "jdbc:postgresql://pgsvc.default.svc.cluster.local:5432/lakesoul_e2e?stringtype=unspecified",
)
LAKESOUL_PG_DRIVER = os.getenv(
    "LAKESOUL_PG_DRIVER", "com.lakesoul.shaded.org.postgresql.Driver"
)
LAKESOUL_PG_USERNAME = os.getenv("LAKESOUL_PG_USERNAME", "lakesoul_e2e")
LAKESOUL_PG_PASSWORD = os.getenv("LAKESOUL_PG_PASSWORD", "lakesoul_e2e")

SPARK_IMAGE = (
    "swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/spark-hadoop-py-3.3.2:1.0"
)

FLINK_IMAGE = (
    "swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/flink-hadoop-3.3.6:1.2"
    # "swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/pyflink:latest"
)


# for logs
FLINK_COMPLETED_JOB_DIR = f"{E2E_PATH_PREFIX}/flink/completed-jobs"
SPARK_COMPLETED_JOB_PREFIX = f"{E2E_PATH_PREFIX}/spark/completed-jobs"

spark_submit_common_args = [
    "spark-submit",
    "--deploy-mode",
    "cluster",
    "--master",
    f"k8s://{api_server_address()}",
    "--conf",
    "spark.executor.instances=1",
    "--conf",
    "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
    "--conf",
    f"spark.kubernetes.container.image={SPARK_IMAGE}",
    "--conf",
    "spark.kubernetes.driver.request.cores=1",
    "--conf",
    "spark.driver.cores=1",
    "--conf",
    "spark.kubernetes.executor.request.cores=1",
    "--conf",
    "spark.executor.cores=1",
    "--conf",
    "spark.driver.memory=512m",
    "--conf",
    "spark.executor.memory=512m",
    "--conf",
    "spark.executor.memoryOverhead=512m",
    "--conf",
    "spark.driver.memoryOverhead=512m",
    "--conf",
    "spark.executor.pyspark.memory=512m",
    "--conf",
    "spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension",
    "--conf",
    "spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog",
    "--conf",
    "spark.sql.defaultCatalog=lakesoul",
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
    # "--conf",
    # "spark.kubernetes.container.image.pullPolicy=Always",
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
]

flink_common_args = [
    "flink",
    "run-application",  # 2.2 is run
    "--target",
    "kubernetes-application",
    "--parallelism",
    "1",
    "-Dtaskmanager.memory.process.size=1024m",
    "-Dkubernetes.taskmanager.cpu=1",
    "-Dtaskmanager.numberOfTaskSlots=2",
    f"-Ds3.access-key={ACCESS_KEY_ID}",
    f"-Ds3.secret-key={SECRET_KEY}",
    f"-Ds3.endpoint={END_POINT}",
    "-Ds3.path.style.access=true",
    "-Ds3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
    f"-Dkubernetes.container.image.ref={FLINK_IMAGE}",
    f"-Dcontainerized.master.env.LAKESOUL_PG_DRIVER={LAKESOUL_PG_DRIVER}",
    f"-Dcontainerized.master.env.LAKESOUL_PG_USERNAME={LAKESOUL_PG_USERNAME}",
    f"-Dcontainerized.master.env.LAKESOUL_PG_PASSWORD={LAKESOUL_PG_PASSWORD}",
    f"-Dcontainerized.master.env.LAKESOUL_PG_URL={LAKESOUL_PG_URL}",
    f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_DRIVER={LAKESOUL_PG_DRIVER}",
    f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_USERNAME={LAKESOUL_PG_USERNAME}",
    f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_PASSWORD={LAKESOUL_PG_PASSWORD}",
    f"-Dcontainerized.taskmanager.env.LAKESOUL_PG_URL={LAKESOUL_PG_URL}",
]


gmap = defaultdict(str)  # global map
j2env = Environment(loader=FileSystemLoader("templates"))
