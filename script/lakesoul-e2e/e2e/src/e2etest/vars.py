# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""
Global Variable definition
"""

import os
from pathlib import Path
import opendal

VERSION = os.getenv("LAKESOUL_VERSION", None)
LAKESOUL_GIT = "https://github.com/lakesoul-io/LakeSoul.git"
TMP_CODE_DIR = Path(os.path.expanduser("~/code"))
CONFIG_FILE = "config.yaml"
MVN_LOCAL = Path("~/.m2/repository/com/dmetasoul")

FLINK_VERSION = "1.20"
SPARK_VERSION = "3.3"

# lakesoul-flink
LAKESOUL_FLINK_PATH = Path(
    os.path.expanduser(
        f"{MVN_LOCAL}/lakesoul-flink/{FLINK_VERSION}-{VERSION}/lakesoul-flink-{FLINK_VERSION}-{VERSION}.jar"
    )
)
# lakesoul-spark
LAKESOUL_SPARK_PATH = Path(
    os.path.expanduser(
        f"{MVN_LOCAL}/lakesoul-spark/{SPARK_VERSION}-{VERSION}/lakesoul-spark-{SPARK_VERSION}-{VERSION}.jar"
    )
)

LAKESOUL_FLINK_E2E_PATH = Path(
    os.path.expanduser(f"{MVN_LOCAL}/flink-e2e/{VERSION}/flink-e2e-{VERSION}.jar")
)

LAKESOUL_SPARK_E2E_PATH = Path(
    os.path.expanduser(f"{MVN_LOCAL}/spark-e2e/{VERSION}/spark-e2e-{VERSION}.jar")
)

# lakesoul-pg
LAKESOUL_PG_URL = os.getenv("LAKESOUL_PG_URL", None)
LAKESOUL_PG_DRIVER = os.getenv("LAKESOUL_PG_DRIVER", None)
LAKESOUL_PG_USERNAME = os.getenv("LAKESOUL_PG_USERNAME", None)
LAKESOUL_PG_PASSWORD = os.getenv("LAKESOUL_PG_PASSWORD", None)

# s3
END_POINT = os.getenv("AWS_ENDPOINT", None)
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", None)
BUCKET = os.getenv("AWS_BUCKET", None)
S3_OPERATOR = opendal.Operator(
    "s3",
    bucket=BUCKET,
    region="auto",
    access_key_id=ACCESS_KEY_ID,
    secret_access_key=SECRET_KEY,
    endpoint=END_POINT,
)

E2E_DATA_DIR = "lakesoul/lakesoul-e2e/data"
E2E_CLASSPATH = "lakesoul/lakesoul-e2e/target"
E2E_CLASSPATH_WITH_ENDPOINT = f"{END_POINT}/{BUCKET}/{E2E_CLASSPATH}"

FLINK_COMPLETED_JOB_DIR = "lakesoul/lakesoul-e2e/flink/completed-jobs"
SPARK_COMPLETED_JOB_DIR = "lakesoul/lakesoul-e2e/spark/completed-jobs"
