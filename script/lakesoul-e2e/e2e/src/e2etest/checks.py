# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""
check e2e test environment settings mainly for k8s . Ex: postgresql, minio and compute engines.
"""

import logging
import subprocess
import psycopg
from e2etest.vars import (
    SECRET_KEY,
    ACCESS_KEY_ID,
    END_POINT,
    LAKESOUL_PG_PASSWORD,
    LAKESOUL_PG_URL,
    LAKESOUL_PG_USERNAME,
)

from e2etest.s3 import s3_list_prefix_dir


def check_pg():
    try:
        if (
            LAKESOUL_PG_URL is None
            or LAKESOUL_PG_USERNAME is None
            or LAKESOUL_PG_PASSWORD is None
        ):
            raise ValueError(
                "some of env variables [`LAKESOUL_PG_URL`,`LAKESOUL_PG_USERNAME`,`LAKESOUL_PG_PASSWORD`] are not set"
            )
        pg_url = LAKESOUL_PG_URL[5 : LAKESOUL_PG_URL.find("?")]
        conn = psycopg.connect(
            conninfo=pg_url, user=LAKESOUL_PG_USERNAME, password=LAKESOUL_PG_PASSWORD
        )
        conn.close()
        logging.info("check pg success")
    except Exception as e:
        logging.error(f"connect pg failed by {e}")


def check_s3():
    if END_POINT is None or SECRET_KEY is None or ACCESS_KEY_ID is None:
        raise ValueError(
            "some of env variables [`AWS_ENDPOINT`,`AWS_SECRET_ACCESS_KEY`,`AWS_ACCESS_KEY_ID`] are not set"
        )

    try:
        s3_list_prefix_dir("")
        logging.info("check s3 service success")
    except KeyError:
        logging.warning("S3 服务异常")


def check_clients():
    try:
        subprocess.run(
            ["flink", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            ["spark-submit", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logging.info("check clients success")
    except Exception as e:
        logging.error(f"check clients failed by {e}")
