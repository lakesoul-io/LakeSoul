# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


from e2etest.s3 import (
    s3_delete_dir,
    s3_delete_jars,
    s3_read,
    s3_upload_jars,
    s3_create_dir,
    s3_list_prefix_dir,
)
from e2etest.vars import E2E_CLASSPATH


def test_s3():
    s3_upload_jars()
    s3_delete_jars()
    s3_delete_dir(E2E_CLASSPATH)


def test_list():
    paths = s3_list_prefix_dir("lakesoul/lakesoul-e2e/spark/completed-jobs")
    print(paths)


def test_create():
    s3_create_dir("lakesoul/lakesoul-e2e/test_s3/")


def test_delete_dir():
    s3_delete_dir(E2E_CLASSPATH)


def test_read():
    content = s3_read(
        "lakesoul/lakesoul-e2e/spark/completed-jobs/spark-3d8703c5f022419e931dc8b50045006b"
    )
    print(content)
