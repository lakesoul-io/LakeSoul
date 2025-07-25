from e2etest.spark import check_log, spark_check_executor
from e2etest.s3 import s3_list_prefix_dir
from e2etest.vars import SPARK_COMPLETED_JOB_DIR


def test_spark_logs_local():
    with open("spark-example.log", "r") as f:
        content = f.read()
        check_log(content)


def test_spark_logs_s3():
    paths = s3_list_prefix_dir(SPARK_COMPLETED_JOB_DIR)
    spark_check_executor(set(paths))
