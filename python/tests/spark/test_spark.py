# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import glob
import os
from pathlib import Path
import random
import string
import tempfile
import shutil
import sys
import threading
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr

from lakesoul.spark import LakeSoulTable
import pytest


def __writeLakeSoulTable(spark, temp_file, datalist):
    df = spark.createDataFrame(datalist, ["key", "value"])
    df.write.format("lakesoul").save(temp_file)


def __writeAsTable(spark, datalist, tblName):
    df = spark.createDataFrame(datalist, ["key", "value"])
    df.write.format("lakesoul").saveAsTable(tblName)


def __overwriteLakeSoulTable(spark, datalist, tmpfile):
    df = spark.createDataFrame(datalist, ["key", "value"])
    df.write.format("lakesoul").mode("overwrite").save(tmpfile)


def __overwriteHashLakeSoulTable(spark, temp_file, datalist):
    df = spark.createDataFrame(datalist, ["key", "value"])
    df.write.format("lakesoul").mode("overwrite").option(
        "hashPartitions", "key"
    ).option("hashBucketNum", "2").save(temp_file)


def __createFile(temp_file, fileName, content):
    with open(os.path.join(temp_file, fileName), "w") as f:
        f.write(content)


def __checkFileExists(temp_file, fileName):
    return os.path.exists(os.path.join(temp_file, fileName))


def __checkAnswer(spark, df, expectedAnswer, schema=["key", "value"]):
    if not expectedAnswer:
        assert df.count() == 0
        return
    expectedDF = spark.createDataFrame(expectedAnswer, schema)
    try:
        assert df.count() == expectedDF.count()
        assert len(df.columns) == len(expectedDF.columns)
        assert [] == df.subtract(expectedDF).take(1)
        assert [] == expectedDF.subtract(df).take(1)
    except AssertionError:
        print("Expected:")
        expectedDF.show()
        print("Found:")
        df.show()
        raise


def __generate_random_string(length):
    characters = string.ascii_lowercase + string.digits
    return "".join(random.choice(characters) for _ in range(length))


@pytest.fixture
def temp_path():
    path = tempfile.mktemp()
    return path


@pytest.fixture
def spark_and_file(temp_path):
    """Fixture to set up a SparkSession for querying LakeSoul tables."""
    old_sys_path = list(sys.path)
    warehouse_dir = Path(temp_path) / "warehouse"
    warehouse_dir.mkdir(parents=True)
    lakesoul_source_dir = os.environ.get("LAKESOUL_SOURCE_DIR")
    jar_pattern = os.path.join(
        lakesoul_source_dir,  # type:ignore
        "lakesoul-spark",
        "target",
        "lakesoul-spark*.jar",
    )
    jar_files = glob.glob(jar_pattern)
    spark = (
        SparkSession.builder.appName("PySparkLakeSoulTest")
        .master("local[4]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "5")
        .config(
            "spark.sql.extensions",
            "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.jars", ",".join(jar_files))
        .getOrCreate()
    )
    temp_file = os.path.join(temp_path, "temp_file")
    yield spark, temp_file
    spark.sparkContext.stop()
    if warehouse_dir.exists() and warehouse_dir.is_dir():
        shutil.rmtree(warehouse_dir)
    sys.path = old_sys_path


# def test_forPath(spark_and_file):
#     spark, temp_file = spark_and_file
#     __writeLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3)])
#     table = LakeSoulTable.forPath(spark, temp_file).toDF()
#     __checkAnswer(spark, table, [("a", 1), ("b", 2), ("c", 3)])


# def test_forName(spark_and_file):
#     spark, _ = spark_and_file
#     name = __generate_random_string(8)
#     __writeAsTable(spark, [("a", 1), ("b", 2), ("c", 3)], name)
#     df = LakeSoulTable.forName(spark, name).toDF()
#     __checkAnswer(spark, df, [("a", 1), ("b", 2), ("c", 3)])


# def test_alias_and_toDF(spark_and_file):
#     spark, temp_file = spark_and_file
#     __writeLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3)])
#     table = LakeSoulTable.forPath(spark, temp_file).toDF()
#     __checkAnswer(
#         spark,
#         table.alias("myTable").select("myTable.key", "myTable.value"),
#         [("a", 1), ("b", 2), ("c", 3)],
#     )


# def test_delete(spark_and_file):
#     spark, temp_file = spark_and_file
#     __writeLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)])
#     table = LakeSoulTable.forPath(spark, temp_file)

#     # delete with condition as str
#     table.delete("key = 'a'")
#     __checkAnswer(spark, table.toDF(), [("b", 2), ("c", 3), ("d", 4)])

#     # delete with condition as Column
#     table.delete(col("key") == lit("b"))
#     __checkAnswer(spark, table.toDF(), [("c", 3), ("d", 4)])

#     # delete without condition
#     table.delete()
#     __checkAnswer(spark, table.toDF(), [])

#     # bad args
#     with pytest.raises(TypeError):
#         table.delete(condition=1)


# def test_update(spark_and_file):
#     spark, temp_file = spark_and_file
#     __writeLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)])
#     table = LakeSoulTable.forPath(spark, temp_file)

#     # update with condition as str and with set exprs as str
#     table.update("key = 'a' or key = 'b'", {"value": "1"})
#     __checkAnswer(spark, table.toDF(), [("a", 1), ("b", 1), ("c", 3), ("d", 4)])

#     # update with condition as Column and with set exprs as Columns
#     table.update(expr("key = 'a' or key = 'b'"), {"value": expr("0")})
#     __checkAnswer(spark, table.toDF(), [("a", 0), ("b", 0), ("c", 3), ("d", 4)])

#     # update without condition
#     table.update(set={"value": "200"})
#     __checkAnswer(spark, table.toDF(), [("a", 200), ("b", 200), ("c", 200), ("d", 200)])

#     # bad args
#     with pytest.raises(ValueError, match="cannot be None"):
#         table.update({"value": "200"})

#     with pytest.raises(ValueError, match="cannot be None"):
#         table.update(condition="a")

#     with pytest.raises(TypeError, match="must be a dict"):
#         table.update(set=1)

#     with pytest.raises(TypeError, match="must be a Spark SQL Column or a string"):
#         table.update(1, {})

#     with pytest.raises(TypeError, match="Values of dict in .* must contain only"):
#         table.update(set={"value": 1})

#     with pytest.raises(TypeError, match="Keys of dict in .* must contain only"):
#         table.update(set={1: ""})

#     with pytest.raises(TypeError):
#         table.update(set=1)


# def test_upsert(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(
#         spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
#     )
#     source = spark.createDataFrame(
#         [("a", -1), ("b", 0), ("e", -5), ("f", -6)], ["key", "value"]
#     )

#     table = LakeSoulTable.forPath(spark, temp_file)
#     table.upsert(source)
#     __checkAnswer(
#         spark,
#         table.toDF(),
#         ([("a", -1), ("b", 0), ("c", 3), ("d", 4), ("e", -5), ("f", -6)]),
#     )


# TODO: this failed
# def test_cleanup(spark_and_file):
#     spark, temp_file = spark_and_file
#     __writeLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3)])
#     table = LakeSoulTable.forPath(spark, temp_file)
#     __createFile(temp_file, "abc.txt", "abcde")
#     __createFile(temp_file, "bac.txt", "abcdf")
#     assert __checkFileExists(temp_file, "abc.txt")
#     table.cleanup()  # will not delete files as default retention is used.

#     assert __checkFileExists(temp_file, "bac.txt")
#     retentionConf = "spark.dmetasoul.lakesoul.cleanup.interval"
#     spark.conf.set(retentionConf, "0")
#     table.cleanup()
#     assert not __checkFileExists(temp_file, "bac.txt")
#     assert not __checkFileExists(temp_file, "abc.txt")


# def test_is_lakesoul_table(spark_and_file):
#     spark, temp_file = spark_and_file
#     df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["key", "value"])
#     df.write.format("parquet").save(temp_file)
#     temp_file2 = temp_file + "_2"
#     df.write.format("lakesoul").save(temp_file2)
#     assert not LakeSoulTable.isLakeSoulTable(spark, temp_file)
#     assert LakeSoulTable.isLakeSoulTable(spark, temp_file2)


# def test_drop_partition(spark_and_file):
#     spark, temp_file = spark_and_file
#     df = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["key", "value"])
#     df.write.format("lakesoul").option("rangePartitions", "key").save(temp_file)
#     LakeSoulTable.forPath(spark, temp_file).dropPartition("key='a'")
#     __checkAnswer(
#         spark,
#         LakeSoulTable.forPath(spark, temp_file).toDF().select("key", "value"),
#         ([("b", 2), ("c", 3)]),
#     )


# TODO: this failed
# def test_drop_table(spark_and_file):
#     spark, temp_file = spark_and_file
#     __writeLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3)])
#     LakeSoulTable.forPath(spark, temp_file).dropTable()
#     assert not LakeSoulTable.isLakeSoulTable(spark, temp_file)


# FIXME: this failed
# def test_compaction(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(
#         spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
#     )
#     table = LakeSoulTable.forPath(spark, temp_file)
#     df = spark.createDataFrame(
#         [("a", 11), ("b", 22), ("e", 55), ("f", 66)], ["key", "value"]
#     )
#     table.upsert(df._jdf)
#     table.compaction()
#     __checkAnswer(
#         spark,
#         table.toDF().select("key", "value"),
#         ([("a", 11), ("b", 22), ("c", 3), ("d", 4), ("e", 55), ("f", 66)]),
#     )


# FIXME:this failed
# def test_compaction_with_merge_operator(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(
#         spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
#     )
#     table = LakeSoulTable.forPath(spark, temp_file)
#     df = spark.createDataFrame(
#         [("a", 11), ("b", 22), ("e", 55), ("f", 66)], ["key", "value"]
#     )
#     table.upsert(df._jdf)
#     merge_info = {"value": "org.apache.spark.sql.lakesoul.MergeOpLong"}
#     table.compaction(mergeOperatorInfo=merge_info)
#     __checkAnswer(
#         spark,
#         table.toDF().select("key", "value"),
#         ([("a", 12), ("b", 24), ("c", 3), ("d", 4), ("e", 55), ("f", 66)]),
#     )


# FIXME: this failed
# def test_read_with_merge_operator(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(
#         spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
#     )
#     table = LakeSoulTable.forPath(spark, temp_file)
#     df = spark.createDataFrame(
#         [("a", 11), ("b", 22), ("e", 55), ("f", 66)], ["key", "value"]
#     )
#     table.upsert(df._jdf)

#     LakeSoulTable.registerMergeOperator(
#         spark, "org.apache.spark.sql.lakesoul.MergeOpLong", "long_op"
#     )
#     re = table.toDF().withColumn("value", expr("long_op(value)"))
#     __checkAnswer(
#         spark,
#         re.select("key", "value"),
#         ([("a", 12), ("b", 24), ("c", 3), ("d", 4), ("e", 55), ("f", 66)]),
#     )


# FIXME: this failed
# def test_snapshot_query(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(
#         spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
#     )
#     table = LakeSoulTable.forPath(spark, temp_file)
#     readEndTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     time.sleep(2)
#     df = spark.createDataFrame([("e", 55), ("f", 66)], ["key", "value"])
#     table.upsert(df)
#     lake1 = LakeSoulTable.forPathSnapshot(
#         spark, temp_file, "", readEndTime, "Asia/Shanghai"
#     )
#     lake2 = (
#         spark.read.format("lakesoul")
#         .option("readendtime", readEndTime)
#         .option("timezone", "Asia/Shanghai")
#         .option("readtype", "snapshot")
#         .load(temp_file)
#         .show()
#     )
#     __checkAnswer(spark, lake1.toDF(), [("a", 1), ("b", 2), ("c", 3), ("d", 4)])
#     __checkAnswer(spark, lake2, [("a", 1), ("b", 2), ("c", 3), ("d", 4)])


# def test_incremental_query(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(
#         spark, temp_file, [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
#     )
#     table = LakeSoulTable.forPath(spark, temp_file)
#     readStartTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     time.sleep(2)
#     df = spark.createDataFrame([("e", 55), ("f", 66)], ["key", "value"])
#     table.upsert(df)
#     time.sleep(2)
#     df = spark.createDataFrame([("g", 77)], ["key", "value"])
#     table.upsert(df)
#     time.sleep(1)
#     readEndTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     lake1 = LakeSoulTable.forPathIncremental(
#         spark, temp_file, "", readStartTime, readEndTime, "Asia/Shanghai"
#     )
#     lake2 = (
#         spark.read.format("lakesoul")
#         .option("readstarttime", readStartTime)
#         .option("readendtime", readEndTime)
#         .option("timezone", "Asia/Shanghai")
#         .option("readtype", "incremental")
#         .load(temp_file)
#     )
#     __checkAnswer(spark, lake1.toDF(), [("e", 55), ("f", 66), ("g", 77)])
#     __checkAnswer(spark, lake2, [("e", 55), ("f", 66), ("g", 77)])


# FIXME: figure it out and
# hang on
# def test_streaming_incremental_query(spark_and_file):
#     spark, temp_file = spark_and_file
#     __overwriteHashLakeSoulTable(spark, temp_file, [("a", 1), ("b", 2), ("c", 3)])
#     table = LakeSoulTable.forPath(spark, temp_file)
#     readStartTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     time.sleep(2)
#     threading.Thread(
#         spark.readStream.format("lakesoul")
#         .option("readstarttime", readStartTime)
#         .option("readtype", "incremental")
#         .load(temp_file)
#         .writeStream.format("console")
#         .trigger(processingTime="2 seconds")
#         .start()
#         .awaitTermination()  # type: ignore
#     )
#     df = spark.createDataFrame([("d", 4), ("e", 55)], ["key", "value"])
#     table.upsert(df)
#     time.sleep(2)
#     df = spark.createDataFrame([("f", 66), ("g", 77)], ["key", "value"])
#     table.upsert(df)
