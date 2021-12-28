#
# Copyright [2022] [DMetaSoul Team]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import unittest

from pyspark.sql.functions import col, lit, expr

from lakesoul.tables import LakeSoulTable
from lakesoul.testing.utils import LakeSoulTestCase


class LakeSoulTableTests(LakeSoulTestCase):

    def test_forPath(self):
        self.__writeLakeSoulTable([('a', 1), ('b', 2), ('c', 3)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(table, [('a', 1), ('b', 2), ('c', 3)])

    def test_forName(self):
        self.__writeAsTable([('a', 1), ('b', 2), ('c', 3)], "test")
        df = LakeSoulTable.forName(self.spark, "test").toDF()
        self.__checkAnswer(df, [('a', 1), ('b', 2), ('c', 3)])

    def test_alias_and_toDF(self):
        self.__writeLakeSoulTable([('a', 1), ('b', 2), ('c', 3)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile).toDF()
        self.__checkAnswer(
            table.alias("myTable").select('myTable.key', 'myTable.value'),
            [('a', 1), ('b', 2), ('c', 3)])

    def test_delete(self):
        self.__writeLakeSoulTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile)

        # delete with condition as str
        table.delete("key = 'a'")
        self.__checkAnswer(table.toDF(), [('b', 2), ('c', 3), ('d', 4)])

        # delete with condition as Column
        table.delete(col("key") == lit("b"))
        self.__checkAnswer(table.toDF(), [('c', 3), ('d', 4)])

        # delete without condition
        table.delete()
        self.__checkAnswer(table.toDF(), [])

        # bad args
        with self.assertRaises(TypeError):
            table.delete(condition=1)

    def test_update(self):
        self.__writeLakeSoulTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile)

        # update with condition as str and with set exprs as str
        table.update("key = 'a' or key = 'b'", {"value": "1"})
        self.__checkAnswer(table.toDF(), [('a', 1), ('b', 1), ('c', 3), ('d', 4)])

        # update with condition as Column and with set exprs as Columns
        table.update(expr("key = 'a' or key = 'b'"), {"value": expr("0")})
        self.__checkAnswer(table.toDF(), [('a', 0), ('b', 0), ('c', 3), ('d', 4)])

        # update without condition
        table.update(set={"value": "200"})
        self.__checkAnswer(table.toDF(), [('a', 200), ('b', 200), ('c', 200), ('d', 200)])

        # bad args
        with self.assertRaisesRegex(ValueError, "cannot be None"):
            table.update({"value": "200"})

        with self.assertRaisesRegex(ValueError, "cannot be None"):
            table.update(condition='a')

        with self.assertRaisesRegex(TypeError, "must be a dict"):
            table.update(set=1)

        with self.assertRaisesRegex(TypeError, "must be a Spark SQL Column or a string"):
            table.update(1, {})

        with self.assertRaisesRegex(TypeError, "Values of dict in .* must contain only"):
            table.update(set={"value": 1})

        with self.assertRaisesRegex(TypeError, "Keys of dict in .* must contain only"):
            table.update(set={1: ""})

        with self.assertRaises(TypeError):
            table.update(set=1)

    def test_upsert(self):
        self.__overwriteHashLakeSoulTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        source = self.spark.createDataFrame([('a', -1), ('b', 0), ('e', -5), ('f', -6)], ["key", "value"])

        table = LakeSoulTable.forPath(self.spark, self.tempFile)
        table.upsert(source._jdf)
        self.__checkAnswer(table.toDF(),
                           ([('a', -1), ('b', 0), ('c', 3), ('d', 4), ('e', -5), ('f', -6)]))

    def test_cleanup(self):
        self.__writeLakeSoulTable([('a', 1), ('b', 2), ('c', 3)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile)
        self.__createFile('abc.txt', 'abcde')
        self.__createFile('bac.txt', 'abcdf')
        self.assertEqual(True, self.__checkFileExists('abc.txt'))
        table.cleanup()  # will not delete files as default retention is used.

        self.assertEqual(True, self.__checkFileExists('bac.txt'))
        retentionConf = "spark.dmetasoul.lakesoul.cleanup.interval"
        self.spark.conf.set(retentionConf, "0")
        table.cleanup()
        self.assertEqual(False, self.__checkFileExists('bac.txt'))
        self.assertEqual(False, self.__checkFileExists('abc.txt'))

    def test_is_lakesoul_table(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("parquet").save(self.tempFile)
        tempFile2 = self.tempFile + '_2'
        df.write.format("lakesoul").save(tempFile2)
        self.assertEqual(LakeSoulTable.isLakeSoulTable(self.spark, self.tempFile), False)
        self.assertEqual(LakeSoulTable.isLakeSoulTable(self.spark, tempFile2), True)

    def test_drop_partition(self):
        df = self.spark.createDataFrame([('a', 1), ('b', 2), ('c', 3)], ["key", "value"])
        df.write.format("lakesoul").option("rangePartitions", "key").save(self.tempFile)
        LakeSoulTable.forPath(self.spark, self.tempFile).dropPartition("key='a'")
        self.__checkAnswer(LakeSoulTable.forPath(self.spark, self.tempFile).toDF().select("key", "value"),
                           ([('b', 2), ('c', 3)]))

    def test_drop_table(self):
        self.__writeLakeSoulTable([('a', 1), ('b', 2), ('c', 3)])
        LakeSoulTable.forPath(self.spark, self.tempFile).dropTable()
        self.assertEqual(LakeSoulTable.isLakeSoulTable(self.spark, self.tempFile), False)

    def test_compaction(self):
        self.__overwriteHashLakeSoulTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile)
        df = self.spark.createDataFrame([('a', 11), ('b', 22), ('e', 55), ('f', 66)], ["key", "value"])
        table.upsert(df._jdf)
        table.compaction()
        self.__checkAnswer(table.toDF().select("key", "value"),
                           ([('a', 11), ('b', 22), ('c', 3), ('d', 4), ('e', 55), ('f', 66)]))

    def test_compaction_with_merge_operator(self):
        self.__overwriteHashLakeSoulTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile)
        df = self.spark.createDataFrame([('a', 11), ('b', 22), ('e', 55), ('f', 66)], ["key", "value"])
        table.upsert(df._jdf)
        merge_info = {"value": "org.apache.spark.sql.lakesoul.MergeOpLong"}
        table.compaction(mergeOperatorInfo=merge_info)
        self.__checkAnswer(table.toDF().select("key", "value"),
                           ([('a', 12), ('b', 24), ('c', 3), ('d', 4), ('e', 55), ('f', 66)]))

    def test_read_with_merge_operator(self):
        self.__overwriteHashLakeSoulTable([('a', 1), ('b', 2), ('c', 3), ('d', 4)])
        table = LakeSoulTable.forPath(self.spark, self.tempFile)
        df = self.spark.createDataFrame([('a', 11), ('b', 22), ('e', 55), ('f', 66)], ["key", "value"])
        table.upsert(df._jdf)

        LakeSoulTable.registerMergeOperator(self.spark, "org.apache.spark.sql.lakesoul.MergeOpLong", "long_op")
        re = table.toDF().withColumn("value", expr("long_op(value)"))
        print(str(re.schema))
        print(re.explain(True))
        self.__checkAnswer(re.select("key", "value"),
                           ([('a', 12), ('b', 24), ('c', 3), ('d', 4), ('e', 55), ('f', 66)]))

    def __checkAnswer(self, df, expectedAnswer, schema=["key", "value"]):
        if not expectedAnswer:
            self.assertEqual(df.count(), 0)
            return
        expectedDF = self.spark.createDataFrame(expectedAnswer, schema)
        try:
            self.assertEqual(df.count(), expectedDF.count())
            self.assertEqual(len(df.columns), len(expectedDF.columns))
            self.assertEqual([], df.subtract(expectedDF).take(1))
            self.assertEqual([], expectedDF.subtract(df).take(1))
        except AssertionError:
            print("Expected:")
            expectedDF.show()
            print("Found:")
            df.show()
            raise

    def __writeLakeSoulTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("lakesoul").save(self.tempFile)

    def __writeAsTable(self, datalist, tblName):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("lakesoul").saveAsTable(tblName)

    def __overwriteLakeSoulTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("lakesoul").mode("overwrite").save(self.tempFile)

    def __overwriteHashLakeSoulTable(self, datalist):
        df = self.spark.createDataFrame(datalist, ["key", "value"])
        df.write.format("lakesoul") \
            .mode("overwrite") \
            .option("hashPartitions", "key") \
            .option("hashBucketNum", "2") \
            .save(self.tempFile)

    def __createFile(self, fileName, content):
        with open(os.path.join(self.tempFile, fileName), 'w') as f:
            f.write(content)

    def __checkFileExists(self, fileName):
        return os.path.exists(os.path.join(self.tempFile, fileName))


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
