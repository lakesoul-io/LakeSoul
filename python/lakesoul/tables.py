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

from pyspark.sql import Column, DataFrame, functions


class LakeSoulTable(object):
    """
        Main class for programmatically interacting with LakeSoul tables.
        You can create LakeSoulTable instances using the path of the LakeSoul table.::
            starTable = LakeSoulTable.forPath(spark, "/path/to/table")
    """

    def __init__(self, spark, jst):
        self._spark = spark
        self._jst = jst

    def toDF(self):
        """
        Get a DataFrame representation of this LakeSoul table.
        """
        return DataFrame(self._jst.toDF(), self._spark._wrapped)

    def alias(self, aliasName):
        """
        Apply an alias to the LakeSoul table.
        """
        jst = self._jst.alias(aliasName)
        return LakeSoulTable(self._spark, jst)

    def delete(self, condition=None):
        """
        Delete data from the table that match the given ``condition``.
        Example::
            starTable.delete("date < '2017-01-01'")        # predicate using SQL formatted string
            starTable.delete(col("date") < "2017-01-01")   # predicate using Spark SQL functions
        :param condition: condition of the update
        :type condition: str or pyspark.sql.Column
        """
        if condition is None:
            self._jst.delete()
        else:
            self._jst.delete(self._condition_to_jcolumn(condition))

    def update(self, condition=None, set=None):
        """
        Update data from the table on the rows that match the given ``condition``,
        which performs the rules defined by ``set``.
        Example::
            # condition using SQL formatted string
            starTable.update(
                condition = "eventType = 'clck'",
                set = { "eventType": "'click'" } )
            # condition using Spark SQL functions
            starTable.update(
                condition = col("eventType") == "clck",
                set = { "eventType": lit("click") } )
        :param condition: Optional condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: Defines the rules of setting the values of columns that need to be updated.
                    *Note: This param is required.* Default value None is present to allow
                    positional args in same order across languages.
        :type set: dict with str as keys and str or pyspark.sql.Column as values
        """
        jmap = self._dict_to_jmap(self._spark, set, "'set'")
        jcolumn = self._condition_to_jcolumn(condition)
        if condition is None:
            self._jst.update(jmap)
        else:
            self._jst.update(jcolumn, jmap)

    def cleanup(self, justList=None):
        """
        Cleanup meta data and files data for the table.
        Example::
            starTable.cleanup(justList = false)
        :param justList: Just list for test, or really cleanup useless data
        :type justList: bool
        """
        if justList is None:
            self._jst.cleanup(False)
        elif type(justList) is bool:
            self._jst.cleanup(justList)
        else:
            e = "param justList must be a Boolean value," \
                + "found to be of type %s" % str(type(justList))
            raise TypeError(e)

    def upsert(self, source, condition=None):
        """
        Upsert data to the table on which rows that match the given ``condition``.
        Example::
            starTable.upsert(
                source = DataFrame(...),
                condition = "date='2017-01-01'")
        :param source: DataFrame that need to upsert
        :type source: pyspark.sql.DataFrame
        :param condition: Optional condition of upsert
        :type condition: str or pyspark.sql.Column
        """
        jcolumn = self._condition_to_jcolumn(condition)
        if condition is None:
            self._jst.upsert(source)
        else:
            self._jst.upsert(source, jcolumn)

    def compaction(self, condition=None, force=None, mergeOperatorInfo=None):
        """
        Compaction delta files for the table.
        Example::
            starTable.compaction(
                condition = "date='2017-01-01'",
                force = True,
                mergeOperatorInfo = { "value": "org.apache.spark.sql.lakesoul.test.MergeOpString"} )
        :param condition: Option condition of compaction, it will compact all partitions by default
        :type condition: str
        :param force: If true, compaction will execute ignore delta file num, compaction interval,
                      *and base file(first write)
        :type force: bool
        :param mergeOperatorInfo: Merge operator class info
        :type mergeOperatorInfo: dict with str as keys and str as values
        """
        if condition is None:
            jcondition = ""
        elif type(condition) is str:
            jcondition = condition
        else:
            e = "param condition must be a String value," \
                + "found to be of type %s" % str(type(condition))
            raise TypeError(e)

        if force is None:
            jforce = True
        elif type(force) is bool:
            jforce = force
        else:
            e = "param force must be a Boolean value," \
                + "found to be of type %s" % str(type(force))
            raise TypeError(e)

        jmo = self._merge_operator_to_jmo(self._spark, mergeOperatorInfo)
        self._jst.compaction(jcondition, jforce, jmo)

    def dropPartition(self, condition):
        if type(condition) is str:
            self._jst.dropPartition(condition)
        else:
            e = "param condition must be a String value," \
                + "found to be of type %s" % str(type(condition))
            raise TypeError(e)

    def dropTable(self):
        self._jst.dropTable()

    @classmethod
    def forPath(cls, sparkSession, path):
        """
        Create a LakeSoulTable for the data at the given `path` using the given SparkSession.
        :param sparkSession: SparkSession to use for loading the table
        :type sparkSession: pyspark.sql.SparkSession
        :return: loaded LakeSoul table
        :rtype: :py:class:`~lakesoul.tables.LakeSoulTable`
        Example::
            starTable = LakeSoulTable.forPath(spark, "/path/to/table")
        """
        assert sparkSession is not None
        jst = sparkSession._sc._jvm.LakeSoulTable.forPath(
            sparkSession._jsparkSession, path)
        return LakeSoulTable(sparkSession, jst)

    @classmethod
    def forName(cls, sparkSession, tableOrViewName):
        """
        Create a LakeSoulTable using the given table or view name using the given SparkSession.

        :param sparkSession: SparkSession to use for loading the table
        :param tableOrViewName: name of the table or view
        :return: loaded LakeSoul table
        :rtype: :py:class:`~lakesoul.tables.LakeSoulTable`

        Example::

            starTable = LakeSoulTable.forName(spark, "tblName")
        """
        assert sparkSession is not None
        jdt = sparkSession._sc._jvm.LakeSoulTable.forName(
            sparkSession._jsparkSession, tableOrViewName)
        return LakeSoulTable(sparkSession, jdt)

    @classmethod
    def isLakeSoulTable(cls, sparkSession, identifier):
        """
        Check if the provided `identifier` string, in this case a file path,
        is the root of a LakeSoul table using the given SparkSession.
        :param sparkSession: SparkSession to use to perform the check
        :param path: location of the table
        :return: If the table is a lakesoul table or not
        :rtype: bool
        Example::
            LakeSoulTable.isLakeSoulTable(spark, "/path/to/table")
        """
        assert sparkSession is not None
        return sparkSession._sc._jvm.LakeSoulTable.isLakeSoulTable(
            identifier)

    @classmethod
    def _dict_to_jmap(cls, sparkSession, pydict, argname):
        """
        convert dict<str, pColumn/str> to Map<str, jColumn>
        """
        # Get the Java map for pydict
        if pydict is None:
            raise ValueError("%s cannot be None" % argname)
        elif type(pydict) is not dict:
            e = "%s must be a dict, found to be %s" % (argname, str(type(pydict)))
            raise TypeError(e)

        jmap = sparkSession._sc._jvm.java.util.HashMap()
        for col, expr in pydict.items():
            if type(col) is not str:
                e = ("Keys of dict in %s must contain only strings with column names" % argname) + \
                    (", found '%s' of type '%s" % (str(col), str(type(col))))
                raise TypeError(e)
            if type(expr) is Column:
                jmap.put(col, expr._jc)
            elif type(expr) is str:
                jmap.put(col, functions.expr(expr)._jc)
            else:
                e = ("Values of dict in %s must contain only Spark SQL Columns " % argname) + \
                    "or strings (expressions in SQL syntax) as values, " + \
                    ("found '%s' of type '%s'" % (str(expr), str(type(expr))))
                raise TypeError(e)
        return jmap

    @classmethod
    def _condition_to_jcolumn(cls, condition, argname="'condition'"):
        if condition is None:
            jcondition = None
        elif type(condition) is Column:
            jcondition = condition._jc
        elif type(condition) is str:
            jcondition = functions.expr(condition)._jc
        else:
            e = ("%s must be a Spark SQL Column or a string (expression in SQL syntax)" % argname) \
                + ", found to be of type %s" % str(type(condition))
            raise TypeError(e)
        return jcondition

    @classmethod
    def _merge_operator_to_jmo(cls, sparkSession, merge_operator):
        """
        convert dict<str, str> to Map<str, str>
        """
        # Get the Java map for merge_operator
        jmo = sparkSession._sc._jvm.java.util.HashMap()
        if merge_operator is None:
            return jmo
        elif type(merge_operator) is not dict:
            e = "mergeOperator must be a dict, found to be %s" % (str(type(merge_operator)))
            raise TypeError(e)

        for col, mop in merge_operator.items():
            if type(col) is not str:
                e = "Keys of dict in mergeOperator must contain only strings with column names" + \
                    (", found '%s' of type '%s" % (str(col), str(type(col))))
                raise TypeError(e)
            if type(mop) is not str:
                e = "Values of dict in mergeOperator must contain only strings with mergeOp class names" + \
                    (", found '%s' of type '%s" % (str(mop), str(type(mop))))
                raise TypeError(e)
            jmo.put(col, mop)

        return jmo

    @classmethod
    def registerMergeOperator(cls, sparkSession, class_name, fun_name):
        return sparkSession._sc._jvm.LakeSoulTable \
            .registerMergeOperator(sparkSession._jsparkSession, class_name, fun_name)
