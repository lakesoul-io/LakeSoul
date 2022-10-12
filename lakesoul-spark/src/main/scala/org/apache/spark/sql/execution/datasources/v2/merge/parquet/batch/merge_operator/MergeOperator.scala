/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, ScalaReflection}
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.lakesoul.LakeSoulUtils
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

trait MergeOperator[T] extends Serializable {

  def mergeData(input: Seq[T]): T

  def register(spark: SparkSession, name: String): Unit = {
    val udf = getUdf(name)
    val funIdentName = FunctionIdentifier(name)
    val info = new ExpressionInfo(
      this.getClass.getCanonicalName, funIdentName.database.orNull, funIdentName.funcName)

    def builder(children: Seq[Expression]) = udf.apply(children.map(Column.apply): _*).expr

    spark.sessionState.functionRegistry.registerFunction(funIdentName, info, builder)
  }

  private def getUdf(name: String): SparkUserDefinedFunction = {
    val f = (data: String) => data
    val ScalaReflection.Schema(dataType, nullable) = ScalaReflection.schemaFor[String]
    val inputEncoders = Try(ExpressionEncoder[String]()).toOption :: Nil
    val udf = SparkUserDefinedFunction(f, dataType, inputEncoders, None, Option(s"${LakeSoulUtils.MERGE_OP}$name"))
    if (nullable) udf else udf.asNonNullable()
  }
}

class DefaultMergeOp[T] extends MergeOperator[T] {
  override def mergeData(input: Seq[T]): T = {
    input.last
  }
}

class MergeOpInt extends MergeOperator[Int] {
  override def mergeData(input: Seq[Int]): Int = {
    input.sum
  }
}

class MergeNonNullOp[T] extends MergeOperator[T] {
  override def mergeData(input: Seq[T]): T = {
    val output=input.filter(_!=null)
    output.filter(!_.equals("null")).last
  }
}

class MergeOpString extends MergeOperator[String] {
  override def mergeData(input: Seq[String]): String = {
    input.mkString(",")
  }
}

class MergeOpLong extends MergeOperator[Long] {
  override def mergeData(input: Seq[Long]): Long = {
    input.sum
  }
}
