// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, ScalaReflection}
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.lakesoul.LakeSoulUtils
import org.apache.spark.sql.{Column, SparkSession}

import scala.util.Try

trait MergeOperator[T] extends Serializable {

  def mergeData(input: Seq[T]): T

  def toNativeName(): String

  def register(spark: SparkSession, name: String): Unit = {
    val udf = getUdf(name)
    val funIdentName = FunctionIdentifier(name)
    val info = new ExpressionInfo(
      this.getClass.getCanonicalName, funIdentName.database.orNull, funIdentName.funcName)

    def builder(children: Seq[Expression]): Expression = udf.apply(children.map(Column.apply): _*).expr

    val builderFunc: FunctionBuilder = builder

    spark.sessionState.functionRegistry.registerFunction(funIdentName, info, builderFunc)
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

  override def toNativeName(): String = "UseLast"
}

class MergeOpInt extends MergeOperator[Int] {
  override def mergeData(input: Seq[Int]): Int = {
    input.sum
  }

  override def toNativeName(): String = "Sum"
}

class MergeNonNullOp[T] extends MergeOperator[T] {
  override def mergeData(input: Seq[T]): T = {
    val output = input.filter(_ != null)
    output.filter(!_.equals("null")).last
  }

  override def toNativeName(): String = "UseLast"
}

class MergeOpString extends MergeOperator[String] {
  override def mergeData(input: Seq[String]): String = {
    input.mkString(",")
  }

  override def toNativeName(): String = "JoinedByComma"
}

class MergeOpLong extends MergeOperator[Long] {
  override def mergeData(input: Seq[Long]): Long = {
    input.sum
  }

  override def toNativeName(): String = "Sum"
}
