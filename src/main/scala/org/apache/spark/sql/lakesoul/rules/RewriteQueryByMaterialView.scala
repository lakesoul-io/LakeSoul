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

package org.apache.spark.sql.lakesoul.rules

import com.dmetasoul.lakesoul.meta.{MaterialView, MetaVersion}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, EncodeUsingSerializer, GetExternalRowField}
import org.apache.spark.sql.catalyst.expressions.xml._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.aggregate.{ScalaAggregator, ScalaUDAF}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.lakesoul.LakeSoulUtils
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.material_view._
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class RewriteQueryByMaterialView(spark: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val canRewrite = if (spark.conf.contains(LakeSoulUtils.USE_MATERIAL_REWRITE)) {
      spark.conf.get(LakeSoulUtils.USE_MATERIAL_REWRITE).toBoolean
    } else {
      true
    }

    if (canRewrite &&
      spark.sessionState.conf.getConf(LakeSoulSQLConf.MATERIAL_QUERY_REWRITE_ENABLE)) {
      var newPlan: LogicalPlan = null
      try {
        val construct = new ConstructQueryInfo
        MaterialViewUtils.parseOutputInfo(plan, construct)
        MaterialViewUtils.parseMaterialInfo(plan, construct, false)
        val queryInfo = construct.buildQueryInfo()

        val queryRelateTables = queryInfo.tableInfo.values
          .map(m => {
            val split = m.split("\\.")
            assert(split(0).equals(LakeSoulSourceUtils.NAME))
            split(1).replace("`", "")
          })
          .toSet

        val allViews = queryRelateTables.map(tableName => {
          val table_id = MetaVersion.getTableInfo(tableName).table_id
          MaterialView.getMaterialRelationInfo(table_id).split(",")
        })


        //now we only use views which contains all the tables
        //todo: support use views which contains part of tables, and use them with compensation predicates
        val candidateViews = allViews.reduce((x, y) => {
          x.intersect(y)
        })

        val viewsCanBeUsed = candidateViews.map(view => {
          var canRewrite = true
          val materialViewInfo = MaterialView.getMaterialViewInfo(view)
          var equalRangeColumns = new ArrayBuffer[String]()

          if (materialViewInfo.isEmpty) {
            //view has been deleted very recently
            canRewrite = false
          } else {
            val viewInfo = materialViewInfo.get.info
            val viewOutputColumns = viewInfo.outputInfo.values.toSet

            //check output info matching
            if (canRewrite) {
              val outputDiff = queryInfo.outputInfo.values.toSet.diff(viewInfo.outputInfo.values.toSet)
              if (outputDiff.nonEmpty) {
                //query's output can be computed from view's output
                val allExist = outputDiff.forall(f => {
                  viewOutputColumns.exists(e => f.contains(e))
                })
                if (!allExist) {
                  canRewrite = false
                }
              }
            }

            //check aggregate info matching
            if (canRewrite) {
              if (!queryInfo.aggregateInfo.toString.equals(viewInfo.aggregateInfo.toString)) {
                canRewrite = false
              }
            }

            //check join info matching
            if (canRewrite) {
              if (!queryInfo.joinInfo.toString.equals(viewInfo.joinInfo.toString)) {
                canRewrite = false
              }
            }

            //check range info matching
            //1. range info of view should all exist in query
            if (canRewrite) {
              val queryRangeConditionCols = queryInfo.rangeInfo.keySet
              val viewRangeConditionCols = viewInfo.rangeInfo.keySet
              val rangeColDiff = viewRangeConditionCols.diff(queryRangeConditionCols)
              if (rangeColDiff.nonEmpty) {
                val iter = rangeColDiff.iterator
                while (canRewrite && iter.hasNext) {
                  val colName = iter.next()
                  //if query has an equal condition to limit this range columns, and the value is under view's range,
                  //the view can also be used to rewrite the query, such as
                  //view: key>1 and key<10
                  //query: key=5
                  if (queryInfo.conditionEqualInfo.contains(colName)) {
                    val value = queryInfo.conditionEqualInfo(colName)
                    if (!RangeInfo.valueInRange(value, viewInfo.rangeInfo(colName)) ||
                      !conditionColumnsExists(colName, viewOutputColumns, viewInfo.columnEqualInfo)) {
                      canRewrite = false
                    }
                  } else {
                    canRewrite = false
                  }
                }
              }
            }
            //2. range info of query should exist in view, or can be inferred from view's output
            if (canRewrite) {
              val iter = queryInfo.rangeInfo.iterator
              while (canRewrite && iter.hasNext) {
                val (colName, queryRange) = iter.next()

                if (viewInfo.rangeInfo.contains(colName)) {
                  //if material view has range limit on the same column, it should contains query range
                  val viewRange = viewInfo.rangeInfo(colName)
                  assert(queryRange.dataType == viewRange.dataType)

                  val compare = RangeInfo.compareRangeDetail(queryRange, viewRange)
                  if (compare < 0) {
                    canRewrite = false
                  } else if (compare == 0) {
                    equalRangeColumns += colName
                  } else {
                    //if range not matching, view's output should contains condition columns
                    if (!conditionColumnsExists(colName, viewOutputColumns, viewInfo.columnEqualInfo)) {
                      canRewrite = false
                    }
                  }
                } else if (!conditionColumnsExists(colName, viewOutputColumns, viewInfo.columnEqualInfo)) {
                  //the column which don't have range limit should exists
                  //(or it's equivalence column exists) in view's output
                  canRewrite = false
                }
              }

            }

            //check equal condition info matching
            if (canRewrite) {
              //equal condition in view should be a subset of query
              val diff = viewInfo.conditionEqualInfo.map(m => m._1 + "=" + m._2).toSeq
                .diff(queryInfo.conditionEqualInfo.map(m => m._1 + "=" + m._2).toSeq)
              if (diff.nonEmpty) {
                canRewrite = false
              }
            }

            //check or condition info matching
            if (canRewrite) {
              //if there is no orInfo in query
              val queryOrInfo =
                if (queryInfo.conditionOrInfo.isEmpty && viewInfo.conditionOrInfo.nonEmpty) {
                  Seq(OrDetail(
                    queryInfo.rangeInfo,
                    queryInfo.conditionEqualInfo,
                    queryInfo.conditionOrInfo,
                    queryInfo.otherInfo))
                } else {
                  queryInfo.conditionOrInfo
                }
              //or condition in query should be a subset of view
              val inbounds = OrInfo.inbounds(queryOrInfo, viewInfo.conditionOrInfo)
              if (!inbounds) {
                canRewrite = false
              }
            }


            //check other condition info matching
            if (canRewrite) {
              //other condition in view should be a subset of query
              val diff = viewInfo.otherInfo.diff(queryInfo.otherInfo)
              if (diff.nonEmpty) {
                canRewrite = false
              }
            }


          }


          (canRewrite, materialViewInfo, equalRangeColumns)
        }).filter(_._1)

        //if there have suitable views, use it rewrite plan
        if (viewsCanBeUsed.nonEmpty) {
          //now we use first view
          //todo: make some rules to choose the best view
          val chosenView = viewsCanBeUsed.head._2.get
          val equalRangeColumns = viewsCanBeUsed.head._3

          val (newTableName, newDataSource) = buildDataSourceV2Relation(chosenView.viewName)
          val outputRef = newDataSource.output.map(m => m.name -> m).toMap

          //find matching column from view output
          def matchViewOutputReference(name: String): (Boolean, Option[AttributeReference]) = {
            val formattedName = ConstructQueryInfo.getFinalStringByReplace(
              name,
              queryInfo.tableInfo,
              queryInfo.columnAsInfo)
            val findFromView = findColumnFromViewOutput(
              formattedName,
              chosenView.info.outputInfo,
              chosenView.info.columnEqualInfo)
            if (findFromView.isDefined) {
              val viewColumnName = findFromView.get._1.split("\\.").last
              (true, Some(outputRef(viewColumnName)))
            } else {
              (false, None)
            }
          }

          //recursive replace attribute
          def findNewAttributeReference(expression: Expression): Expression = {
            val (find, target) = matchViewOutputReference(expression.sql)
            if (find) {
              target.get
            } else {

              expression match {
                //leaf expression
                case lit: Literal => lit
                case attr: AttributeReference =>
                  matchViewOutputReference(attr.sql)._2.get
                case leaf: LeafExpression => leaf
                case as@Alias(child, name) =>
                  as.copy(child = findNewAttributeReference(child))(
                    exprId = as.exprId,
                    qualifier = as.qualifier,
                    explicitMetadata = as.explicitMetadata,
                    nonInheritableMetadataKeys = as.nonInheritableMetadataKeys)

                //unary expression
                case abs@Abs(child) => abs.copy(child = findNewAttributeReference(child))
                case e@Cast(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@AnsiCast(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@Acos(child) => e.copy(child = findNewAttributeReference(child))
                case e@Acosh(child) => e.copy(child = findNewAttributeReference(child))
                case e@ArrayDistinct(child) => e.copy(child = findNewAttributeReference(child))
                case e@ArrayMax(child) => e.copy(child = findNewAttributeReference(child))
                case e@ArrayMin(child) => e.copy(child = findNewAttributeReference(child))
                case e@Ascii(child) => e.copy(child = findNewAttributeReference(child))
                case e@Asin(child) => e.copy(child = findNewAttributeReference(child))
                case e@Asinh(child) => e.copy(child = findNewAttributeReference(child))
                case e@AssertNotNull(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Atan(child) => e.copy(child = findNewAttributeReference(child))
                case e@Atanh(child) => e.copy(child = findNewAttributeReference(child))
                case e@Base64(child) => e.copy(child = findNewAttributeReference(child))
                case e@Bin(child) => e.copy(child = findNewAttributeReference(child))
                case e@BitLength(child) => e.copy(child = findNewAttributeReference(child))
                case e@BitwiseCount(child) => e.copy(child = findNewAttributeReference(child))
                case e@BitwiseNot(child) => e.copy(child = findNewAttributeReference(child))
                case e@Cbrt(child) => e.copy(child = findNewAttributeReference(child))
                case e@Ceil(child) => e.copy(child = findNewAttributeReference(child))
                case e@CheckOverflow(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@CheckOverflowInSum(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@Chr(child) => e.copy(child = findNewAttributeReference(child))
                case e@Cos(child) => e.copy(child = findNewAttributeReference(child))
                case e@Cosh(child) => e.copy(child = findNewAttributeReference(child))
                case e@Cot(child) => e.copy(child = findNewAttributeReference(child))
                case e@Crc32(child) => e.copy(child = findNewAttributeReference(child))
                case e@CsvToStructs(_, _, child, _) => e.copy(child = findNewAttributeReference(child))
                case e@DateFromUnixDate(child) => e.copy(child = findNewAttributeReference(child))
                case e@DatePart(field, source, child) =>
                  e.copy(
                    field = findNewAttributeReference(field),
                    source = findNewAttributeReference(source),
                    child = findNewAttributeReference(child))
                case e@DatetimeSub(start, interval, child) =>
                  e.copy(
                    start = findNewAttributeReference(start),
                    interval = findNewAttributeReference(interval),
                    child = findNewAttributeReference(child))
                case e@DayOfMonth(child) => e.copy(child = findNewAttributeReference(child))
                case e@DayOfWeek(child) => e.copy(child = findNewAttributeReference(child))
                case e@DayOfYear(child) => e.copy(child = findNewAttributeReference(child))
                case e@DynamicPruningExpression(child) => e.copy(child = findNewAttributeReference(child))
                case e@Empty2Null(child) => e.copy(child = findNewAttributeReference(child))
                case e@EncodeUsingSerializer(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Exp(child) => e.copy(child = findNewAttributeReference(child))
                case e@Explode(child) => e.copy(child = findNewAttributeReference(child))
                case e@Expm1(child) => e.copy(child = findNewAttributeReference(child))
                case e@Extract(field, source, child) =>
                  e.copy(
                    field = findNewAttributeReference(field),
                    source = findNewAttributeReference(source),
                    child = findNewAttributeReference(child))
                case e@ExtractIntervalDays(child) => e.copy(child = findNewAttributeReference(child))
                case e@ExtractIntervalMonths(child) => e.copy(child = findNewAttributeReference(child))
                case e@ExtractIntervalYears(child) => e.copy(child = findNewAttributeReference(child))
                case e@ExtractIntervalHours(child) => e.copy(child = findNewAttributeReference(child))
                case e@ExtractIntervalMinutes(child) => e.copy(child = findNewAttributeReference(child))
                case e@ExtractIntervalSeconds(child) => e.copy(child = findNewAttributeReference(child))
                case e@Factorial(child) => e.copy(child = findNewAttributeReference(child))
                case e@Flatten(child) => e.copy(child = findNewAttributeReference(child))
                case e@Floor(child) => e.copy(child = findNewAttributeReference(child))
                case e@GetArrayStructFields(child, _, _, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@GetExternalRowField(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@GetStructField(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@Hex(child) => e.copy(child = findNewAttributeReference(child))
                case e@Hour(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@IfNull(left, right, child) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right),
                    child = findNewAttributeReference(child)
                  )
                case e@InSet(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@InitCap(child) => e.copy(child = findNewAttributeReference(child))
                case e@Inline(child) => e.copy(child = findNewAttributeReference(child))
                case e@IsNaN(child) => e.copy(child = findNewAttributeReference(child))
                case e@IsNotNull(child) => e.copy(child = findNewAttributeReference(child))
                case e@IsNull(child) => e.copy(child = findNewAttributeReference(child))
                case e@JsonObjectKeys(child) => e.copy(child = findNewAttributeReference(child))
                case e@JsonToStructs(_, _, child, _) => e.copy(child = findNewAttributeReference(child))
                case e@KnownFloatingPointNormalized(child) => e.copy(child = findNewAttributeReference(child))
                case e@KnownNotNull(child) => e.copy(child = findNewAttributeReference(child))
                case e@LastDay(child) => e.copy(startDate = findNewAttributeReference(child))
                case e@Left(str, len, child) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    len = findNewAttributeReference(len),
                    child = findNewAttributeReference(child))
                case e@Length(child) => e.copy(child = findNewAttributeReference(child))
                case e@LengthOfJsonArray(child) => e.copy(child = findNewAttributeReference(child))
                case e@LikeAll(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@LikeAny(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Log(child) => e.copy(child = findNewAttributeReference(child))
                case e@Log10(child) => e.copy(child = findNewAttributeReference(child))
                case e@Log1p(child) => e.copy(child = findNewAttributeReference(child))
                case e@Log2(child) => e.copy(child = findNewAttributeReference(child))
                case e@Lower(child) => e.copy(child = findNewAttributeReference(child))
                case e@MakeDecimal(child, _, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@MapEntries(child) => e.copy(child = findNewAttributeReference(child))
                case e@MapFromEntries(child) => e.copy(child = findNewAttributeReference(child))
                case e@MapKeys(child) => e.copy(child = findNewAttributeReference(child))
                case e@MapValues(child) => e.copy(child = findNewAttributeReference(child))
                case e@Md5(child) => e.copy(child = findNewAttributeReference(child))
                case e@MicrosToTimestamp(child) => e.copy(child = findNewAttributeReference(child))
                case e@MillisToTimestamp(child) => e.copy(child = findNewAttributeReference(child))
                case e@Minute(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Month(child) => e.copy(child = findNewAttributeReference(child))
                case e@MultiAlias(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@NormalizeNaNAndZero(child) => e.copy(child = findNewAttributeReference(child))
                case e@Not(child) => e.copy(child = findNewAttributeReference(child))
                case e@NotLikeAll(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@NotLikeAny(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@NullIf(left, right, child) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right),
                    child = findNewAttributeReference(child)
                  )
                case e@Nvl(left, right, child) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right),
                    child = findNewAttributeReference(child)
                  )
                case e@Nvl2(expr1, expr2, expr3, child) =>
                  e.copy(
                    expr1 = findNewAttributeReference(expr1),
                    expr2 = findNewAttributeReference(expr2),
                    expr3 = findNewAttributeReference(expr3),
                    child = findNewAttributeReference(child))
                case e@OctetLength(child) => e.copy(child = findNewAttributeReference(child))
                case e@ParseToDate(left, _, child) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    child = findNewAttributeReference(child))
                case e@ParseToTimestamp(left, _, child) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    child = findNewAttributeReference(child))
                case e@PosExplode(child) => e.copy(child = findNewAttributeReference(child))
                case e@PreciseTimestampConversion(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@PrintToStderr(child) => e.copy(child = findNewAttributeReference(child))
                case e@PromotePrecision(child) => e.copy(child = findNewAttributeReference(child))
                case e@Quarter(child) => e.copy(child = findNewAttributeReference(child))
                case e@RaiseError(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Rand(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Randn(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Reverse(child) => e.copy(child = findNewAttributeReference(child))
                case e@Rint(child) => e.copy(child = findNewAttributeReference(child))
                case e@SchemaOfCsv(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@SchemaOfJson(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Second(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@SecondWithFraction(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@SecondsToTimestamp(child) => e.copy(child = findNewAttributeReference(child))
                case e@Sha1(child) => e.copy(child = findNewAttributeReference(child))
                case e@Shuffle(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Signum(child) => e.copy(child = findNewAttributeReference(child))
                case e@Sin(child) => e.copy(child = findNewAttributeReference(child))
                case e@Sinh(child) => e.copy(child = findNewAttributeReference(child))
                case e@Size(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@SortPrefix(child) => e.copy(child = findNewAttributeReference(child).asInstanceOf[SortOrder])
                case e@SortOrder(child, _, _, sameOrderExpressions) =>
                  e.copy(
                    child = findNewAttributeReference(child),
                    sameOrderExpressions = sameOrderExpressions.map(findNewAttributeReference))
                case e@SoundEx(child) => e.copy(child = findNewAttributeReference(child))
                case e@Sqrt(child) => e.copy(child = findNewAttributeReference(child))
                case e@StringSpace(child) => e.copy(child = findNewAttributeReference(child))
                case e@StructsToCsv(_, child, _) => e.copy(child = findNewAttributeReference(child))
                case e@StructsToJson(_, child, _) => e.copy(child = findNewAttributeReference(child))
                case e@Tan(child) => e.copy(child = findNewAttributeReference(child))
                case e@Tanh(child) => e.copy(child = findNewAttributeReference(child))
                case e@TimeWindow(child, _, _, _) => e.copy(timeColumn = findNewAttributeReference(child))
                case e@ToDegrees(child) => e.copy(child = findNewAttributeReference(child))
                case e@ToRadians(child) => e.copy(child = findNewAttributeReference(child))
                case e@TypeOf(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnBase64(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnaryMinus(child, _) => e.copy(child = findNewAttributeReference(child))
                case e@UnaryPositive(child) => e.copy(child = findNewAttributeReference(child))
                case e@Unhex(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnixDate(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnixMillis(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnixMicros(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnixSeconds(child) => e.copy(child = findNewAttributeReference(child))
                case e@UnscaledValue(child) => e.copy(child = findNewAttributeReference(child))
                case e@UpCast(child, _, _) => e.copy(child = findNewAttributeReference(child))
                case e@Upper(child) => e.copy(child = findNewAttributeReference(child))
                case e@WeekDay(child) => e.copy(child = findNewAttributeReference(child))
                case e@WeekOfYear(child) => e.copy(child = findNewAttributeReference(child))
                case e@Year(child) => e.copy(child = findNewAttributeReference(child))
                case e@YearOfWeek(child) => e.copy(child = findNewAttributeReference(child))

                //binary expression
                case e@Add(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@AddMonths(left, right) =>
                  e.copy(
                    startDate = findNewAttributeReference(left),
                    numMonths = findNewAttributeReference(right))
                case e@And(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayContains(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayExcept(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayIntersect(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayPosition(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayRemove(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayRepeat(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArrayUnion(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ArraysOverlap(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Atan2(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@BRound(child, scale) =>
                  e.copy(
                    child = findNewAttributeReference(child),
                    scale = findNewAttributeReference(scale))
                case e@BitwiseAnd(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@BitwiseOr(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@BitwiseXor(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Contains(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@DateAdd(startDate, days) =>
                  e.copy(
                    startDate = findNewAttributeReference(startDate),
                    days = findNewAttributeReference(days))
                case e@DateAddInterval(start, interval, _, _) =>
                  e.copy(
                    start = findNewAttributeReference(start),
                    interval = findNewAttributeReference(interval))
                case e@DateDiff(endDate, startDate) =>
                  e.copy(
                    endDate = findNewAttributeReference(endDate),
                    startDate = findNewAttributeReference(startDate))
                case e@DateFormatClass(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@DateSub(startDate, days) =>
                  e.copy(
                    startDate = findNewAttributeReference(startDate),
                    days = findNewAttributeReference(days))
                case e@Decode(bin, charset) =>
                  e.copy(
                    bin = findNewAttributeReference(bin),
                    charset = findNewAttributeReference(charset))
                case e@Divide(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@DivideInterval(interval, num, _) =>
                  e.copy(
                    interval = findNewAttributeReference(interval),
                    num = findNewAttributeReference(num))
                case e@ElementAt(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Encode(value, charset) =>
                  e.copy(
                    value = findNewAttributeReference(value),
                    charset = findNewAttributeReference(charset))
                case e@EndsWith(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@EqualNullSafe(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@EqualTo(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@FindInSet(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@FormatNumber(x, d) =>
                  e.copy(
                    x = findNewAttributeReference(x),
                    d = findNewAttributeReference(d))
                case e@FromUTCTimestamp(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@FromUnixTime(sec, format, _) =>
                  e.copy(
                    sec = findNewAttributeReference(sec),
                    format = findNewAttributeReference(format))
                case e@GetArrayItem(child, ordinal, _) =>
                  e.copy(
                    child = findNewAttributeReference(child),
                    ordinal = findNewAttributeReference(ordinal))
                case e@GetJsonObject(json, path) =>
                  e.copy(
                    json = findNewAttributeReference(json),
                    path = findNewAttributeReference(path))
                case e@GetMapValue(child, key, _) =>
                  e.copy(
                    child = findNewAttributeReference(child),
                    key = findNewAttributeReference(key))
                case e@GreaterThan(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@GreaterThanOrEqual(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Hypot(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@IntegralDivide(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@LessThan(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@LessThanOrEqual(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Levenshtein(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Like(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Logarithm(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@MapFromArrays(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Multiply(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@MultiplyInterval(interval, num, _) =>
                  e.copy(
                    interval = findNewAttributeReference(interval),
                    num = findNewAttributeReference(num))
                case e@NaNvl(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@NextDay(startDate, dayOfWeek) =>
                  e.copy(
                    startDate = findNewAttributeReference(startDate),
                    dayOfWeek = findNewAttributeReference(dayOfWeek))
                case e@Or(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Pmod(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Pow(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@RLike(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Remainder(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@Round(child, scale) =>
                  e.copy(
                    child = findNewAttributeReference(child),
                    scale = findNewAttributeReference(scale))
                case e@Sha2(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ShiftLeft(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ShiftRight(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ShiftRightUnsigned(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@SortArray(base, ascendingOrder) =>
                  e.copy(
                    base = findNewAttributeReference(base),
                    ascendingOrder = findNewAttributeReference(ascendingOrder))
                case e@StartsWith(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@StringInstr(str, substr) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    substr = findNewAttributeReference(substr))
                case e@StringRepeat(str, times) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    times = findNewAttributeReference(times))
                case e@Subtract(left, right, _) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@SubtractDates(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@SubtractTimestamps(endTimestamp, startTimestamp) =>
                  e.copy(
                    endTimestamp = findNewAttributeReference(endTimestamp),
                    startTimestamp = findNewAttributeReference(startTimestamp))
                case e@TimeAdd(start, interval, _) =>
                  e.copy(
                    start = findNewAttributeReference(start),
                    interval = findNewAttributeReference(interval))
                case e@ToUTCTimestamp(left, right) =>
                  e.copy(
                    left = findNewAttributeReference(left),
                    right = findNewAttributeReference(right))
                case e@ToUnixTimestamp(timeExp, format, _, _) =>
                  e.copy(
                    timeExp = findNewAttributeReference(timeExp),
                    format = findNewAttributeReference(format))
                case e@TruncDate(date, format) =>
                  e.copy(
                    date = findNewAttributeReference(date),
                    format = findNewAttributeReference(format))
                case e@TruncTimestamp(format, timestamp, _) =>
                  e.copy(
                    format = findNewAttributeReference(format),
                    timestamp = findNewAttributeReference(timestamp))
                case e@UnixTimestamp(timeExp, format, _, _) =>
                  e.copy(
                    timeExp = findNewAttributeReference(timeExp),
                    format = findNewAttributeReference(format))
                case e@XPathBoolean(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))
                case e@XPathDouble(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))
                case e@XPathFloat(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))
                case e@XPathList(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))
                case e@XPathLong(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))
                case e@XPathShort(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))
                case e@XPathString(xml, path) =>
                  e.copy(
                    xml = findNewAttributeReference(xml),
                    path = findNewAttributeReference(path))

                //Ternary Expression
                case e@Conv(numExpr, fromBaseExpr, toBaseExpr) =>
                  e.copy(
                    numExpr = findNewAttributeReference(numExpr),
                    fromBaseExpr = findNewAttributeReference(fromBaseExpr),
                    toBaseExpr = findNewAttributeReference(toBaseExpr))
                case e@MakeDate(year, month, day, _) =>
                  e.copy(
                    year = findNewAttributeReference(year),
                    month = findNewAttributeReference(month),
                    day = findNewAttributeReference(day))
                case e@MonthsBetween(date1, date2, roundOff, _) =>
                  e.copy(
                    date1 = findNewAttributeReference(date1),
                    date2 = findNewAttributeReference(date2),
                    roundOff = findNewAttributeReference(roundOff))
                case e@RegExpExtract(subject, regexp, idx) =>
                  e.copy(
                    subject = findNewAttributeReference(subject),
                    regexp = findNewAttributeReference(regexp),
                    idx = findNewAttributeReference(idx))
                case e@RegExpExtractAll(subject, regexp, idx) =>
                  e.copy(
                    subject = findNewAttributeReference(subject),
                    regexp = findNewAttributeReference(regexp),
                    idx = findNewAttributeReference(idx))
                case e@Slice(x, start, length) =>
                  e.copy(
                    x = findNewAttributeReference(x),
                    start = findNewAttributeReference(start),
                    length = findNewAttributeReference(length))
                case e@StringLPad(str, len, pad) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    len = findNewAttributeReference(len),
                    pad = findNewAttributeReference(pad))
                case e@StringLocate(substr, str, start) =>
                  e.copy(
                    substr = findNewAttributeReference(substr),
                    str = findNewAttributeReference(str),
                    start = findNewAttributeReference(start))
                case e@StringRPad(str, len, pad) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    len = findNewAttributeReference(len),
                    pad = findNewAttributeReference(pad))
                case e@StringReplace(srcExpr, searchExpr, replaceExpr) =>
                  e.copy(
                    srcExpr = findNewAttributeReference(srcExpr),
                    searchExpr = findNewAttributeReference(searchExpr),
                    replaceExpr = findNewAttributeReference(replaceExpr))
                case e@StringSplit(str, regex, limit) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    regex = findNewAttributeReference(regex),
                    limit = findNewAttributeReference(limit))
                case e@StringToMap(text, pairDelim, keyValueDelim) =>
                  e.copy(
                    text = findNewAttributeReference(text),
                    pairDelim = findNewAttributeReference(pairDelim),
                    keyValueDelim = findNewAttributeReference(keyValueDelim))
                case e@StringTranslate(srcExpr, matchingExpr, replaceExpr) =>
                  e.copy(
                    srcExpr = findNewAttributeReference(srcExpr),
                    matchingExpr = findNewAttributeReference(matchingExpr),
                    replaceExpr = findNewAttributeReference(replaceExpr))
                case e@Substring(str, pos, len) =>
                  e.copy(
                    str = findNewAttributeReference(str),
                    pos = findNewAttributeReference(pos),
                    len = findNewAttributeReference(len))
                case e@SubstringIndex(strExpr, delimExpr, countExpr) =>
                  e.copy(
                    strExpr = findNewAttributeReference(strExpr),
                    delimExpr = findNewAttributeReference(delimExpr),
                    countExpr = findNewAttributeReference(countExpr))


                //Quaternary Expression
                case e@Overlay(input, replace, pos, len) =>
                  e.copy(
                    input = findNewAttributeReference(input),
                    replace = findNewAttributeReference(replace),
                    pos = findNewAttributeReference(pos),
                    len = findNewAttributeReference(len))

                case e@RegExpReplace(subject, regexp, rep, pos) =>
                  e.copy(
                    subject = findNewAttributeReference(subject),
                    regexp = findNewAttributeReference(regexp),
                    rep = findNewAttributeReference(rep),
                    pos = findNewAttributeReference(pos))
                case e@WidthBucket(value, minValue, maxValue, numBucket) =>
                  e.copy(
                    value = findNewAttributeReference(value),
                    minValue = findNewAttributeReference(minValue),
                    maxValue = findNewAttributeReference(maxValue),
                    numBucket = findNewAttributeReference(numBucket))


                //Septenary Expression
                case e@MakeInterval(years, months, weeks, days, hours, mins, secs, _) =>
                  e.copy(
                    years = findNewAttributeReference(years),
                    months = findNewAttributeReference(months),
                    weeks = findNewAttributeReference(weeks),
                    days = findNewAttributeReference(days),
                    hours = findNewAttributeReference(hours),
                    mins = findNewAttributeReference(mins),
                    secs = findNewAttributeReference(secs))
                case e@MakeTimestamp(year, month, day, hour, min, sec, _, _, _) =>
                  e.copy(
                    year = findNewAttributeReference(year),
                    month = findNewAttributeReference(month),
                    day = findNewAttributeReference(day),
                    hour = findNewAttributeReference(hour),
                    min = findNewAttributeReference(min),
                    sec = findNewAttributeReference(sec))


                //ComplexTypeMergingExpression
                case e@CaseWhen(branches, elseValue) =>
                  val newBranches = branches.map(m => (findNewAttributeReference(m._1), findNewAttributeReference(m._2)))
                  val newElseValue = if (elseValue.isDefined) {
                    Some(findNewAttributeReference(elseValue.get))
                  } else {
                    None
                  }
                  e.copy(
                    branches = newBranches,
                    elseValue = newElseValue)
                case e@Coalesce(children) => e.copy(children = children.map(findNewAttributeReference))
                case e@Concat(children) => e.copy(children = children.map(findNewAttributeReference))
                case e@Greatest(children) => e.copy(children = children.map(findNewAttributeReference))
                case e@If(predicate, trueValue, falseValue) =>
                  e.copy(
                    predicate = findNewAttributeReference(predicate),
                    trueValue = findNewAttributeReference(trueValue),
                    falseValue = findNewAttributeReference(falseValue))
                case e@Least(children) => e.copy(children = children.map(findNewAttributeReference))
                case e@MapConcat(children) => e.copy(children = children.map(findNewAttributeReference))

                //UserDefinedExpression
                case e@PythonUDF(_, _, _, children, _, _, _) =>
                  e.copy(children = children.map(findNewAttributeReference))

                case e@ScalaAggregator(children, _, _, _, _, _, _, _) =>
                  e.copy(children = children.map(findNewAttributeReference))
                case e@ScalaUDAF(children, _, _, _) =>
                  e.copy(children = children.map(findNewAttributeReference))
                case e@ScalaUDF(_, _, children, _, _, _, _, _) =>
                  e.copy(children = children.map(findNewAttributeReference))


                case c@ConcatWs(children) => c.copy(children = children.map(findNewAttributeReference))
                case c@Elt(children, _) => c.copy(children = children.map(findNewAttributeReference))
                case c@ParseUrl(children, _) => c.copy(children = children.map(findNewAttributeReference))
                case c@Sentences(str, language, country) =>
                  c.copy(
                    str = findNewAttributeReference(str),
                    language = findNewAttributeReference(language),
                    country = findNewAttributeReference(country))


                case e@Years(child) => e.copy(child = findNewAttributeReference(child))
                case e@Months(child) => e.copy(child = findNewAttributeReference(child))
                case e@Days(child) => e.copy(child = findNewAttributeReference(child))
                case e@Hours(child) => e.copy(child = findNewAttributeReference(child))
                case e@Bucket(_, child) => e.copy(child = findNewAttributeReference(child))

                case unsupport => throw LakeSoulErrors.unsupportedLogicalPlanWhileRewriteQueryException(unsupport.sql)
              }
            }
          }

          def checkAndReplaceRangeCondition(expression: Expression): (Boolean, Expression) = {
            val formattedName = ConstructQueryInfo.getFinalStringByReplace(
              expression.sql,
              queryInfo.tableInfo,
              queryInfo.columnAsInfo)
            if (equalRangeColumns.contains(formattedName)) {
              //if this condition is equal to view's, remove it
              (true, null)
            } else {
              (false, findNewAttributeReference(expression))
            }
          }

          //parse, check and replace filter expression
          def parseCondition(expression: Expression): Expression = {
            expression match {
              case c@GreaterThan(left: AttributeReference, right: Literal) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(left)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(left = replaceAttr)
                }

              case c@GreaterThan(left: Literal, right: AttributeReference) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(right)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(right = replaceAttr)
                }

              case c@GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(left)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(left = replaceAttr)
                }

              case c@GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(right)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(right = replaceAttr)
                }

              case c@LessThan(left: AttributeReference, right: Literal) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(left)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(left = replaceAttr)
                }

              case c@LessThan(left: Literal, right: AttributeReference) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(right)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(right = replaceAttr)
                }

              case c@LessThanOrEqual(left: AttributeReference, right: Literal) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(left)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(left = replaceAttr)
                }

              case c@LessThanOrEqual(left: Literal, right: AttributeReference) =>
                val (conditionEqual, replaceAttr) = checkAndReplaceRangeCondition(right)
                if (conditionEqual) {
                  Literal(true)
                } else {
                  c.copy(right = replaceAttr)
                }

              case eq@EqualTo(left: Expression, right: Literal) =>
                val formattedName = ConstructQueryInfo.getFinalStringByReplace(
                  left.sql,
                  queryInfo.tableInfo,
                  queryInfo.columnAsInfo
                )
                if (chosenView.info.conditionEqualInfo.contains(formattedName)) {
                  Literal(true)
                } else {
                  eq.copy(left = findNewAttributeReference(left))
                }

              case eq@EqualTo(left: Literal, right: Expression) =>
                val formattedName = ConstructQueryInfo.getFinalStringByReplace(
                  right.sql,
                  queryInfo.tableInfo,
                  queryInfo.columnAsInfo
                )
                if (chosenView.info.conditionEqualInfo.contains(formattedName)) {
                  Literal(true)
                } else {
                  eq.copy(right = findNewAttributeReference(right))
                }

              case eq@EqualNullSafe(left: Expression, right: Literal) =>
                val formattedName = ConstructQueryInfo.getFinalStringByReplace(
                  left.sql,
                  queryInfo.tableInfo,
                  queryInfo.columnAsInfo
                )
                if (chosenView.info.conditionEqualInfo.contains(formattedName)) {
                  Literal(true)
                } else {
                  eq.copy(left = findNewAttributeReference(left))
                }

              case eq@EqualNullSafe(left: Literal, right: Expression) =>
                val formattedName = ConstructQueryInfo.getFinalStringByReplace(
                  right.sql,
                  queryInfo.tableInfo,
                  queryInfo.columnAsInfo
                )
                if (chosenView.info.conditionEqualInfo.contains(formattedName)) {
                  Literal(true)
                } else {
                  eq.copy(right = findNewAttributeReference(right))
                }


              case other =>
                val cond = ConstructQueryInfo.getFinalStringByReplace(
                  other.sql,
                  queryInfo.tableInfo,
                  queryInfo.columnAsInfo
                )
                if (chosenView.info.otherInfo.contains(cond)) {
                  Literal(true)
                } else {
                  findNewAttributeReference(other)
                }

            }
          }


          newPlan = plan resolveOperators {
            case prj@Project(projectList, child) =>
              val newProjectList = projectList.map(findNewAttributeReference(_).asInstanceOf[NamedExpression])
              prj.copy(projectList = newProjectList)

            case f@Filter(condition, child) =>
              val withoutAliasCondition = removeAlias(condition)
              val newCondition = splitConjunctivePredicates(withoutAliasCondition)
                .map(parseCondition)
                .reduceLeftOption(And).get
              f.copy(condition = newCondition)

            case join@Join(_, _, joinType, _, _) =>
              if (joinType.sql.equals(Inner.sql)) {
                //extract filter conditions from inner join
                val filterConditions = new ArrayBuffer[Expression]()
                extractFilterConditions(join, filterConditions)
                val usefulCondition = filterConditions.map(parseCondition).reduceLeftOption(And).get
                Filter(usefulCondition, newDataSource)
              } else {
                newDataSource
              }

            case _: Aggregate =>
              newDataSource

            //remove alias for lakesoul data source
            case sa@SubqueryAlias(ident, child@DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _))
              if ident.toString()
                .startsWith(s"${CatalogManager.SESSION_CATALOG_NAME}.${LakeSoulSourceUtils.NAME}") =>
              sa.copy(identifier = ident.copy(name = newTableName))


            case dsv2: DataSourceV2Relation =>
              newDataSource

          }


        } else {
          newPlan = plan
        }
      } catch {
        case e: Throwable => newPlan = plan
      }

      newPlan
    } else {
      plan
    }

  }

  //column of query condition which not matching view must exist
  // (or it's equivalence column) in view output column set
  def conditionColumnsExists(colName: String,
                             viewOutput: Set[String],
                             columnEqualInfo: Seq[Set[String]]): Boolean = {
    if (viewOutput.contains(colName)) {
      true
    } else {
      val find = columnEqualInfo.find(f => f.contains(colName))
      if (find.isDefined) {
        find.get.exists(e => viewOutput.contains(e))
      } else {
        false
      }

    }
  }

  def findColumnFromViewOutput(colName: String,
                               outputInfo: Map[String, String],
                               columnEqualInfo: Seq[Set[String]]): Option[(String, String)] = {
    var find = outputInfo.find(f => f._2.equals(colName))
    if (find.isDefined) {
      find
    } else {

      val equalCols = columnEqualInfo.find(f => f.contains(colName))
      if (equalCols.isDefined) {
        val equalColsItr = equalCols.get.iterator
        var notFound = true

        while (notFound && equalColsItr.hasNext) {
          val col = equalColsItr.next()
          find = outputInfo.find(f => f._2.equals(col))
          if (find.isDefined) {
            notFound = false
          }
        }
        if (notFound) {
          None
        } else {
          find
        }
      } else {
        None
      }

    }
  }


  def removeAlias(condition: Expression): Expression = {
    condition match {
      case Alias(c, _) => c
      case other => other
    }
  }

  def extractFilterConditions(plan: LogicalPlan,
                              filterConditions: ArrayBuffer[Expression]): Unit = {
    plan match {
      case Join(left, right, joinType, condition, _) =>
        assert(joinType.sql.equals(Inner.sql))
        if (condition.isDefined) {
          val newCondition = removeAlias(condition.get)
          filterConditions.append(splitConjunctivePredicates(newCondition): _*)
        }
        extractFilterConditions(left, filterConditions)
        extractFilterConditions(right, filterConditions)

      case Filter(condition, child) =>
        val newCondition = removeAlias(condition)
        filterConditions.append(splitConjunctivePredicates(newCondition): _*)
        extractFilterConditions(child, filterConditions)

      case lp: LogicalPlan if lp.children.nonEmpty =>
        lp.children.foreach(extractFilterConditions(_, filterConditions))

      case _ =>
    }
  }

  def buildDataSourceV2Relation(viewName: String): (String, DataSourceV2Relation) = {
    val (exists, tablePath) = MetaVersion.isShortTableNameExists(viewName)
    assert(exists, "Material view may be dropped very recently")
    val table = LakeSoulTableV2(spark, new Path(tablePath))

    (tablePath,
      DataSourceV2Relation(
        table,
        table.schema().toAttributes,
        None,
        None,
        new CaseInsensitiveStringMap(
          Map("basePath" -> tablePath).asJava))
    )
  }


}
