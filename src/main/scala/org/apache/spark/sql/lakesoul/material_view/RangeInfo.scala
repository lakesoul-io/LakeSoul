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

package org.apache.spark.sql.lakesoul.material_view

import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.material_view.RangeInfo.compareRange
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

class RangeInfo(dataType: DataType) {
  private var lower: Any = _
  private var upper: Any = _
  private var includeLower: Boolean = true
  private var includeUpper: Boolean = true

  def buildDetail(): RangeDetail = {
    RangeDetail(dataType, lower, upper, includeLower, includeUpper)
  }

  def setRangeProperties(colName: String,
                         limit: Any,
                         rangeType: String): Unit = {
    rangeType match {
      case "GreaterThan" =>
        if (lower == null) {
          lower = limit
          includeLower = false
        } else {
          val re = compareRange(limit, lower, dataType)
          if (re >= 0) {
            lower = limit
            includeLower = false
          }
        }

      case "GreaterThanOrEqual" =>
        if (lower == null) {
          lower = limit
        } else {
          val re = compareRange(limit, lower, dataType)
          if (re > 0) {
            lower = limit
            includeLower = true
          } else if (re == 0) {
            lower = limit
          }
        }

      case "LessThan" =>
        if (upper == null) {
          upper = limit
          includeUpper = false
        } else {
          val re = compareRange(limit, upper, dataType)
          if (re <= 0) {
            upper = limit
            includeUpper = false
          }
        }

      case "LessThanOrEqual" =>
        if (upper == null) {
          upper = limit
        } else {
          val re = compareRange(limit, upper, dataType)
          if (re < 0) {
            upper = limit
            includeUpper = true
          } else if (re == 0) {
            upper = limit
          }
        }
    }
  }

}

object RangeInfo {
  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r

  private val otherTypes = {
    Seq(
      ("NullType", NullType),
      ("DateType", DateType),
      ("TimestampType", TimestampType),
      ("BinaryType", BinaryType),
      ("IntegerType", IntegerType),
      ("BooleanType", BooleanType),
      ("LongType", LongType),
      ("DoubleType", DoubleType),
      ("FloatType", FloatType),
      ("ShortType", ShortType),
      ("ByteType", ByteType),
      ("StringType", StringType),
      ("CalendarIntervalType", CalendarIntervalType))
      .toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case VARCHAR_TYPE(length) => VarcharType(length.toInt)
      case other => otherTypes.getOrElse(
        other,
        throw new IllegalArgumentException(
          s"Failed to convert the JSON string '$name' to a data type."))
    }
  }

  private def getValue(str: String): Any = {
    if (str.equals("_STAR_META_NULL_")) {
      null
    } else {
      str
    }
  }

  def buildDetail(str: String): RangeDetail = {
    val split = str.split(",")
    RangeDetail(
      nameToType(split(0)),
      getValue(split(1)),
      getValue(split(2)),
      split(3).toBoolean,
      split(4).toBoolean)
  }

  def setRangeInfo(rangeInfo: mutable.Map[String, RangeInfo],
                   dataType: DataType,
                   colName: String,
                   limit: Any,
                   rangeType: String): Unit = {
    val info = rangeInfo.getOrElse(
      colName,
      new RangeInfo(dataType)
    )

    info.setRangeProperties(colName, limit, rangeType)

    if (!rangeInfo.contains(colName)) {
      rangeInfo.put(colName, info)
    }

  }


  def matchEqual(a: RangeDetail, b: RangeDetail): Boolean = {
    val lowerMatch = if (a.lower == null && b.lower == null) {
      true
    } else if (a.lower != null && b.lower != null) {
      if (a.includeLower == b.includeLower && transAndCompareRange(a.lower, b.lower, a.dataType) == 0) {
        true
      } else {
        false
      }
    } else {
      false
    }

    if (lowerMatch) {
      if (a.upper == null && b.upper == null) {
        true
      } else if (a.upper != null && b.upper != null) {
        if (a.includeUpper == b.includeUpper && transAndCompareRange(a.upper, b.upper, a.dataType) == 0) {
          true
        } else {
          false
        }
      } else {
        false
      }
    } else {
      false
    }
  }

  def compareRange(left: Any, right: Any, dataType: DataType): Int = {
    dataType match {
      case BooleanType => left.asInstanceOf[Boolean].compareTo(right.asInstanceOf[Boolean])
      case ByteType => left.asInstanceOf[Byte].compareTo(right.asInstanceOf[Byte])
      case ShortType => left.asInstanceOf[Short].compareTo(right.asInstanceOf[Short])
      case IntegerType | DateType => left.asInstanceOf[Int].compareTo(right.asInstanceOf[Int])
      case LongType | TimestampType => left.asInstanceOf[Long].compareTo(right.asInstanceOf[Long])
      case FloatType => left.asInstanceOf[Float].compareTo(right.asInstanceOf[Float])
      case DoubleType => left.asInstanceOf[Double].compareTo(right.asInstanceOf[Double])
      case StringType => left.asInstanceOf[UTF8String].compareTo(right.asInstanceOf[UTF8String])
      case _ => throw LakeSoulErrors.unsupportedDataTypeInMaterialRewriteQueryException(dataType)
    }
  }


  def transAndCompareRange(left: Any, right: Any, dataType: DataType): Int = {
    dataType match {
      case BooleanType => left.toString.toBoolean.compareTo(right.toString.toBoolean)
      case ByteType => left.toString.toByte.compareTo(right.toString.toByte)
      case ShortType => left.toString.toShort.compareTo(right.toString.toShort)
      case IntegerType | DateType => left.toString.toInt.compareTo(right.toString.toInt)
      case LongType | TimestampType => left.toString.toLong.compareTo(right.toString.toLong)
      case FloatType => left.toString.toFloat.compareTo(right.toString.toFloat)
      case DoubleType => left.toString.toDouble.compareTo(right.toString.toDouble)
      case StringType => UTF8String.fromString(left.toString).compareTo(UTF8String.fromString(right.toString))
      case _ => throw LakeSoulErrors.unsupportedDataTypeInMaterialRewriteQueryException(dataType)
    }
  }


  def valueInRange(value: String, range: RangeDetail): Boolean = {
    //check lower boundary
    val lowerIn = if (range.lower == null) {
      true
    } else {
      val result = transAndCompareRange(range.lower, value, range.dataType)
      if (result < 0) {
        true
      } else if (result == 0 && range.includeLower) {
        true
      } else {
        false
      }
    }

    if (lowerIn) {
      //check upper boundary
      if (range.upper == null) {
        true
      } else {
        val result = transAndCompareRange(range.upper, value, range.dataType)
        if (result > 0) {
          true
        } else if (result == 0 && range.includeUpper) {
          true
        } else {
          false
        }
      }
    } else {
      false
    }
  }


  //return -1 if queryRange is not in scope of viewRange
  //return 0 if queryRange is equal to viewRange
  //return 1 if queryRange is a subset of viewRange
  def compareRangeDetail(queryRange: RangeDetail, viewRange: RangeDetail): Int = {
    var result = 0

    if (queryRange.lower == null && viewRange.lower != null) {
      result = -1
    } else if (viewRange.lower != null) {
      val compare = transAndCompareRange(queryRange.lower, viewRange.lower, queryRange.dataType)

      if (compare < 0) {
        result = -1
      } else if (compare == 0) {
        if (queryRange.includeLower && !viewRange.includeLower) {
          result = -1
        } else if (!queryRange.includeLower.equals(viewRange.includeLower)) {
          result = 1
        }
      } else {
        result = 1
      }
    }

    if (result >= 0) {
      if (queryRange.upper == null && viewRange.upper != null) {
        result = -1
      } else if (viewRange.upper != null) {
        val compare = RangeInfo.transAndCompareRange(queryRange.upper, viewRange.upper, queryRange.dataType)
        if (compare > 0) {
          result = -1
        } else if (compare == 0) {
          if (queryRange.includeUpper && !viewRange.includeUpper) {
            result = -1
          } else if (!queryRange.includeUpper.equals(viewRange.includeUpper)) {
            result = 1
          }
        } else {
          result = 1
        }
      }
    }
    result


  }


}

//range detail info of query/table
case class RangeDetail(dataType: DataType,
                       lower: Any,
                       upper: Any,
                       includeLower: Boolean,
                       includeUpper: Boolean) {
  override def toString: String = {
    dataType.toString + "," + getString(lower) + "," + getString(upper) + "," +
      includeLower.toString + "," + includeUpper.toString
  }

  private def getString(str: Any): String = {
    if (str == null) {
      "_STAR_META_NULL_"
    } else {
      str.toString
    }
  }
}
