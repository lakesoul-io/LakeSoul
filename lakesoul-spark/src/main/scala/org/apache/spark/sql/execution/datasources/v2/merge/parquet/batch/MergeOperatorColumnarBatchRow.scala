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

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.{DefaultMergeOp, FieldIndex, MergeColumnIndex, MergeOperator}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
  *
  * @param columns        ordered column vectors of all file
  * @param mergeOps       merge operators
  * @param indexTypeArray result schema index and type
  */
class MergeOperatorColumnarBatchRow(columns: Array[ColumnVector],
                                    mergeOps: Seq[MergeOperator[Any]],
                                    indexTypeArray: Seq[FieldIndex]) extends MergeBatchRow {

  val size: Int = indexTypeArray.length
  var idMix: Seq[Seq[MergeColumnIndex]] = _
  var value: Array[Any] = new Array[Any](size)

  private def getIndex(ordinal: Int): Seq[MergeColumnIndex] = {
    idMix(ordinal)
  }


  //merge data from idMix
  def mergeValues(): Unit = {
    idMix.zipWithIndex.foreach(m => {
      if (m._1.nonEmpty) {
        val dataType = indexTypeArray(m._2).filedType
        dataType match {
          case StringType => mergeUTF8String(m._1, m._2)
          case IntegerType | DateType => mergeInt(m._1, m._2)
          case BooleanType => mergeBoolean(m._1, m._2)
          case ByteType => mergeBoolean(m._1, m._2)
          case ShortType => mergeShort(m._1, m._2)
          case LongType | TimestampType => mergeLong(m._1, m._2)
          case FloatType => mergeFloat(m._1, m._2)
          case DoubleType => mergeDouble(m._1, m._2)
          case BinaryType => mergeBinary(m._1, m._2)
          case CalendarIntervalType => mergeInterval(m._1, m._2)
          case t: DecimalType => mergeDecimal(m._1, m._2, t.precision, t.scale)
          case t: StructType => mergeStruct(m._1, m._2, t.size)
          case _: ArrayType => mergeArray(m._1, m._2)
          case _: MapType => mergeMap(m._1, m._2)
          case o => throw new UnsupportedOperationException(s"LakeSoul MergeOperator don't support type ${o.typeName}")
        }
      }
    })
  }

  private def getMergeOp(ordinal: Int): MergeOperator[Any] = {
    mergeOps(ordinal)
  }

  override def numFields(): Int = {
    idMix.length
  }

  override def copy(): InternalRow = {
    val row: GenericInternalRow = new GenericInternalRow(idMix.length)
    (0 to numFields()).foreach(i => {
      if (isNullAt(i)) {
        row.setNullAt(i)
      } else {
        val colIdAndRowId: Seq[MergeColumnIndex] = getIndex(i)
        val dt = columns(colIdAndRowId.head.columnVectorIndex).dataType()
        setRowData(i, dt, row)
      }
    })
    row
  }


  override def anyNull: Boolean = {
    throw new UnsupportedOperationException()
  }

  override def isNullAt(ordinal: Int): Boolean = {
    getIndex(ordinal).isEmpty || value(ordinal) == null
  }


  override def getBoolean(ordinal: Int): Boolean = {
    value(ordinal).asInstanceOf[Boolean]
  }

  override def getByte(ordinal: Int): Byte = {
    value(ordinal).asInstanceOf[Byte]
  }

  override def getShort(ordinal: Int): Short = {
    value(ordinal).asInstanceOf[Short]
  }

  override def getInt(ordinal: Int): Int = {
    value(ordinal).asInstanceOf[Int]
  }

  override def getLong(ordinal: Int): Long = {
    value(ordinal).asInstanceOf[Long]
  }

  override def getFloat(ordinal: Int): Float = {
    value(ordinal).asInstanceOf[Float]
  }

  override def getDouble(ordinal: Int): Double = {
    value(ordinal).asInstanceOf[Double]
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    value(ordinal).asInstanceOf[Decimal]
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    value(ordinal).asInstanceOf[UTF8String]
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    value(ordinal).asInstanceOf[Array[Byte]]
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    value(ordinal).asInstanceOf[CalendarInterval]
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    value(ordinal).asInstanceOf[InternalRow]
  }

  override def getArray(ordinal: Int): ArrayData = {
    value(ordinal).asInstanceOf[ArrayData]
  }

  override def getMap(ordinal: Int): MapData = {
    value(ordinal).asInstanceOf[MapData]
  }

  override def update(i: Int, value: Any): Unit = {
    throw new UnsupportedOperationException()
  }

  override def setNullAt(i: Int): Unit = {
    throw new UnsupportedOperationException()
  }


  /** merge values */

  def mergeBoolean(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getBoolean(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getBoolean(m.rowIndex)
        }
      }).asInstanceOf[Seq[Boolean]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Boolean]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeByte(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getByte(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getByte(m.rowIndex)
        }
      }).asInstanceOf[Seq[Byte]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Byte]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeShort(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getShort(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getShort(m.rowIndex)
        }
      }).asInstanceOf[Seq[Short]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Short]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeInt(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getInt(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getInt(m.rowIndex)
        }
      }).asInstanceOf[Seq[Int]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Int]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeLong(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getLong(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getLong(m.rowIndex)
        }
      }).asInstanceOf[Seq[Int]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Int]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeFloat(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getFloat(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getFloat(m.rowIndex)
        }
      }).asInstanceOf[Seq[Float]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Float]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeDouble(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getDouble(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getDouble(m.rowIndex)
        }
      }).asInstanceOf[Seq[Double]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Double]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeDecimal(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int, precision: Int, scale: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getDecimal(colIdAndRowId.last.rowIndex, precision, scale)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getDecimal(colIdAndRowId.last.rowIndex, precision, scale)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Decimal]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeUTF8String(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getUTF8String(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getUTF8String(m.rowIndex).toString
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[String]]
      value(ordinal) = UTF8String.fromString(mergeOp.mergeData(data))
    }
  }

  def mergeBinary(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getBinary(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getBinary(m.rowIndex)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Array[Byte]]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeInterval(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getInterval(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getInterval(m.rowIndex)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[CalendarInterval]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeStruct(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int, numFields: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getStruct(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getStruct(m.rowIndex)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[InternalRow]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeArray(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getArray(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getArray(m.rowIndex)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[ArrayData]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeMap(colIdAndRowId: Seq[MergeColumnIndex], ordinal: Int): Unit = {
    if (getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]) {
      if (columns(colIdAndRowId.last.columnVectorIndex).isNullAt(colIdAndRowId.last.rowIndex)) {
        value(ordinal) = null
      } else {
        value(ordinal) = columns(colIdAndRowId.last.columnVectorIndex).getMap(colIdAndRowId.last.rowIndex)
      }
    } else {
      val data = colIdAndRowId.map(m => {
        if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
          null
        } else {
          columns(m.columnVectorIndex).getMap(m.rowIndex)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[MapData]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }


  ///////////////////////////////////////////////////////////////////////////////////


  /** get values need to be merged */

  def getMergeBoolean(ordinal: Int): Seq[Boolean] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getBoolean(m.rowIndex)
      }
    }).asInstanceOf[Seq[Boolean]]
  }

  def getMergeByte(ordinal: Int): Seq[Byte] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getByte(m.rowIndex)
      }
    }).asInstanceOf[Seq[Byte]]
  }

  def getMergeShort(ordinal: Int): Seq[Short] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getShort(m.rowIndex)
      }
    }).asInstanceOf[Seq[Short]]
  }

  def getMergeInt(ordinal: Int): Seq[Int] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getInt(m.rowIndex)
      }
    }).asInstanceOf[Seq[Int]]
  }

  def getMergeLong(ordinal: Int): Seq[Long] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getLong(m.rowIndex)
      }
    }).asInstanceOf[Seq[Long]]
  }

  def getMergeFloat(ordinal: Int): Seq[Float] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getFloat(m.rowIndex)
      }
    }).asInstanceOf[Seq[Float]]
  }

  def getMergeDouble(ordinal: Int): Seq[Double] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getDouble(m.rowIndex)
      }
    }).asInstanceOf[Seq[Double]]
  }

  def getMergeDecimal(ordinal: Int, precision: Int, scale: Int): Seq[Decimal] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getDecimal(m.rowIndex, precision, scale)
      }
    })
  }

  def getMergeUTF8String(ordinal: Int): Seq[UTF8String] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getUTF8String(m.rowIndex).clone()
      }
    })
  }

  def getMergeBinary(ordinal: Int): Seq[Array[Byte]] = {
    val colIdAndRowId = getIndex(ordinal)
    columns(colIdAndRowId.head.columnVectorIndex).getBinary(colIdAndRowId.head.rowIndex)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getBinary(m.rowIndex)
      }
    })
  }

  def getMergeInterval(ordinal: Int): Seq[CalendarInterval] = {
    val colIdAndRowId = getIndex(ordinal)
    columns(colIdAndRowId.head.columnVectorIndex).getInterval(colIdAndRowId.head.rowIndex)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getInterval(m.rowIndex)
      }
    })
  }

  def getMergeStruct(ordinal: Int, numFields: Int): Seq[InternalRow] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getStruct(m.rowIndex)
      }
    })
  }

  def getMergeArray(ordinal: Int): Seq[ArrayData] = {
    val colIdAndRowId = getIndex(ordinal)
    columns(colIdAndRowId.head.columnVectorIndex).getArray(colIdAndRowId.head.rowIndex)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getArray(m.rowIndex)
      }
    })
  }

  def getMergeMap(ordinal: Int): Seq[MapData] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if (columns(m.columnVectorIndex).isNullAt(m.rowIndex)) {
        null
      } else {
        columns(m.columnVectorIndex).getMap(m.rowIndex)
      }
    })
  }


}
