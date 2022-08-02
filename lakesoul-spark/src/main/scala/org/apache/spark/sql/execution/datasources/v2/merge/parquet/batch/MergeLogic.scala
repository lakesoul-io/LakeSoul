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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.InternalRow.getAccessor
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.{KeyIndex, MergePartitionedFile}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.BufferedIterator

trait MergeLogic {

  def getValueByType(row: InternalRow, fieldIndex: Int, dataType: DataType): Any = {
    dataType match {
      case StringType => if (row.isNullAt(fieldIndex)) null else row.getUTF8String(fieldIndex).clone()
      case IntegerType | DateType => if (row.isNullAt(fieldIndex)) null else row.getInt(fieldIndex)
      case BooleanType => if (row.isNullAt(fieldIndex)) null else row.getBoolean(fieldIndex)
      case ByteType => if (row.isNullAt(fieldIndex)) null else row.getByte(fieldIndex)
      case ShortType => if (row.isNullAt(fieldIndex)) null else row.getShort(fieldIndex)
      case LongType | TimestampType => if (row.isNullAt(fieldIndex)) null else row.getLong(fieldIndex)
      case FloatType => if (row.isNullAt(fieldIndex)) null else row.getFloat(fieldIndex)
      case DoubleType => if (row.isNullAt(fieldIndex)) null else row.getDouble(fieldIndex)
      case BinaryType => if (row.isNullAt(fieldIndex)) null else row.getBinary(fieldIndex)
      case CalendarIntervalType => if (row.isNullAt(fieldIndex)) null else row.getInterval(fieldIndex)
      case t: DecimalType => if (row.isNullAt(fieldIndex)) null else row.getDecimal(fieldIndex, t.precision, t.scale)
      case t: StructType => if (row.isNullAt(fieldIndex)) null else row.getStruct(fieldIndex, t.size)
      case _: ArrayType => if (row.isNullAt(fieldIndex)) null else row.getArray(fieldIndex)
      case _: MapType => if (row.isNullAt(fieldIndex)) null else row.getMap(fieldIndex)
      case u: UserDefinedType[_] => getAccessor(u.sqlType, true)
      case _ => if (row.isNullAt(fieldIndex)) null else row.get(fieldIndex, dataType)
    }
  }

  /** The file readers have been read should be closed at once. */
  def closeReadFileReader(): Unit

}


import scala.collection.JavaConverters._

class MergeSingletonFile(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]) extends MergeLogic {

  //initialize index
  val keyInfoArray: Array[KeyIndex] = filesInfo.head._1.keyInfo.toArray

  val typeArray: Array[DataType] = filesInfo.head._1.fileInfo.map(_.fieldType).toArray

  var temporaryRow: Array[Any] = new Array[Any](filesInfo.head._1.resultSchema.length)
  // get next batch
  var fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)] = MergeUtils.getNextBatch(filesInfo)

  val fileSchema: Seq[String] = filesInfo.head._1.fileInfo.map(_.fieldName)
  val resIndex: Array[Int] = filesInfo.head._1.resultSchema.map(_.fieldName).map(schema => {
    fileSchema.indexOf(schema)
  }).toArray

  val emptyBatch: Boolean = fileSeq.isEmpty
  var temporaryStoreLastRow = false

  var singletonBatch: SingletonFileColumnarBatch = _
  var bufferedIt: BufferedIterator[(InternalRow, Int)] = _

  if (fileSeq.nonEmpty) {
    singletonBatch = initMergeBatch(fileSeq.head, resIndex)
    bufferedIt = fileSeq.head._2.rowIterator().asScala.zipWithIndex.buffered
  }

  var rowId: Int = -1

  def deDuplication(): Boolean = {
    if (emptyBatch) {
      return false
    }
    var lastKey: String = null
    rowId = -1
    while (true) {
      if (bufferedIt.hasNext) {
        val currentRow = bufferedIt.head._1
        if (StringUtils.isEmpty(lastKey)) {
          lastKey = combineKey(currentRow)
          rowId = bufferedIt.head._2
        } else {
          if (combineKey(currentRow).equals(lastKey)) {
            if (temporaryStoreLastRow) temporaryStoreLastRow = false
            rowId = bufferedIt.head._2
          } else {
            return true
          }
        }
        bufferedIt.next()
      } else {
        if (rowId == -1) return false
        val tempRow = getRow()
        resIndex.indices.foreach(i => {
          if (resIndex(i) == -1) {
            temporaryRow(i) = null
          } else {
            temporaryRow(i) = getValueByType(tempRow, i, typeArray(resIndex(i)))
          }
        })
        temporaryStoreLastRow = true
        fileSeq = MergeUtils.getNextBatch(filesInfo)
        if (fileSeq.nonEmpty) {
          bufferedIt = fileSeq.head._2.rowIterator().asScala.zipWithIndex.buffered
          singletonBatch = initMergeBatch(fileSeq.head, resIndex)
        } else {
          return true
        }
      }

    }
    false
  }

  def combineKey(row: InternalRow): String = {
    keyInfoArray.map(keyIndex => {
      row.get(keyIndex.index, keyIndex.keyType).toString
    })
      .reduce(_.concat(_))
  }

  def getRow(): InternalRow = {
    if (temporaryStoreLastRow) {
      val batchLastRow = new GenericInternalRow(temporaryRow.clone())
      temporaryRow.indices.foreach(temporaryRow(_) = null)
      temporaryStoreLastRow = false
      batchLastRow
    } else {
      singletonBatch.getRow(rowId)
    }
  }

  def initMergeBatch(file: (MergePartitionedFile, ColumnarBatch), resIndex: Array[Int]): SingletonFileColumnarBatch = {
    val columnArr =
      resIndex.map(res => {
        if (res == -1) {
          null
        } else {
          file._2.column(res)
        }
      })
    new SingletonFileColumnarBatch(columnArr)
  }

  override def closeReadFileReader(): Unit = {
    filesInfo.foreach(f => f._2.close())
  }

}


