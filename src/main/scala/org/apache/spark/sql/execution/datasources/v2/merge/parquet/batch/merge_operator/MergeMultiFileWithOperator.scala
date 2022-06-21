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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.{FieldInfo, KeyIndex, MergePartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.{MergeLogic, MergeOperatorColumnarBatchRow, MergeOptimizeHeap, MergeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @param filesInfo (file, columnar reader of file)
  */
class MergeMultiFileWithOperator(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])],
                                 mergeOperatorInfo: Map[String, MergeOperator[Any]],
                                 defaultMergeOp: MergeOperator[Any]) extends MergeLogic {

  //all result columns name and type
  val resultSchema: Seq[FieldInfo] = filesInfo.head._1.resultSchema
  var temporaryStoreLastRow = false

  /**
    * Get the key information of each merge file And build it into a Map.
    * Map[version->keyInfo]
    * keyInfo:Array[(Int,DataType)]
    */
  var versionKeyInfoMap: Map[Long, Array[KeyIndex]] = filesInfo.map(info => {
    info._1.writeVersion -> info._1.keyInfo.toArray
  }).toMap

  /**
    * The corresponding index of each field in mergeRow.
    * Map:[fieldName->index] , index: Indicates the position of the field after the merge
    */
  val fieldIndexMap: Map[String, Int] = resultSchema.zipWithIndex.map(m => {
    m._1.fieldName -> m._2
  }).toMap

  /**
    * Replace the field name with the index.
    * The order of the array elements represents the order of the entire mergeRow elements。
    */
  val indexTypeArray: Seq[FieldIndex] =
    resultSchema.map(fieldInfo =>
      FieldIndex(
        fieldIndexMap(fieldInfo.fieldName),
        fieldInfo.fieldType))

  /**
    * Get the file information of each merge file And build it into a Map.
    * Map[writeVersion -> Array(FieldIndex)]
    */
  val versionFileInfoMap: Map[Long, Array[FieldIndex]] = filesInfo.map(t => {
   t._1.writeVersion->
      t._1.fileInfo.map(fieldInfo => FieldIndex(fieldIndexMap(fieldInfo.fieldName), fieldInfo.fieldType)).toArray
  }).toMap

  //every column has an ArrayBuffer to keep all the value index of the same primary key
  private val resultIndex = new Array[ArrayBuffer[MergeColumnIndex]](resultSchema.length)
  MergeUtils.intBatchIndexMerge(resultIndex)

//  private val defaultMergeOp = if(defaultMergeOpInfo == null) new DefaultMergeOp[Any] else defaultMergeOpInfo
  private val mergeOp: Seq[MergeOperator[Any]] = resultSchema.map(fieldInfo => {
    if (mergeOperatorInfo.contains(fieldInfo.fieldName)) {
      mergeOperatorInfo(fieldInfo.fieldName)
    } else {
      defaultMergeOp
    }
  })


  //Initialize the last piece of data when the batch is switched
  var temporaryRow: Array[ArrayBuffer[Any]] = new Array[ArrayBuffer[Any]](resultSchema.length)
  MergeUtils.initTemporaryRow(temporaryRow)


  //get next batch
  val fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)] = MergeUtils.getNextBatch(filesInfo)
  val mergeHeap = new MergeOptimizeHeap(versionKeyInfoMap)
  mergeHeap.enqueueBySeq(MergeUtils.toBufferedIterator(fileSeq))

  /** initialize mergeColumnIndexMap and mergeColumnarBatch object */
  //Map[Long, Array[Int]] => Map(write_version, columns_index_of_this_file_reader_in_total_index_array)
  val mergeColumnIndexMap: mutable.Map[Long, Array[Int]] = mutable.Map[Long, Array[Int]]()
  //initialize mergeColumnIndexMap, create column index array for all files, ordered by write version
  MergeUtils.initMergeBatchAndMergeIndex(fileSeq, mergeColumnIndexMap)
  //initialize mergeColumnarBatch object
  val mergeColumnarBatch: MergeColumnarBatchNew = MergeUtils.initMergeBatchNew(fileSeq, mergeOp, indexTypeArray)


  var lastVersion: Long = -5

  def getRowByProxyMergeBatch(): InternalRow = {
    mergeColumnarBatch.getRow(resultIndex)
  }

  def isTemporaryRow(): Boolean = temporaryStoreLastRow


  def getTemporaryRow(): Array[Any] = {
    temporaryRow.zipWithIndex.map(m => {
      val index = m._2
      val dt = resultSchema(index).fieldType
      getStoredMergeRowByType(temporaryRow(index), mergeOp(index), dt)
    })
  }

  def setTemporaryRowFalse(): Unit = {
    temporaryStoreLastRow = false
  }


  def isHeapEmpty: Boolean = mergeHeap.isEmpty

  def merge(): Unit = {

    MergeUtils.resetBatchIndexMerge(resultIndex)
    var lastKey: String = null
    while (mergeHeap.nonEmpty) {
      val currentFile = mergeHeap.dequeue()
      val currentRowAndLineId = currentFile._2.head
      val currentVersion = currentFile._1

      if (StringUtils.isEmpty(lastKey)) {
        lastKey = combineKey(currentVersion, currentRowAndLineId._1)
      } else {
        if (!combineKey(currentVersion, currentRowAndLineId._1).equals(lastKey)) {
          mergeHeap.enqueue(currentFile)
          lastKey = null
          lastVersion = -5
          // End the merge
          return
        }
      }

      currentFile._2.next()
      if (currentFile._2.hasNext) {
        //calculate the field index in File And fill into the MergeBatch Object
        //if previous row has the BatchLastRow, store row data in temp row, else add index into resultIndex
        if (temporaryStoreLastRow) {
          storeRow(currentVersion, currentRowAndLineId._1)
        } else {
          fillMergeBatchIndex(currentRowAndLineId, currentVersion)
        }

        mergeHeap.enqueue(currentFile)
      } else {
        //if it is the first BatchLastRow, take the rows in resultIndex into temporaryRow
        if (!temporaryStoreLastRow) {
          MergeUtils.resetTemporaryRow(temporaryRow)

          //add the rows stored in resultIndex to temporaryRow
          putIndexedRowToTemporaryRow()
          temporaryStoreLastRow = true
        }
        //store current row to temporaryRow
        storeRow(currentVersion, currentRowAndLineId._1)

        val fileInfo = filesInfo.filter(t => t._1.writeVersion.equals(currentVersion))
        val nextBatches = MergeUtils.getNextBatch(fileInfo)

        if (nextBatches.nonEmpty) {
          val bufferIt = MergeUtils.toBufferedIterator(nextBatches)
          mergeHeap.enqueue(bufferIt.head)
        } else {
          mergeHeap.poll()
        }
      }
      lastVersion = currentVersion
    }

  }

  def putIndexedRowToTemporaryRow(): Unit = {
    if (resultIndex.head.nonEmpty) {
      val row = mergeColumnarBatch.getMergeRow(resultIndex)
      storeRowByMergeBatch(row)

      MergeUtils.resetBatchIndexMerge(resultIndex)
    }
  }

  def storeRowByMergeBatch(row: MergeOperatorColumnarBatchRow): Unit = {
    for (i <- resultIndex.indices) {
      if (resultIndex(i).nonEmpty) {
        val fieldIndex = indexTypeArray(i)
        getMergeValuesByType(row, i, fieldIndex.filedType).foreach(temporaryRow(i) += _)
      }

    }
  }

  def storeRow(version: Long, row: InternalRow): Unit = {
    for (i <- versionFileInfoMap(version).indices) {
      val fieldIndex = versionFileInfoMap(version)(i)
      if (lastVersion == version) {
        //it has duplicate data in one file, we just store the last one
        //seq.init() == list.removeLast()
        temporaryRow(fieldIndex.index) = temporaryRow(fieldIndex.index).init += getValueByType(row, i, fieldIndex.filedType)
      } else {
        temporaryRow(fieldIndex.index) += getValueByType(row, i, fieldIndex.filedType)
      }
    }
  }

  def fillMergeBatchIndex(rowAndId: (InternalRow, Int), writerVersion: Long): Unit = {
    //get fileInfo ,Array[0,3,4] -> a,d,e
    val columns = versionFileInfoMap(writerVersion)
    //get field Index for MergeBatch Object
    val mergeBatchIndex = mergeColumnIndexMap(writerVersion)

    columns.indices.foreach(i => {
      if (lastVersion == writerVersion) {
        //it has duplicate data in one file, we just store the last one
        //seq.init() == list.removeLast()
        resultIndex(columns(i).index) = resultIndex(columns(i).index).init += MergeColumnIndex(mergeBatchIndex(i), rowAndId._2)
      } else {
        resultIndex(columns(i).index) += MergeColumnIndex(mergeBatchIndex(i), rowAndId._2)
      }
    })
  }

  def combineKey(version: Long, row: InternalRow): String = {
    versionKeyInfoMap(version)
      .map(key_type => {
        row.get(key_type.index, key_type.keyType).toString
      })
      .reduce(_.concat(_))
  }

  def getMergeValuesByType(row: MergeOperatorColumnarBatchRow, fieldIndex: Int, dataType: DataType): Seq[Any] = {
    dataType match {
      case StringType => row.getMergeUTF8String(fieldIndex)
      case IntegerType | DateType => row.getMergeInt(fieldIndex)
      case BooleanType => row.getMergeBoolean(fieldIndex)
      case ByteType => row.getMergeBoolean(fieldIndex)
      case ShortType => row.getMergeShort(fieldIndex)
      case LongType | TimestampType => row.getMergeLong(fieldIndex)
      case FloatType => row.getMergeFloat(fieldIndex)
      case DoubleType => row.getMergeDouble(fieldIndex)
      case BinaryType => row.getMergeBinary(fieldIndex)
      case CalendarIntervalType => row.getMergeInterval(fieldIndex)
      case t: DecimalType => row.getMergeDecimal(fieldIndex, t.precision, t.scale)
      case t: StructType => row.getMergeStruct(fieldIndex, t.size)
      case _: ArrayType => row.getMergeArray(fieldIndex)
      case _: MapType => row.getMergeMap(fieldIndex)
      case o => throw new UnsupportedOperationException(s"LakeSoul MergeOperator don't support type ${o.typeName}")
    }
  }

  def getStoredMergeRowByType(row: ArrayBuffer[Any], mergeClass: MergeOperator[Any], dataType: DataType): Any = {
    if (row.isEmpty) {
      return null
    }
    dataType match {
      case StringType => UTF8String.fromString(mergeClass.asInstanceOf[MergeOperator[String]].mergeData(row.asInstanceOf[Seq[UTF8String]].map(r => if (r == null) null else r.toString)))
      case IntegerType | DateType => mergeClass.asInstanceOf[MergeOperator[Int]].mergeData(row.asInstanceOf[Seq[Int]])
      case BooleanType => mergeClass.asInstanceOf[MergeOperator[Boolean]].mergeData(row.asInstanceOf[Seq[Boolean]])
      case ByteType => mergeClass.asInstanceOf[MergeOperator[Byte]].mergeData(row.asInstanceOf[Seq[Byte]])
      case ShortType => mergeClass.asInstanceOf[MergeOperator[Short]].mergeData(row.asInstanceOf[Seq[Short]])
      case LongType | TimestampType => mergeClass.asInstanceOf[MergeOperator[Long]].mergeData(row.asInstanceOf[Seq[Long]])
      case FloatType => mergeClass.asInstanceOf[MergeOperator[Float]].mergeData(row.asInstanceOf[Seq[Float]])
      case DoubleType => mergeClass.asInstanceOf[MergeOperator[Double]].mergeData(row.asInstanceOf[Seq[Double]])
      case BinaryType => mergeClass.asInstanceOf[MergeOperator[Array[Byte]]].mergeData(row.asInstanceOf[Seq[Array[Byte]]])
      case CalendarIntervalType => mergeClass.asInstanceOf[MergeOperator[CalendarInterval]].mergeData(row.asInstanceOf[Seq[CalendarInterval]])
      case t: DecimalType => mergeClass.asInstanceOf[MergeOperator[Decimal]].mergeData(row.asInstanceOf[Seq[Decimal]])
      case t: StructType => mergeClass.asInstanceOf[MergeOperator[InternalRow]].mergeData(row.asInstanceOf[Seq[InternalRow]])
      case _: ArrayType => mergeClass.asInstanceOf[MergeOperator[ArrayData]].mergeData(row.asInstanceOf[Seq[ArrayData]])
      case _: MapType => mergeClass.asInstanceOf[MergeOperator[MapData]].mergeData(row.asInstanceOf[Seq[MapData]])
      case o => throw new UnsupportedOperationException(s"LakeSoul MergeOperator don't support type ${o.typeName}")
    }
  }

  override def closeReadFileReader(): Unit = {
    filesInfo.foreach(f => f._2.close())
  }


}


/**
  * @param index     filed index in result schema, such as ResultScheme:[a,k,b], field a is 0, k is 1,b is 2
  * @param filedType DataType of key field
  */
case class FieldIndex(index: Int, filedType: DataType)

/**
  * Construct an index to locate a value in column vector array of all files.
  *
  * @param columnVectorIndex index of column vector array of all files
  * @param rowIndex          index of row in a column vector
  */
case class MergeColumnIndex(columnVectorIndex: Int, rowIndex: Int)