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

package org.apache.spark.sql.execution.datasources.v2.merge

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.DataType

import scala.collection.mutable

/**
  * A part (i.e. "block") of a single file that should be read, along with partition column values
  * that need to be prepended to each row.
  *
  * @param partitionValues value of partition columns to be prepended to each row.
  * @param filePath        URI of the file to read
  * @param start           the beginning offset (in bytes) of the block.
  * @param length          number of bytes to read.
  * @param locations       locality information (list of nodes that have the data).
  */
case class MergePartitionedFile(partitionValues: InternalRow,
                                filePath: String,
                                start: Long,
                                length: Long,
                                qualifiedName: String,
                                rangeKey: String,
                                keyInfo: Seq[KeyIndex], //(hash key index of file, dataType)
                                resultSchema: Seq[FieldInfo], //all result columns name and type
                                fileInfo: Seq[FieldInfo], //file columns name and type
                                writeVersion: Long,
                                rangeVersion: String,
                                fileBucketId: Int, //hash split id
                                @transient locations: Array[String] = Array.empty) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}


//column name and type
case class FieldInfo(fieldName: String, fieldType: DataType)

/**
  * @param index   hash key index in file result schema, such as fileResultScheme:[a,k,b], keyIndex is 1
  * @param keyType DataType of key field
  */
case class KeyIndex(index: Int, keyType: DataType)


/**
  * A collection of file blocks that should be read as a single task
  * (possibly from multiple partitioned directories).
  */
case class MergeFilePartition(index: Int, files: Array[Array[MergePartitionedFile]], isSingleFile: Boolean)
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach(f => f.foreach(file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }))

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}













