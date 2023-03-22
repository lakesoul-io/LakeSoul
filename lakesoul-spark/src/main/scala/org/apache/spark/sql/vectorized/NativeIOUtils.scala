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

package org.apache.spark.sql.vectorized

import org.apache.arrow.lakesoul.io.NativeIOBase
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

class NativeIOUtils {
}

class NativeIOOptions(val s3Bucket: String,
                      val s3Ak: String,
                      val s3Sk: String,
                      val s3Endpoint: String,
                      val s3Region: String,
                      val fsUser: String,
                      val defaultFS: String
                     )

object NativeIOUtils{

  def asArrayColumnVector(vectorSchemaRoot: VectorSchemaRoot): Array[ColumnVector] = {
    asScalaIteratorConverter(vectorSchemaRoot.getFieldVectors.iterator())
      .asScala
      .toSeq
      .map(vector => {
        asColumnVector(vector)
      })
      .toArray
  }

  private def asArrowColumnVector(vector: ValueVector): ArrowColumnVector = {
    new ArrowColumnVector(vector)
  }

  private def asColumnVector(vector: ValueVector): ColumnVector ={
    asArrowColumnVector(vector).asInstanceOf[ColumnVector]
  }

  def getNativeIOOptions(taskAttemptContext: TaskAttemptContext, file: Path): NativeIOOptions = {
    val user = Utils.getCurrentUserName
    var defaultFS = taskAttemptContext.getConfiguration.get("fs.defaultFS")
    if (defaultFS == null) defaultFS = taskAttemptContext.getConfiguration.get("fs.default.name")
    val fileSystem = file.getFileSystem(taskAttemptContext.getConfiguration)
    fileSystem match {
      case s3aFileSystem: S3AFileSystem =>
        val awsS3Bucket = s3aFileSystem.getBucket
        val s3aEndpoint = taskAttemptContext.getConfiguration.get("fs.s3a.endpoint")
        val s3aRegion = taskAttemptContext.getConfiguration.get("fs.s3a.endpoint.region")
        val s3aAccessKey = taskAttemptContext.getConfiguration.get("fs.s3a.access.key")
        val s3aSecretKey = taskAttemptContext.getConfiguration.get("fs.s3a.secret.key")
        new NativeIOOptions(awsS3Bucket, s3aAccessKey, s3aSecretKey, s3aEndpoint, s3aRegion, user, defaultFS)
      case _ => new NativeIOOptions(null, null, null, null, null, user, defaultFS)
    }
  }

  def setNativeIOOptions(nativeIO: NativeIOBase, options: NativeIOOptions): Unit = {
    nativeIO.setObjectStoreOptions(
      options.s3Ak,
      options.s3Sk,
      options.s3Region,
      options.s3Bucket,
      options.s3Endpoint,
      options.fsUser,
      options.defaultFS
    )
  }
}
