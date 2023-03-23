/*
 *   Copyright [2022] [DMetaSoul Team]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.sink

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Locale

class MysqlSink(extraSinkParms: ExtraSinkParms) extends ExtraSink {
  override def save(): Unit = {
    val processType = extraSinkParms.processType.toLowerCase(Locale.ROOT)
    processType match {
      case processType if processType == ProcessType.BATCH.toString => saveBatch()
      case processType if processType == ProcessType.STREAM.toString => saveStream()
      case _ => throw new IllegalArgumentException(s"Unsupported commit sinkType '${extraSinkParms.sinkType}'. ")
    }
  }

  def saveBatch(): Unit = {
    val writer = extraSinkParms.df.write.format("jdbc").option("url", extraSinkParms.url)
      .option("dbtable", extraSinkParms.database + "." + extraSinkParms.tableName)
      .option("user", extraSinkParms.user)
      .option("password", extraSinkParms.password)
      .mode(extraSinkParms.saveMode)
    if (extraSinkParms.saveMode == SaveMode.Overwrite)
      writer.mode(SaveMode.Overwrite).save()
    else writer.save()
  }

  def saveStream(): Unit = {
    extraSinkParms.df.writeStream
      .outputMode("complete")
      .foreachBatch {
        (batchDF: DataFrame, _: Long) => {
          batchDF.write
            .format("jdbc")
            .mode("overwrite")
            .option("url", extraSinkParms.url)
            .option("dbtable", extraSinkParms.database + "." + extraSinkParms.tableName)
            .option("user", extraSinkParms.user)
            .option("password", extraSinkParms.password)
            .save()
        }
      }
      .start()
      .awaitTermination()
  }
}
