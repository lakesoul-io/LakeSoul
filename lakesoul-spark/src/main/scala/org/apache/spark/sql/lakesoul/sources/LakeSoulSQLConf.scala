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

package org.apache.spark.sql.lakesoul.sources

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.sql.internal.SQLConf

object LakeSoulSQLConf {

  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.dmetasoul.lakesoul.$key")

  def buildStaticConf(key: String): ConfigBuilder =
    SQLConf.buildStaticConf(s"spark.dmetasoul.lakesoul.$key")

  val SCHEMA_AUTO_MIGRATE: ConfigEntry[Boolean] =
    buildConf("schema.autoMerge.enabled")
      .doc("If true, enables schema merging on appends and on overwrites.")
      .booleanConf
      .createWithDefault(false)

  val USE_DELTA_FILE: ConfigEntry[Boolean] =
    buildConf("deltaFile.enabled")
      .doc("If true, enables delta files on specific scene(e.g. upsert).")
      .booleanConf
      .createWithDefault(true)

  val MAX_DELTA_FILE_NUM: ConfigEntry[Int] =
    buildConf("deltaFile.max.num")
      .doc("Maximum delta files allowed, default is 5.")
      .intConf
      .createWithDefault(5)

  val COMPACTION_TIME: ConfigEntry[Long] =
    buildConf("compaction.interval")
      .doc("If the last update time exceeds the set interval, compaction will be triggered, default is 12 hours.")
      .longConf
      .createWithDefault(12 * 60 * 60 * 1000L)

  //default meta database name
  val META_DATABASE_NAME: ConfigEntry[String] =
    buildConf("meta.database.name")
      .doc(
        """
          |Default database of meta tables in PG DB.
          |User should not change it unless you know what you are going to do.
        """.stripMargin)
      .stringConf
      .createWithDefault("lakesoul_meta")


  val META_HOST: ConfigEntry[String] =
    buildConf("meta.host")
      .doc(
        """
          |Contact point to connect to the PG DB.
          |A comma separated list may also be used.("127.0.0.1,192.168.0.1")
        """.stripMargin)
      .stringConf
      .createWithDefault("localhost")

  val META_PORT: ConfigEntry[Int] =
    buildConf("meta.port")
      .doc("PG DB native connection port.")
      .intConf
      .createWithDefault(5433)

  val META_USERNAME: ConfigEntry[String] =
    buildConf("meta.username")
      .doc(
        """
          |PG DB username, default is `yugabyte`.
        """.stripMargin)
      .stringConf
      .createWithDefault("yugabyte")

  val META_PASSWORD: ConfigEntry[String] =
    buildConf("meta.password")
      .doc(
        """
          |PG DB password, default is `yugabyte`.
        """.stripMargin)
      .stringConf
      .createWithDefault("yugabyte")

  val META_STREAMING_INFO_TIMEOUT: ConfigEntry[Long] =
    buildConf("meta.streaming_info.timeout")
      .doc(
        """
          |The maximum timeout for streaming info.
          |This parameter will only be used in Cleanup operation.
        """.stripMargin)
      .longConf
      .createWithDefault(12 * 60 * 60 * 1000L)

  val META_MAX_COMMIT_ATTEMPTS: ConfigEntry[Int] =
    buildConf("meta.commit.max.attempts")
      .doc(
        """
          |The maximum times for a job attempts to commit.
        """.stripMargin)
      .intConf
      .createWithDefault(5)

  //dorp table await time
  val DROP_TABLE_WAIT_SECONDS: ConfigEntry[Int] =
    buildConf("drop.table.wait.seconds")
      .doc(
        """
          |When dropping table or partition, we need wait a few seconds for the other commits to be completed.
        """.stripMargin)
      .intConf
      .createWithDefault(1)

  val ALLOW_FULL_TABLE_UPSERT: ConfigEntry[Boolean] =
    buildConf("full.partitioned.table.scan.enabled")
      .doc("If true, enables full table scan when upsert.")
      .booleanConf
      .createWithDefault(false)

  val PARQUET_BLOCK_SIZE: ConfigEntry[Long] =
    buildConf("parquet.block.size")
      .doc("Parquet block size.")
      .longConf
      .createWithDefault(32 * 1024 * 1024L)


  val PARQUET_COMPRESSION: ConfigEntry[String] =
    buildConf("parquet.compression")
      .doc(
        """
          |Parquet compression type.
        """.stripMargin)
      .stringConf
      .createWithDefault("snappy")

  val PARQUET_COMPRESSION_ENABLE: ConfigEntry[Boolean] =
    buildConf("parquet.compression.enable")
      .doc(
        """
          |Whether to use parquet compression.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val BUCKET_SCAN_MULTI_PARTITION_ENABLE: ConfigEntry[Boolean] =
    buildConf("bucket.scan.multi.partition.enable")
      .doc(
        """
          |Hash partitioned table can read multi-partition data partitioned by hash keys without shuffle,
          |this parameter controls whether this feature is enabled or not.
          |Using this feature, the parallelism will equal to hash bucket num.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val PART_MERGE_ENABLE: ConfigEntry[Boolean] =
    buildConf("part.merge.enable")
      .doc(
        """
          |If true, part files merging will be used to avoid OOM when it has too many delta files.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val PART_MERGE_COMPACTION_COMMIT_ENABLE: ConfigEntry[Boolean] =
    buildConf("part.merge.compaction.commit.enable")
      .doc(
        """
          |If true, it will commit the compacted files into meta store, and the later reader can read faster.
          |Note that if you read a column by self-defined merge operator, the compacted result should also use
          |this merge operator, make sure that the result is expected or disable compaction commit.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val PART_MERGE_FILE_MINIMUM_NUM: ConfigEntry[Int] =
    buildConf("part.merge.file.minimum.num")
      .doc(
        """
          |If delta file num more than this count, we will check for part merge.
        """.stripMargin)
      .intConf
      .createWithDefault(5)


  val PART_MERGE_FILE_SIZE_FACTOR: ConfigEntry[Double] =
    buildConf("part.merge.file.size.factor")
      .doc(
        """
          |File size factor to calculate part merge max size.
          |Expression: PART_MERGE_FILE_MINIMUM_NUM * PART_MERGE_FILE_SIZE_FACTOR * 128M
        """.stripMargin)
      .doubleConf
      .createWithDefault(0.1)
}
