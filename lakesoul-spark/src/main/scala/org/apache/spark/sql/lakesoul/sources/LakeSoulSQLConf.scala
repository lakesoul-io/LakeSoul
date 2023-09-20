// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.sources

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.sql.internal.SQLConf

object LakeSoulSQLConf {

  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.dmetasoul.lakesoul.$key")

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

  // drop table await time
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

  val PART_MERGE_FILE_MINIMUM_NUM: ConfigEntry[Int] =
    buildConf("part.merge.file.minimum.num")
      .doc(
        """
          |If delta file num more than this count, we will check for part merge.
        """.stripMargin)
      .intConf
      .createWithDefault(5)

  val SNAPSHOT_CACHE_EXPIRE: ConfigEntry[Int] =
    buildConf("snapshot.cache.expire.seconds")
      .doc(
        """
          |Expire snapshot cache in seconds
        """.stripMargin)
      .intConf
      .createWithDefault(10)

  val NATIVE_IO_ENABLE: ConfigEntry[Boolean] =
    buildConf("native.io.enable")
      .doc(
        """
           |If ture, NativeIO would be enabled for both read and write
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)


  val NATIVE_IO_PREFETCHER_BUFFER_SIZE: ConfigEntry[Int] =
    buildConf("native.io.prefetch.buffer.size")
      .doc(
        """
           |If NATIVE_IO_ENABLE=true, NATIVE_IO_PREFETCHER_BUFFER_SIZE of batches will be buffered while native-io prefetching
        """.stripMargin)
      .intConf
      .createWithDefault(1)

  val NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE: ConfigEntry[Int] =
    buildConf("native.io.write.max.rowgroup.size")
      .doc(
        """
          |If NATIVE_IO_ENABLE=true, NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE of rows will be used to write a new row group
      """.stripMargin)
      .intConf
      .createWithDefault(250000)

  val NATIVE_IO_THREAD_NUM: ConfigEntry[Int] =
    buildConf("native.io.thread.num")
      .doc(
        """
          |If NATIVE_IO_ENABLE=true, tokio::runtime::Runtime will be build with NATIVE_IO_THREAD_NUM thread_num
        """.stripMargin)
      .intConf
      .createWithDefault(2)

  val NATIVE_IO_READER_AWAIT_TIMEOUT: ConfigEntry[Int] =
    buildConf("native.io.await.timeout")
      .doc(
        """
          |If NATIVE_IO_ENABLE=true, timeout for each iterate will be set to NATIVE_IO_READER_AWAIT_TIMEOUT mills
        """.stripMargin)
      .intConf
      .createWithDefault(10000)
}
