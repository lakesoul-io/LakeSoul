// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.{
  DataFileInfo,
  DataOperation,
  SparkMetaVersion
}
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.arrow.{CompactBucketIO, CompressDataFileInfo}
import scala.collection.JavaConverters._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{
  LakeSoulSQLCommandTest,
  LakeSoulTestSparkSession
}
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TableInfo}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class L1RollingCompactionSuite
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach
    with LakeSoulSQLCommandTest {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.network.timeout", "10000000")
      .set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .set(
        "spark.sql.extensions",
        "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension"
      )
      .set(LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key, "4")
      .set(
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key,
        "100MB"
      )
      .set(LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key, "256MB")
      .set(LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_ENABLED.key, "true")
      .set(LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_TABLES.key, "")
  }

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set(
      "spark.sql.catalog.lakesoul",
      classOf[LakeSoulCatalog].getName
    )
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  import testImplicits._

  // Basic cases keep L1 as the maximum level; promotion cases explicitly enable L2.
  private val simpleOptions = Map(
    LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_ENABLED.key -> "true",
    LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_TABLES.key -> "",
    LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "4",
    LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "100MB",
    LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> "256MB",
    LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1"
  )

  private def writeBatch(path: String, version: Int): Unit = {
    val rows = (1 to 16)
      .map(id => (id, s"v$version-$id"))
      .toDF("id", "value")
      .coalesce(1)
    if (version == 0)
      rows.write
        .format("lakesoul")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "1")
        .save(path)
    else LakeSoulTable.forPath(path).upsert(rows)
    verifyBatch(path, version)
  }

  private def verifyBatch(path: String, version: Int): Unit = {
    LakeSoulTable.uncached(path)
    checkAnswer(
      LakeSoulTable.forPath(path).toDF.select("id", "value"),
      (1 to 16).map(id => Row(id, s"v$version-$id"))
    )
  }

  test(
    "three L0 files wait and the fourth merges below promotion size back to L0"
  ) {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withCompactionOptions(simpleOptions) {
        (0 until 3).foreach(v => writeBatch(path, v))
        val before = getFileList(path).map(_.path).toSet
        assert(before.size == 3)
        LakeSoulTable.forPath(path).newCompaction()
        assert(getFileList(path).map(_.path).toSet == before)
        verifyBatch(path, 2)
        writeBatch(path, 3)
        LakeSoulTable.forPath(path).newCompaction()
        val merged = getFileList(path)
        assert(merged.length == 1)
        assert(!merged.head.path.contains("/compactdir"))
        assert(!before.contains(merged.head.path))
        verifyBatch(path, 3)
        // The merged L0 remains usable with later writes and another compaction.
        (4 to 6).foreach(v => writeBatch(path, v))
        LakeSoulTable.forPath(path).newCompaction()
        assert(getFileList(path).length == 1)
        verifyBatch(path, 6)
      }
    }
  }

  test(
    "L0 reader limit one retains files and later rolling preserves last-write-wins"
  ) {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withCompactionOptions(simpleOptions) {
        (0 to 3).foreach(v => writeBatch(path, v))
        val before = getFileList(path).map(_.path).toSet
        // A one-reader budget cannot combine files; do not rewrite singletons.
        // Later two-reader rolling must still preserve the original key order.
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(1))
        val after = getFileList(path)
        assert(after.length == 4)
        assert(after.forall(f => !f.path.contains("/compactdir")))
        assert(after.map(_.path).toSet == before)
        verifyBatch(path, 3)
        writeBatch(path, 4)
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(2))
        assert(getFileList(path).length == 1)
        verifyBatch(path, 4)
      }
    }
  }

  test("L0 byte budgets bound real input files before rolling") {
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      (0 until 4).foreach { version =>
        data.write(
          Seq(
            (1, version, s"latest-$version"),
            (100 + version, version, s"unique-$version")
          )
        )
      }
      val original = data.files
      val input = original
        .map(f =>
          new CompressDataFileInfo(
            f.path,
            f.size,
            f.file_exist_cols,
            f.modification_time
          )
        )
        .toList
        .asJava
      val conf = spark.sessionState.newHadoopConf()
      simpleOptions.foreach { case (key, value) => conf.set(key, value) }
      conf.set(
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key,
        "10MB"
      )
      conf.unset("max_file_size")
      val info = getTableInfo(data.path)
      val inputs =
        scala.collection.mutable.ArrayBuffer.empty[Seq[CompressDataFileInfo]]
      val written =
        scala.collection.mutable.ArrayBuffer.empty[CompressDataFileInfo]
      def compact(budget: Long) = {
        val io = new CompactBucketIO(
          conf,
          input,
          info,
          info.table_path.toString,
          "-5",
          1,
          3,
          budget,
          false,
          0L,
          "parquet"
        ) {
          override protected def mergeSmallFileBatch(
              files: java.util.List[CompressDataFileInfo],
              level: Int
          ): java.util.List[CompressDataFileInfo] = {
            assert(level == 0)
            assert(files.size() <= 3)
            assert(files.asScala.map(_.getFileSize).sum <= budget)
            inputs += files.asScala.toVector
            val generated = super.mergeSmallFileBatch(files, level)
            assert(generated.size() == 1)
            written += generated.get(0)
            generated
          }
        }
        try io.startCompactTask()
        finally io.close()
      }
      // This real-size budget cannot accommodate any two of the input files.
      val limited = compact(original.map(_.size).min)
      assertDiscardPathsAreSafe(limited)
      assert(inputs.isEmpty)
      assert(
        limited.get("-5").asScala.map(_.getFilePath).toSeq == original
          .map(_.path)
          .toSeq
      )
      assert(limited.get(CompactBucketIO.DISCARD_FILE_LIST_KEY).isEmpty())

      val budget = 1024L * 1024
      assert(original.map(_.size).sum < budget)
      val rolled = compact(budget)
      assertDiscardPathsAreSafe(rolled)
      assert(inputs.map(_.size).toSeq == Seq(3, 2))
      assert(
        inputs.head.map(_.getFilePath) == original.take(3).map(_.path).toSeq
      )
      assert(inputs(1).head.getFilePath == written.head.getFilePath)
      assert(
        inputs(1).tail.map(_.getFilePath) == original.drop(3).map(_.path).toSeq
      )
      val outputs = rolled.get("-5").asScala.toSeq
      assert(outputs.size == 1 && outputs.head.getFileSize < 10L * 1024 * 1024)
      assert(!outputs.head.getFilePath.contains("/compactdir"))
      assert(
        rolled
          .get(CompactBucketIO.DISCARD_FILE_LIST_KEY)
          .asScala
          .exists(_.getFilePath == written.head.getFilePath)
      )
      checkAnswer(
        spark.read
          .parquet(outputs.head.getFilePath)
          .select("id", "version", "payload"),
        Seq(Row(1, 3, "latest-3")) ++
          (0 until 4).map(v => Row(100 + v, v, s"unique-$v"))
      )
      data.verify()
    }
  }

  test("L0 rolls real files over six rounds to a fixed 10MB promotion target") {
    checkRealRolling(level = 0)
  }

  test(
    "L1 rolls real files over six rounds to 10MB and preserves CDC through L2"
  ) {
    checkRealRolling(level = 1)
  }

  private def checkRealRolling(level: Int): Unit = {
    val target = 10L * 1024 * 1024
    val l0Target = if (level == 0) target else 2L * 1024 * 1024
    val rowsPerBatch = 384
    val cdc = level == 1
    val completedLevel = level + 1
    def atLevel(file: DataFileInfo, selected: Int): Boolean = {
      if (selected == 0) !file.path.contains("/compactdir")
      else file.path.contains(s"/compactdir$selected/")
    }
    val options = simpleOptions ++ Map(
      LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key ->
        (if (level == 0) "10MB" else "2MB"),
      LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> "10MB",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> completedLevel.toString,
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "1000",
      LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key -> "1GB",
      LakeSoulSQLConf.NATIVE_IO_PHYSICAL_FORMAT.key -> "parquet",
      "max_file_size" -> (64L * 1024 * 1024).toString
    )
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath, cdc = cdc)
      var growingRounds = 0
      var promotionRounds = 0
      withCompactionOptions(options) {
        // 24 writes of about 1.5MB raw payload each. All sizes used by
        // compaction come from actual files; no size metadata is overridden.
        (0 until 6).foreach { round =>
          val before = if (round == 0) Array.empty[DataFileInfo] else data.files
          val oldSmall = before.filter(file => atLevel(file, level))
          val oldCompleted =
            before.filter(file => atLevel(file, completedLevel))
          val oldCompletedPaths = oldCompleted.map(_.path).toSet
          val updateOnly = round == 2 || round == 5
          (0 until 4).foreach { batch =>
            val version = round * 4 + batch
            val first =
              1000 + (if (updateOnly) batch else version) * rowsPerBatch
            val rows = (first until first + rowsPerBatch).map { id =>
              (id, version, payload(id + version * 1000003, 4096))
            }
            val special = if (version == 0) {
              Seq((2, version, "persistent"), (3, version, "untouched"))
            } else if (cdc && version == 5) {
              Seq((2, version, "deleted"))
            } else if (cdc && version == 17) {
              Seq((2, version, "reinserted"))
            } else Seq.empty[(Int, Int, String)]
            data.write(
              rows ++ Seq((1, version, s"latest-$version")) ++ special,
              if (cdc && version == 5) Set(2) else Set.empty[Int],
              verifyAfterWrite = false
            )
          }
          data.verify()
          val inputSnapshot = data.files
          val l0Inputs = inputSnapshot.filter(file => atLevel(file, 0))
          assert(
            l0Inputs.nonEmpty && l0Inputs.forall(_.size < l0Target),
            s"Round $round must start with genuinely small L0 files"
          )

          data.compact(Some(3))
          val after = data.files
          val small = after.filter(file => atLevel(file, level))
          val completed = after.filter(file => atLevel(file, completedLevel))
          val completedPaths = completed.map(_.path).toSet
          assert(
            small.length <= 1,
            s"Round $round left multiple unfinished L$level files"
          )
          assert(small.forall(_.size < target))
          assert(completed.forall(_.size >= target))
          assert(
            after.filter(file => atLevel(file, 0)).forall(_.size < l0Target)
          )
          assert(
            oldCompletedPaths.subsetOf(completedPaths),
            "Completed files must stop participating in regular rolling"
          )
          assert(
            completed.take(oldCompleted.length).map(_.path).toSeq ==
              oldCompleted.map(_.path).toSeq,
            "Previously completed files must retain their priority order"
          )
          assert(
            after.take(completed.length).map(_.path).toSeq == completed
              .map(_.path)
              .toSeq,
            "Completed higher-level files must precede the unfinished lower-level tail"
          )
          if ((completedPaths -- oldCompletedPaths).nonEmpty)
            promotionRounds += 1
          if (
            oldSmall.nonEmpty && small.nonEmpty
            && oldSmall.head.path != small.head.path
            && small.head.size > oldSmall.head.size
          ) growingRounds += 1

          if (round == 2) {
            // Rewriting existing keys crosses the INPUT target, but cannot
            // increase the logical data size. The undersized output must keep
            // rolling through all remaining inputs rather than create tails.
            assert(oldSmall.length == 1 && oldSmall.head.size >= target * 4 / 5)
            assert(small.length == 1 && small.head.size < target)
            assert(small.head.path != oldSmall.head.path)
            assert(completedPaths == oldCompletedPaths)
          }
          inputSnapshot.foreach { file =>
            val path = new Path(file.path)
            assert(
              path
                .getFileSystem(spark.sessionState.newHadoopConf())
                .exists(path),
              "Compaction must keep the previous snapshot's paths readable"
            )
          }
          val stablePaths = after.map(_.path).toSeq
          LakeSoulTable.forPath(data.path).newCompaction(fileNumLimit = Some(3))
          assert(
            data.files.map(_.path).toSeq == stablePaths,
            "No new input means no additional rolling rewrite"
          )
        }
      }
      assert(
        growingRounds >= 2,
        "Real unfinished outputs must grow over multiple rounds"
      )
      assert(
        promotionRounds >= 2,
        "The workload must reach the fixed target more than once"
      )
    }
  }

  test(
    "L0 reconsiders an earlier injected split when the pass reduces file count"
  ) {
    checkSplitL0Outputs(alwaysSplit = false)
  }

  test("L0 terminates when injected splits keep the file count unchanged") {
    checkSplitL0Outputs(alwaysSplit = true)
  }

  private def checkSplitL0Outputs(alwaysSplit: Boolean): Unit = {
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      (0 until 4).foreach { version =>
        data.write(
          Seq(
            (1, version, s"latest-$version"),
            (100 + version, version, s"unique-$version")
          )
        )
      }
      val original = data.files
      val input = original
        .map(f =>
          new CompressDataFileInfo(
            f.path,
            f.size,
            f.file_exist_cols,
            f.modification_time
          )
        )
        .toList
        .asJava
      val conf = spark.sessionState.newHadoopConf()
      simpleOptions.foreach { case (key, value) => conf.set(key, value) }
      val info = getTableInfo(data.path)
      val origin = scala.collection.mutable.Map.empty[String, Int]
      original.zipWithIndex.foreach { case (file, index) =>
        origin(file.path) = index
      }
      var calls = 0
      val io = new CompactBucketIO(
        conf,
        input,
        info,
        info.table_path.toString,
        "-5",
        1,
        2,
        Long.MaxValue,
        false,
        0L,
        "parquet"
      ) {
        override protected def mergeSmallFileBatch(
            files: java.util.List[CompressDataFileInfo],
            level: Int
        ): java.util.List[CompressDataFileInfo] = {
          calls += 1
          assert(
            files.size() <= 2,
            "Every IO batch must respect readFileNumLimit"
          )
          assert(
            calls <= 8,
            "Writer splits must not cause an endless rolling loop"
          )
          if (alwaysSplit || calls == 1) {
            // Ordered copies exercise split-output control flow. This does not
            // reproduce the Native writer's actual file-splitting conditions.
            files.asScala
              .map { file =>
                val source = new Path(file.getFilePath)
                val destination =
                  new Path(source.getParent, s"split-$calls-${source.getName}")
                val fs = source.getFileSystem(conf)
                assert(
                  org.apache.hadoop.fs.FileUtil
                    .copy(fs, source, fs, destination, false, false, conf)
                )
                origin(destination.toString) = origin(file.getFilePath)
                new CompressDataFileInfo(
                  destination.toString,
                  file.getFileSize,
                  file.getFileExistCols,
                  fs.getFileStatus(destination).getModificationTime
                )
              }
              .toList
              .asJava
          } else super.mergeSmallFileBatch(files, level)
        }
      }
      val result =
        try io.startCompactTask()
        finally io.close()
      assertDiscardPathsAreSafe(result)
      val outputs = result.get("-5").asScala.toSeq
      if (alwaysSplit) {
        assert(calls == 3 && outputs.size == 4)
        assert(outputs.map(file => origin(file.getFilePath)) == Seq(0, 1, 2, 3))
      } else {
        assert(
          calls == 4,
          "The earlier small split must be merged again after the first pass"
        )
        assert(outputs.size == 1)
        checkAnswer(
          spark.read
            .parquet(outputs.head.getFilePath)
            .select("id", "version", "payload"),
          Seq(Row(1, 3, "latest-3")) ++
            (0 until 4).map(v => Row(100 + v, v, s"unique-$v"))
        )
      }
      // Copies and intermediate outputs must not invalidate the old snapshot.
      data.verify()
    }
  }

  test(
    "L1 merges promoted L0 copies while preserving original snapshot paths"
  ) {
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      (0 until 4).foreach { version =>
        data.write(
          Seq(
            (1, version, s"updated-$version"),
            (100 + version, version, payload(version))
          )
        )
      }
      val original = data.files
      assert(original.length == 4)
      val promotion = original.map(_.size).min - 1
      val l1Target = 256L * 1024 * 1024
      assert(original.map(_.size).sum < l1Target)
      val oldSnapshot = LakeSoulTable
        .forPath(data.path)
        .toDF
        .select("id", "version", "payload")
      val oldRows = oldSnapshot.collect().toSeq
      withCompactionOptions(
        simpleOptions ++ Map(
          LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> s"${promotion}B"
        )
      ) {
        data.compact()
        val compacted = data.files
        assert(compacted.length == 1)
        assert(compacted.head.path.contains("/compactdir1/"))
        assert(compacted.head.size < l1Target)
        val conf = spark.sessionState.newHadoopConf()
        val promotedPaths = original.map { file =>
          val source = new Path(file.path)
          val fs = source.getFileSystem(conf)
          assert(fs.getFileStatus(source).getLen == file.size)
          // L0 copies retain their names and bytes, then become real consumed
          // L1 inputs rather than the final files in the committed snapshot.
          val promoted =
            new Path(new Path(source.getParent, "compactdir1"), source.getName)
          assert(fs.getFileStatus(promoted).getLen == file.size)
          promoted.toString
        }.toSet
        val expectedDiscardPaths = original.map(_.path).toSet ++ promotedPaths
        expectedDiscardPaths.foreach { path =>
          assert(
            SparkMetaVersion.dbManager.getDiscardCompressedFileByFilePath(
              path
            ) != null,
            s"Consumed original files and L1 copies must be recorded for cleanup: $path"
          )
        }
        assert(
          SparkMetaVersion.dbManager
            .getDiscardCompressedFileByFilePath(compacted.head.path) == null
        )
        checkAnswer(oldSnapshot, oldRows)
      }
    }
  }

  test(
    "the shared switch and exact namespace.table allowlist govern both L0 and L1"
  ) {
    Seq(
      (false, "match", false),
      (true, "other_namespace", false),
      (true, "other_table", false),
      (true, "match", true),
      (true, "all", true)
    ).foreach { case (enabled, scope, shouldMerge) =>
      withTempDir { dir =>
        val data = new CorrectnessData(dir.getCanonicalPath)
        (0 until 4).foreach { version =>
          data.write(
            Seq(
              (1, version, s"latest-$version"),
              (100 + version, version, s"unique-$version")
            )
          )
        }
        val original = data.files
        val tableName = "rolling_config_fixture"
        val info =
          getTableInfo(data.path).copy(short_table_name = Some(tableName))
        val tables = scope match {
          case "match" => s"unrelated.table, ${info.namespace}.$tableName "
          case "other_namespace" => s"${info.namespace}_other.$tableName"
          case "other_table"     => s"${info.namespace}.${tableName}_other"
          case "all"             => ""
          case other             => fail(s"Unexpected table scope: $other")
        }
        val conf = spark.sessionState.newHadoopConf()
        simpleOptions.foreach { case (key, value) => conf.set(key, value) }
        conf.setBoolean(
          LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_ENABLED.key,
          enabled
        )
        conf.set(LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_TABLES.key, tables)
        conf.set(
          LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key,
          "1B"
        )
        conf.setInt(
          LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key,
          1000
        )
        conf.set(LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key, "1GB")
        conf.unset("max_file_size")
        val input = original
          .map(f =>
            new CompressDataFileInfo(
              f.path,
              f.size,
              f.file_exist_cols,
              f.modification_time
            )
          )
          .toList
          .asJava
        var l0Merges = 0
        var l1Merges = 0
        val io = new CompactBucketIO(
          conf,
          input,
          info,
          info.table_path.toString,
          "-5",
          1,
          1,
          1L,
          false,
          0L,
          "parquet"
        ) {
          override protected def mergeSmallFileBatch(
              files: java.util.List[CompressDataFileInfo],
              level: Int
          ): java.util.List[CompressDataFileInfo] = {
            if (level == 0) {
              assert(
                files.size() == 1,
                "Legacy L0 must rewrite singleton batches"
              )
              l0Merges += 1
            } else {
              assert(level == 1)
              l1Merges += 1
            }
            super.mergeSmallFileBatch(files, level)
          }
        }
        val result =
          try io.startCompactTask()
          finally io.close()
        assertDiscardPathsAreSafe(result)
        val outputs = result.get("-5").asScala.toSeq
        assert(
          l0Merges == (if (shouldMerge) 0 else 4),
          s"enabled=$enabled tables=$tables"
        )
        assert(
          l1Merges == (if (shouldMerge) 1 else 0),
          s"enabled=$enabled tables=$tables"
        )
        assert(outputs.forall(_.getFilePath.contains("/compactdir1/")))
        if (shouldMerge) {
          assert(outputs.size == 1)
          checkAnswer(
            spark.read
              .parquet(outputs.head.getFilePath)
              .select("id", "version", "payload"),
            Seq(Row(1, 3, "latest-3")) ++
              (0 until 4).map(v => Row(100 + v, v, s"unique-$v"))
          )
        } else {
          assert(outputs.size == 4)
          outputs.zipWithIndex.foreach { case (file, version) =>
            checkAnswer(
              spark.read
                .parquet(file.getFilePath)
                .select("id", "version", "payload"),
              Seq(
                Row(1, version, s"latest-$version"),
                Row(100 + version, version, s"unique-$version")
              )
            )
          }
        }
        data.verify()
      }
    }
  }

  test(
    "completed L1 files are retained without rewriting at the maximum level"
  ) {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val options = simpleOptions ++ Map(
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B",
        LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> "1B"
      )
      withCompactionOptions(options) {
        (0 to 3).foreach(v => writeBatch(path, v))
        LakeSoulTable.forPath(path).newCompaction()
        val first = getFileList(path).map(_.path).toSet
        (4 to 7).foreach(v => writeBatch(path, v))
        LakeSoulTable.forPath(path).newCompaction()
        val second = getFileList(path).map(_.path).toSet
        assert(first.subsetOf(second))
        assert(first.size == 4 && second.size == 8)
        verifyBatch(path, 7)
      }
    }
  }

  test(
    "disabled rolling preserves legacy L0 count and post-add byte grouping"
  ) {
    Seq((3, Long.MaxValue), (4, 20L * 1024)).foreach { case (limit, budget) =>
      withTempDir { dir =>
        val data = new CorrectnessData(dir.getCanonicalPath)
        (0 until 4).foreach { version =>
          data.write(
            Seq(
              (1, version, s"latest-$version"),
              (100 + version, version, s"unique-$version")
            )
          )
        }
        val original = data.files
        val info = getTableInfo(data.path)
        val conf = spark.sessionState.newHadoopConf()
        simpleOptions.foreach { case (key, value) => conf.set(key, value) }
        conf.setBoolean(
          LakeSoulSQLConf.COMPACTION_LOWLEVEL_ROLLING_ENABLED.key,
          false
        )
        conf.set(
          LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key,
          "1B"
        )
        conf.setInt(
          LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key,
          1000
        )
        conf.unset("max_file_size")
        // Planning sizes exceed the promotion threshold, but legacy L0 must
        // still rewrite them. The byte check admits a third 10KB file at 20KB.
        val input = original
          .map(f =>
            new CompressDataFileInfo(
              f.path,
              10L * 1024,
              f.file_exist_cols,
              f.modification_time
            )
          )
          .toList
          .asJava
        val batches = scala.collection.mutable.ArrayBuffer.empty[Seq[String]]
        val io = new CompactBucketIO(
          conf,
          input,
          info,
          info.table_path.toString,
          "-5",
          1,
          limit,
          budget,
          false,
          0L,
          "parquet"
        ) {
          override protected def mergeSmallFileBatch(
              files: java.util.List[CompressDataFileInfo],
              level: Int
          ): java.util.List[CompressDataFileInfo] = {
            assert(level == 0, "Disabled rolling must not enter L1 rolling")
            batches += files.asScala.map(_.getFilePath).toVector
            super.mergeSmallFileBatch(files, level)
          }
        }
        val result =
          try io.startCompactTask()
          finally io.close()
        assertDiscardPathsAreSafe(result)
        assert(batches.map(_.size).toSeq == Seq(3, 1))
        assert(batches.flatten.toSeq == original.map(_.path).toSeq)
        val outputs = result.get("-5").asScala.toSeq
        assert(outputs.size == 2)
        assert(outputs.forall(_.getFilePath.contains("/compactdir1/")))
        Seq(0 until 3, 3 until 4).zip(outputs).foreach {
          case (versions, file) =>
            checkAnswer(
              spark.read
                .parquet(file.getFilePath)
                .select("id", "version", "payload"),
              versions.map(v => Row(100 + v, v, s"unique-$v")) :+
                Row(1, versions.last, s"latest-${versions.last}")
            )
        }
        assert(
          result
            .get(CompactBucketIO.DISCARD_FILE_LIST_KEY)
            .asScala
            .map(_.getFilePath)
            .toSet == original.map(_.path).toSet
        )
        data.verify()
      }
    }
  }

  // Keep an independent expected state: later batches intentionally update only
  // some keys, so losing an untouched old row cannot be hidden by a full overwrite.
  private class CorrectnessData(
      val path: String,
      cdc: Boolean = false,
      buckets: Int = 1,
      partitioned: Boolean = false
  ) {
    private val expected =
      scala.collection.mutable.Map.empty[Int, (Int, String)]
    private var created = false

    def write(
        rows: Seq[(Int, Int, String)],
        deletes: Set[Int] = Set.empty,
        verifyAfterWrite: Boolean = true
    ): Unit = {
      require(cdc || deletes.isEmpty)
      val events = rows.map { case (id, version, payload) =>
        (id, version, payload, if (deletes.contains(id)) "delete" else "insert")
      }
      var frame = events.toDF("id", "version", "payload", "op").coalesce(1)
      if (!cdc) frame = frame.drop("op")
      if (partitioned) {
        frame = frame.withColumn(
          "pt",
          when(col("id") % 2 === 0, "even").otherwise("odd")
        )
      }
      if (!created) {
        val writer = frame.write
          .format("lakesoul")
          .option("hashPartitions", "id")
          .option("hashBucketNum", buckets.toString)
        if (cdc) writer.option("lakesoul_cdc_change_column", "op")
        if (partitioned) writer.option("rangePartitions", "pt")
        writer.save(path)
        created = true
      } else {
        LakeSoulTable.forPath(path).upsert(frame)
      }
      rows.foreach { case (id, version, payload) =>
        if (deletes.contains(id)) expected.remove(id)
        else expected.update(id, (version, payload))
      }
      if (verifyAfterWrite) verify()
    }

    def verify(): Unit = {
      LakeSoulTable.uncached(path)
      val frame = LakeSoulTable.forPath(path).toDF
      if (partitioned) {
        checkAnswer(
          frame.select("id", "version", "payload", "pt"),
          expected.toSeq.map { case (id, (version, payload)) =>
            Row(id, version, payload, if (id % 2 == 0) "even" else "odd")
          }
        )
      } else {
        checkAnswer(
          frame.select("id", "version", "payload"),
          expected.toSeq.map { case (id, (version, payload)) =>
            Row(id, version, payload)
          }
        )
      }
    }

    def files: Array[DataFileInfo] = getFileList(path)

    def compact(limit: Option[Int] = None): Unit = {
      LakeSoulTable.forPath(path).newCompaction(fileNumLimit = limit)
      verify()
    }
  }

  private def payload(seed: Int, length: Int = 512): String = {
    val random = new scala.util.Random(seed)
    val alphabet =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    Array
      .fill(length)(alphabet.charAt(random.nextInt(alphabet.length)))
      .mkString
  }

  private def lifecycleBatch(
      round: Int,
      batch: Int
  ): Seq[(Int, Int, String)] = {
    val version = round * 4 + batch
    val first = 1000 + version * 64
    (first until first + 64).map(id => (id, version, payload(id))) ++
      Seq(
        (1, version, s"updated-$version"),
        (2, version, s"persistent-$version")
      )
  }

  // Measure Native output in a separate fixture. Each workload keeps fixed
  // thresholds after calibration, including across writes and promotions.
  private def calibrationSize(batches: Seq[Seq[(Int, Int, String)]]): Long = {
    var size = 0L
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      withCompactionOptions(simpleOptions) {
        batches.foreach(rows => data.write(rows))
        data.compact()
        assert(data.files.length == 1)
        size = data.files.head.size
      }
    }
    size
  }

  test(
    "L0 promotion preserves the large-small-large prefix and latest values"
  ) {
    val batches = Seq(
      (100 until 228).map(id => (id, 0, payload(id))) :+ (1, 0, "first"),
      Seq((1, 1, "small-middle"), (20, 1, "keep-middle")),
      (300 until 428).map(id => (id, 2, payload(id))) :+ (1, 2, "later-large"),
      Seq((1, 3, "latest"), (30, 3, "keep-tail"))
    )
    val smallSize = calibrationSize(Seq.fill(4)(batches(1)))
    val largeSize = calibrationSize(Seq.fill(4)(batches.head))
    assert(
      largeSize > smallSize * 2,
      "Fixture needs distinct large and small Native outputs"
    )
    val promotion = smallSize + (largeSize - smallSize) / 2
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      withCompactionOptions(
        simpleOptions +
          (LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> s"${promotion}B")
      ) {
        batches.foreach(rows => data.write(rows))
        data.compact(Some(1))
        val l1 = data.files.filter(_.path.contains("/compactdir1/"))
        val l0 = data.files.filterNot(_.path.contains("/compactdir"))
        assert(l1.length == 1, "Only the oldest large output may promote")
        assert(l0.length == 3)
        assert(l1.head.size >= promotion)
        assert(l0.head.size < promotion)
        assert(
          l0(1).size >= promotion,
          "The newer large output must remain behind the small L0"
        )
        assert(l0.last.size < promotion)
        val oldL1 = l1.head.path
        data.write(Seq((1, 4, "after-mixed-promotion"), (40, 4, "new-row")))
        data.compact()
        assert(data.files.count(_.path.contains("/compactdir1/")) == 1)
        assert(data.files.exists(_.path == oldL1))
        // The unfinished prefix can remain below promotion after deduplication;
        // a newer large L0 must not jump across it. The old L1 remains valid.
        data.verify()
      }
    }
  }

  test(
    "partial key updates retain untouched rows across repeated L0 and L1 merges"
  ) {
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      withCompactionOptions(
        simpleOptions +
          (LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B")
      ) {
        data.write((1 to 100).map(id => (id, 0, s"original-$id")))
        (1 to 3).foreach { version =>
          data.write(
            (1 to 10).map(id => (id, version, s"changed-$version-$id")) :+
              (100 + version, version, s"inserted-$version")
          )
        }
        data.compact()
        val firstL1 = data.files.filter(_.path.contains("/compactdir1/"))
        assert(firstL1.length == 1)
        (4 to 6).foreach { version =>
          data.write(
            (11 to 20).map(id => (id, version, s"changed-$version-$id")) :+
              (100 + version, version, s"inserted-$version")
          )
        }
        data.compact()
        assert(data.files.exists(_.path == firstL1.head.path))
        assert(data.files.count(f => !f.path.contains("/compactdir")) == 3)
        data.write(
          Seq((1, 7, "final-1"), (20, 7, "final-20"), (107, 7, "inserted-7"))
        )
        data.compact()
        assert(
          data.files.length == 1 && data.files.head.path
            .contains("/compactdir1/")
        )
        assert(data.files.head.path != firstL1.head.path)
        // Keys 21..100 have never been rewritten by the input workload.
        data.verify()
      }
    }
  }

  test("repeated L1 rolling promotion and L2 merges preserve partial updates") {
    checkRepeatedLayers(cdc = false)
  }

  test(
    "a permanent CDC delete cannot resurrect through subsequent L1 and L2 merges"
  ) {
    checkRepeatedLayers(cdc = true)
  }

  test(
    "an unfinished L1 prefix blocks L2 promotion without crossing completed files"
  ) {
    def batch(round: Int, index: Int): Seq[(Int, Int, String)] = {
      val count = if (round == 1) 512 else 16
      val first = 10000 + round * 100000 + index * count
      val version = round * 4 + index
      val rows =
        (first until first + count).map(id => (id, version, payload(id))) :+
          (1, version, s"middle-boundary-$version")
      if (round <= 1) rows :+ (2, version, s"middle-only-$version") else rows
    }
    val smallSize = calibrationSize((0 until 4).map(index => batch(0, index)))
    val target = smallSize * 4
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      withCompactionOptions(
        simpleOptions ++ Map(
          LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B",
          LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> s"${target}B",
          LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "2"
        )
      ) {
        (0 until 4).foreach(index => data.write(batch(0, index)))
        data.compact()
        assert(data.files.length == 1 && data.files.head.size < target)
        val firstSmall = data.files.head.path
        (0 until 4).foreach(index => data.write(batch(1, index)))
        data.compact()
        val large = data.files.filter(_.size >= target)
        assert(large.nonEmpty)
        assert(
          large.forall(_.path.contains("/compactdir1/")),
          "Completed files must not promote past the older unfinished L1 prefix"
        )
        val largePaths = large.map(_.path).toSet
        val largeOrder = large.map(_.path).toSeq
        assert(data.files.exists(_.path == firstSmall))
        (0 until 4).foreach(index => data.write(batch(2, index)))
        data.compact()
        val tail =
          data.files.filterNot(f => (largePaths + firstSmall).contains(f.path))
        assert(tail.length == 1 && tail.head.size < target)
        (0 until 4).foreach(index => data.write(batch(3, index)))
        data.compact()
        assert(data.files.length == largePaths.size + 2)
        assert(data.files.exists(_.path == firstSmall))
        assert(largePaths.subsetOf(data.files.map(_.path).toSet))
        assert(data.files.forall(_.path.contains("/compactdir1/")))
        assert(
          data.files.take(large.length + 1).map(_.path).toSeq ==
            Seq(firstSmall) ++ largeOrder
        )
        assert(
          !data.files.exists(_.path == tail.head.path),
          "Only the trailing small interval must rewrite"
        )
        data.verify()
      }
    }
  }

  test(
    "different partitions and buckets preserve data when only one partition reaches four L0 files"
  ) {
    withTempDir { dir =>
      val data = new CorrectnessData(
        dir.getCanonicalPath,
        buckets = 4,
        partitioned = true
      )
      withCompactionOptions(
        simpleOptions +
          (LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B")
      ) {
        (0 until 3).foreach { version =>
          data.write(
            (1 to 128).map(id => (id, version, s"shared-$version-$id")) ++
              (1000 + version * 32 until 1032 + version * 32)
                .map(id => (id, version, s"untouched-$id"))
          )
        }
        val before = data.files
        assert(before.map(_.file_bucket_id).toSet.size == 4)
        assert(before.map(_.range_partitions).toSet == Set("pt=even", "pt=odd"))
        data.compact()
        assert(data.files.map(_.path).toSet == before.map(_.path).toSet)
        val oddPaths =
          before.filter(_.range_partitions == "pt=odd").map(_.path).toSet
        data.write((2 to 128 by 2).map(id => (id, 3, s"even-update-$id")))
        data.compact()
        val afterEven = data.files
        assert(
          afterEven
            .filter(_.range_partitions == "pt=odd")
            .map(_.path)
            .toSet == oddPaths
        )
        val evenFiles = afterEven.filter(_.range_partitions == "pt=even")
        assert(
          evenFiles.length == 4 && evenFiles.forall(
            _.path.contains("/compactdir1/")
          )
        )
        val evenPaths = evenFiles.map(_.path).toSet
        data.write((1 to 127 by 2).map(id => (id, 4, s"odd-update-$id")))
        data.compact()
        assert(
          data.files
            .filter(_.range_partitions == "pt=even")
            .map(_.path)
            .toSet == evenPaths
        )
        val oddFiles = data.files.filter(_.range_partitions == "pt=odd")
        assert(
          oddFiles.length == 4 && oddFiles.forall(
            _.path.contains("/compactdir1/")
          )
        )
        data.verify()
      }
    }
  }

  private def checkRepeatedLayers(cdc: Boolean): Unit = {
    val single = calibrationSize(
      (0 until 4).map(batch => lifecycleBatch(0, batch))
    )
    val target = single + single / 2
    val options = simpleOptions ++ Map(
      LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B",
      LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> s"${target}B",
      LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "2",
      LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
      LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_SIZE_LIMIT.key -> "256MB"
    )
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath, cdc = cdc)
      var sawRolling = false
      var l2Merges = 0
      var l2MergesAfterDelete = 0
      var deleted = false
      withCompactionOptions(options) {
        (0 until 8).foreach { round =>
          val before = if (round > 0) data.files else Array.empty[DataFileInfo]
          val oldL1 = before.filter(_.path.contains("/compactdir1/"))
          val oldL2 = before.filter(_.path.contains("/compactdir2/"))
          (0 until 4).foreach { batch =>
            val deleteNow = cdc && round == 2 && batch == 0
            val rows = lifecycleBatch(round, batch)
              .filterNot(row => deleted && row._1 == 2)
            data.write(rows, if (deleteNow) Set(2) else Set.empty[Int])
            if (deleteNow) deleted = true
          }
          data.compact()
          val after = data.files
          val names = after.map(file => new Path(file.path).getName).toSet
          if (
            oldL1.exists(file => !names.contains(new Path(file.path).getName))
          ) {
            sawRolling = true
          }
          if (
            oldL2.exists(file => !names.contains(new Path(file.path).getName))
          ) {
            l2Merges += 1
            if (deleted) l2MergesAfterDelete += 1
          }
          // Promotion timing depends on actual output sizes, not a fixed round.
          // The assertions below require repeated real L2 rewrites over the run.
          if (deleted)
            assert(
              LakeSoulTable
                .forPath(data.path)
                .toDF
                .filter(col("id") === 2)
                .take(1)
                .isEmpty
            )
        }
      }
      assert(sawRolling, "Workload must actually rewrite an existing L1 file")
      assert(
        l2Merges >= 2,
        "Workload must trigger repeated L2 rewrites, not only promotion"
      )
      if (cdc)
        assert(
          l2MergesAfterDelete >= 2,
          "Deletion must survive repeated actual L2 merges"
        )
    }
  }

  private def assertDiscardPathsAreSafe(
      result: java.util.Map[String, java.util.List[CompressDataFileInfo]]
  ): Unit = {
    val discarded =
      result.get(CompactBucketIO.DISCARD_FILE_LIST_KEY).asScala.toSeq
    val discardPaths = discarded.map(_.getFilePath)
    val activePaths = result.asScala.iterator
      .filter { case (partition, _) =>
        partition != CompactBucketIO.DISCARD_FILE_LIST_KEY
      }
      .flatMap { case (_, files) => files.asScala.iterator.map(_.getFilePath) }
      .toSet
    assert(
      discardPaths.distinct.size == discardPaths.size,
      "Each discarded path must be recorded only once"
    )
    assert(
      activePaths.intersect(discardPaths.toSet).isEmpty,
      "Final output paths must never be marked discarded"
    )
    val conf = spark.sessionState.newHadoopConf()
    discarded.foreach { file =>
      val path = new Path(file.getFilePath)
      assert(
        path.getFileSystem(conf).getFileStatus(path).isFile,
        s"Discard must reference a real file: $path"
      )
    }
  }

  private def withCompactionOptions(
      options: Map[String, String]
  )(body: => Unit): Unit = {
    val session = SparkSession.active
    val hadoop = options.map { case (key, value) =>
      ("spark.hadoop." + key) -> value
    }
    val previous =
      hadoop.keys.map(key => key -> session.conf.getOption(key)).toMap
    withSQLConf(options.toSeq: _*) {
      try {
        hadoop.foreach { case (key, value) => session.conf.set(key, value) }
        body
      } finally {
        previous.foreach {
          case (key, Some(value)) => session.conf.set(key, value)
          case (key, None)        => session.conf.unset(key)
        }
      }
    }
  }

  // Auxiliary method: Get the number of files
  def getFileList(tablePath: String): Array[DataFileInfo] = {
    val sm = SnapshotManagement(
      SparkUtil.makeQualifiedTablePath(new Path(tablePath)).toString
    )
    val partitionList =
      SparkMetaVersion.getAllPartitionInfo(sm.getTableInfoOnly.table_id)
    DataOperation.getTableDataInfo(partitionList)
  }

  def getTableInfo(tablePath: String): TableInfo = {
    val sm = SnapshotManagement(
      SparkUtil.makeQualifiedTablePath(new Path(tablePath)).toString
    )
    sm.getTableInfoOnly
  }

}
