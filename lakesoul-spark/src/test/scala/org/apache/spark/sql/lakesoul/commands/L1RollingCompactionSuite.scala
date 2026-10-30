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
      .set("spark.dmetasoul.lakesoul.compaction.level0.file.number.limit", "2")
      .set(
        "spark.dmetasoul.lakesoul.compaction.pick.next.level.min.file.size",
        "1KB"
      )
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

  test("new compaction preserves multi-batch upsert order") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      val spark = SparkSession.active

      def batch(start: Int, end: Int, value: String): DataFrame =
        spark.range(start, end).select(col("id"), lit(value).as("value")).coalesce(1)

      // Batch 1: id 1..10000 -> v1.
      batch(1, 10001, "v1").write
        .format("lakesoul")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "4")
        .save(tablePath)

      val table = LakeSoulTable.forPath(tablePath)

      // Batch 2: update id 1..2000 -> v2, insert id 10001..12000 -> v2.
      // upsert is synchronous, so each batch is committed before the next one starts.
      table.upsert(
        batch(1, 2001, "v2").union(batch(10001, 12001, "v2"))
      )

      // Batch 3: update id 1001..3000 -> v3.
      table.upsert(batch(1001, 3001, "v3"))

      val expected = spark.range(1, 12001).select(
        col("id"),
        when(col("id") <= 1000, lit("v2"))
          .when(col("id") <= 3000, lit("v3"))
          .when(col("id") <= 10000, lit("v1"))
          .otherwise(lit("v2"))
          .as("value")
      )

      // Program expectation vs. pre-compaction read, all fields.
      val before = LakeSoulTable.forPath(tablePath).toDF.select("id", "value")
      assert(before.count() == 12000)
      checkAnswer(before.orderBy("id"), expected.orderBy("id"))

      val filesBefore = getFileList(tablePath).length

      val l1Confs = Map(
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "true",
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "100MB",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_BYTES.key -> "256MB",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_FILES.key -> "20",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "2",
        // Keep this test on the regular rolling branch rather than layer pressure.
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "1000",
        LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key -> "1GB"
      )
      val hadoopConfs = l1Confs.map { case (k, v) => ("spark.hadoop." + k) -> v }
      val previousHadoopConfs = hadoopConfs.keys.map(k => k -> spark.conf.getOption(k)).toMap
      withSQLConf(l1Confs.toSeq: _*) {
        // The executor reads Hadoop Configuration, not just the session SQLConf.
        try {
          hadoopConfs.foreach { case (k, v) => spark.conf.set(k, v) }
          LakeSoulTable.forPath(tablePath).newCompaction()
          LakeSoulTable.uncached(tablePath)

          val firstCompacted = getFileList(tablePath)
          assert(firstCompacted.length < filesBefore)
          val oldL1Paths = firstCompacted.map(_.path).toSet
          assert(firstCompacted.forall(_.path.contains("/compactdir1/")))
          assert(firstCompacted.groupBy(_.file_bucket_id).size == 4)
          assert(firstCompacted.groupBy(_.file_bucket_id).values.forall(_.length == 1))
          checkAnswer(
            LakeSoulTable.forPath(tablePath).toDF.select("id", "value"),
            expected
          )

          // Establish newer values overlapping every bucket and the OLD L1 contents.
          LakeSoulTable.forPath(tablePath).upsert(batch(1, 12001, "v4"))
          LakeSoulTable.forPath(tablePath).upsert(batch(1001, 3001, "v5"))
          val expectedSecond = spark.range(1, 12001).select(
            col("id"),
            when(col("id").between(1001, 3000), lit("v5"))
              .otherwise(lit("v4")).as("value")
          )
          LakeSoulTable.uncached(tablePath)
          checkAnswer(
            LakeSoulTable.forPath(tablePath).toDF.select("id", "value"),
            expectedSecond
          )
          val secondInputs = getFileList(tablePath)
          val l0Inputs = secondInputs.filterNot(_.path.contains("/compactdir"))
          assert(oldL1Paths.subsetOf(secondInputs.map(_.path).toSet))
          assert(l0Inputs.groupBy(_.file_bucket_id).size == 4)
          assert(l0Inputs.groupBy(_.file_bucket_id).values.forall(_.length >= 2))

          // One L0 input per batch forces multiple new L1 outputs next to each old L1.
          // A single final L1 per bucket therefore cannot be explained by L0 merging alone.
          LakeSoulTable.forPath(tablePath).newCompaction(fileNumLimit = Some(1))
          LakeSoulTable.uncached(tablePath)
          val secondCompacted = getFileList(tablePath)
          assert(secondCompacted.length < secondInputs.length)
          assert(secondCompacted.forall(_.path.contains("/compactdir1/")))
          assert(secondCompacted.groupBy(_.file_bucket_id).size == 4)
          assert(secondCompacted.groupBy(_.file_bucket_id).values.forall(_.length == 1))
          assert(oldL1Paths.intersect(secondCompacted.map(_.path).toSet).isEmpty,
            "Old L1 files must participate in rolling compaction, not remain untouched")
          assert(l0Inputs.map(_.path).toSet.intersect(secondCompacted.map(_.path).toSet).isEmpty)
          val after = LakeSoulTable.forPath(tablePath).toDF.select("id", "value")
          assert(after.count() == 12000)
          checkAnswer(after, expectedSecond)

        } finally {
          previousHadoopConfs.foreach {
            case (k, Some(v)) => spark.conf.set(k, v)
            case (k, None) => spark.conf.unset(k)
          }
        }
      }
    }
  }

  // Real files + native IO, not a simulation of the planner's output.
  test("rolling preserves values across retained files and multiple merge groups") {
    checkGroupedUpserts("interleaved", Seq(512, 8, 8, 512, 8, 8), Set(0, 3), 4)
  }

  test("absorbing adjacent large files preserves values and untouched intervals") {
    checkGroupedUpserts("absorption", Seq(512, 8, 8192, 8, 8192, 8, 512),
      Set(2, 3, 4), 5)
  }

  test("connecting small groups includes middle large file and preserves boundary updates") {
    // Base groups: [huge][small][large][small][huge]. Only connectSmallGroups
    // can turn the three middle groups into one; single-pass adjacent absorption
    // cannot consume the same large neighbor twice.
    checkGroupedUpserts("connection", Seq(8192, 8, 512, 8, 8192), Set(0, 4), 3)
  }

  test("pressure compaction preserves values across multiple budget limited groups") {
    checkGroupedUpserts("pressure", Seq.fill(7)(8), Set(6), 4)
  }

  test("regular L1 rolling is followed by L1 pressure after more writes") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val expected = scala.collection.mutable.Map.empty[Long, (Int, String)]
      val random = new scala.util.Random(71)
      def writeBatch(batch: Int, payloadLength: Int): Unit = {
        val rows = ((0L until 64L) ++ Seq(1000L + batch)).map { id =>
          val payload = random.alphanumeric.take(payloadLength).mkString
          expected(id) = (batch, payload)
          (id, batch, payload)
        }
        val frame = rows.toDF("id", "batch", "payload").coalesce(1)
        if (batch == 0) {
          frame.write.format("lakesoul").option("hashPartitions", "id")
            .option("hashBucketNum", "1").save(path)
        } else LakeSoulTable.forPath(path).upsert(frame)
      }
      def verify(): Unit = {
        LakeSoulTable.uncached(path)
        checkAnswer(LakeSoulTable.forPath(path).toDF.select("id", "batch", "payload"),
          expected.toSeq.map { case (id, (batch, payload)) => Row(id, batch, payload) })
      }
      // Seed one large and two small L1 files; no promotion in this L1-only test.
      writeBatch(0, 512)
      writeBatch(1, 8)
      writeBatch(2, 8)
      val seed = Map(
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "false",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "2",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "1000",
        LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key -> "1GB",
        LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1"
      )
      withCompactionOptions(seed) {
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(1))
      }
      verify()
      val inputs = getFileList(path)
      assert(inputs.length == 3 && inputs.forall(_.path.contains("/compactdir1/")))
      val target = inputs.drop(1).map(_.size).max * 4
      assert(inputs.head.size >= target && inputs.drop(1).map(_.size).sum < target)
      // Keep these options unchanged across both calls. Three inputs are below
      // pressure=4; after rolling leaves two, two new L1 files reach pressure=4.
      val options = seed ++ Map(
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "true",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "4",
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> s"${target}B",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_BYTES.key -> "256MB",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_FILES.key -> "2",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_SMALL_FILE_LIMIT.key -> "1000",
        LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
        LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_SIZE_LIMIT.key -> "256MB"
      )
      withCompactionOptions(options) {
        LakeSoulTable.forPath(path).newCompaction()
        verify()
        val first = getFileList(path)
        assert(first.length == 2)
        assert(first.map(_.path).toSet.intersect(inputs.map(_.path).toSet) ==
          Set(inputs.head.path), "Regular rolling must retain the large L1 and merge the small pair")
        assert(first.map(_.size).sum < 1024L * 1024 * 1024)

        writeBatch(3, 8)
        writeBatch(4, 8)
        verify()
        val beforePressure = getFileList(path)
        assert(beforePressure.count(f => !f.path.contains("/compactdir")) == 2)
        assert(first.map(_.path).toSet.subsetOf(beforePressure.map(_.path).toSet))
        // Keep each L0 separate so the L1 planner receives exactly four files.
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(1))
        verify()
        val second = getFileList(path)
        assert(second.length == 2 && second.forall(_.path.contains("/compactdir1/")))
        assert(first.map(_.path).toSet.intersect(second.map(_.path).toSet).isEmpty,
          "Pressure must rewrite the previously retained large L1 and rolling output; " +
            "regular rolling would keep that large file")
      }
    }
  }

  test("rolling L1 cooperates with promotion and repeated legacy L2 merges") {
    checkLayerLifecycle("multilevel", Seq.fill(12)(true), cdc = false)
  }

  test("same table supports legacy to rolling and rollback to legacy") {
    checkLayerLifecycle("switch", Seq.fill(3)(false) ++ Seq.fill(5)(true) ++ Seq.fill(4)(false), cdc = false)
  }

  test("CDC deletions survive rolling promotion and L2 merging without resurrection") {
    checkLayerLifecycle("cdc", Seq.fill(12)(true), cdc = true)
  }

  private def checkLayerLifecycle(
      scenario: String, rollingModes: Seq[Boolean], cdc: Boolean): Unit = {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val expected = scala.collection.mutable.Map.empty[Int, (String, String)]
      val random = new scala.util.Random(903)
      def payload(): String = random.alphanumeric.take(200).mkString
      val initial = (1 to 32).map(id => (id, payload(), "insert"))
      initial.foreach { case (id, value, op) => expected(id) = (value, op) }
      val writer = initial.toDF("id", "value", "op").coalesce(1).write
        .format("lakesoul").option("hashPartitions", "id").option("hashBucketNum", "1")
      if (cdc) writer.option("lakesoul_cdc_change_column", "op")
      writer.save(path)

      def verify(): Unit = {
        LakeSoulTable.uncached(path)
        // Full multiset equality catches missing rows AND duplicate primary keys.
        checkAnswer(LakeSoulTable.forPath(path).toDF.select("id", "value", "op"),
          expected.toSeq.map { case (id, (value, op)) => Row(id, value, op) })
      }
      def write(rows: Seq[(Int, String, String)]): Unit = {
        LakeSoulTable.forPath(path).upsert(rows.toDF("id", "value", "op").coalesce(1))
        rows.foreach { case (id, value, op) =>
          if (op == "delete") expected.remove(id) else expected(id) = (value, op)
        }
        verify()
      }
      // Fixed for the entire workload: use small thresholds to keep the test cheap,
      // but NEVER lower a threshold to force an already-written file to promote.
      val target = 32L * 1024
      val base = Map(
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "2",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "3",
        LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key -> "1GB",
        LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2",
        LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_SIZE_LIMIT.key -> "256MB",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_BYTES.key -> "256MB",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_FILES.key -> "20",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_SMALL_FILE_LIMIT.key -> "2",
        LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "3",
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "32KB",
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE_MULTIPLIER.key -> "1024"
      )
      var sawRollingRewrite = false
      var sawL2Merge = false
      var sawL1BelowTarget = false
      var deletionRound = -1
      var reinserted = false
      var sawL2MergeAfterDelete = false
      var sawRollingRewriteAfterDelete = false
      // Promotion preserves the basename; an actual merge generates a new one.
      def fileNames(files: Array[DataFileInfo]): Set[String] =
        files.map(f => new Path(f.path).getName).toSet
      var transitionInputs = Set.empty[String]
      var transitionRewritten = false
      var checkedTransitions = 0
      rollingModes.zipWithIndex.foreach { case (rolling, round) =>
        val before = getFileList(path)
        if (round > 0 && rolling != rollingModes(round - 1)) {
          if (checkedTransitions > 0) {
            assert(transitionRewritten,
              s"$scenario: previous strategy transition never rewrote its inherited files")
          }
          transitionInputs = fileNames(before)
          assert(transitionInputs.nonEmpty, "Strategy switch must inherit existing files")
          transitionRewritten = false
          checkedTransitions += 1
        }
        val oldL1 = before.filter(_.path.contains("/compactdir1/"))
        val oldL2Paths = before.filter(_.path.contains("/compactdir2/")).map(_.path).toSet
        val events = if (cdc && deletionRound < 0 && oldL2Paths.nonEmpty) {
          // The old values of keys 1 and 2 have naturally reached L2.
          deletionRound = round
          Seq((1, "deleted-one", "delete"), (2, "deleted-two", "delete"))
        } else if (cdc && deletionRound >= 0 && !reinserted && round >= deletionRound + 3) {
          reinserted = true
          Seq((1, "reinserted-one", "insert"))
        } else Seq.empty[(Int, String, String)]
        // Each round adds 64 genuinely new keys plus updates to existing keys.
        // Two committed batches naturally satisfy the L0 trigger; no empty task
        // or artificial threshold change is used for promotion.
        val firstId = 1000 + round * 64
        write((3 to 16).map(id => (id, payload(), "insert")) ++
          (firstId until firstId + 32).map(id => (id, payload(), "insert")) ++ events)
        write((10 to 24).map(id => (id, payload(), "insert")) ++
          (firstId + 32 until firstId + 64).map(id => (id, payload(), "insert")))
        withCompactionOptions(base +
          (LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> rolling.toString)) {
          LakeSoulTable.forPath(path).newCompaction()
        }
        verify()
        val after = getFileList(path)
        val paths = after.map(_.path).toSet
        val l1 = after.filter(_.path.contains("/compactdir1/"))
        val l2 = after.filter(_.path.contains("/compactdir2/"))
        assert(after.forall(f => f.path.contains("/compactdir1/") ||
          f.path.contains("/compactdir2/")), "Expected completed L0 compaction and only L1/L2")
        assert(l2.forall(_.size >= target), "L2 must arise from natural size-based promotion")
        sawL1BelowTarget ||= l1.exists(_.size < target)
        val afterNames = fileNames(after)
        if (checkedTransitions > 0 && transitionInputs.diff(afterNames).nonEmpty) {
          transitionRewritten = true
        }
        val rewrittenL1 = fileNames(oldL1).diff(afterNames)
        if (rolling && rewrittenL1.nonEmpty) {
          sawRollingRewrite = true
          if (deletionRound >= 0) sawRollingRewriteAfterDelete = true
        }
        // With no L3 output, disappearance of an existing L2 path proves the
        // legacy higher-level merge ran, not merely that another L1 was promoted.
        if (oldL2Paths.diff(paths).nonEmpty) {
          sawL2Merge = true
          if (deletionRound >= 0) sawL2MergeAfterDelete = true
        }
        println(s"[L1-correctness] PASS $scenario round=$round rolling=$rolling " +
          s"rows=${expected.size} L1=${l1.length} L2=${l2.length}: complete data equality")
      }
      if (checkedTransitions > 0) {
        assert(checkedTransitions == 2 && transitionRewritten,
          "Both legacy-to-rolling and rolling-to-legacy phases must rewrite inherited files")
      }
      assert(sawL1BelowTarget, "Workload must retain a small L1 before growth")
      assert(sawRollingRewrite, "Workload must actually rewrite an old L1 with rolling enabled")
      assert(sawL2Merge, "Workload must grow enough to exercise the legacy L2 merge")
      write(Seq((3, "after-all-compactions", "insert")))
      if (cdc) {
        assert(deletionRound >= 0 && sawRollingRewriteAfterDelete && sawL2MergeAfterDelete,
          "Deletion must be checked across both a real rolling L1 rewrite and an L2 merge")
        assert(!expected.contains(2) && reinserted && expected(1)._1 == "reinserted-one")
      }
    }
  }

  test("rolling table allowlist isolates named tables in the same job") {
    withTempDir { dir =>
      val suffix = java.util.UUID.randomUUID().toString.replace("-", "")
      val names = Seq(s"l1_selected_$suffix", s"l1_unselected_$suffix")
      val paths = Seq("directory_a", "directory_b").map(n => new java.io.File(dir, n).getPath)
      val base = Map(
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "false",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_TABLES.key -> "",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "2",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "1000",
        LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key -> "1GB",
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "100MB",
        LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1"
      )
      withTable(names: _*) {
        paths.zip(names).foreach { case (path, name) =>
          (0 until 3).foreach { batch =>
            val frame = Seq((1, s"v$batch"), (100 + batch, s"unique$batch"))
              .toDF("id", "value").coalesce(1)
            if (batch == 0) {
              frame.write.format("lakesoul").option("hashPartitions", "id")
                .option("hashBucketNum", "1").option("shortTableName", name).save(path)
            } else LakeSoulTable.forPath(path).upsert(frame)
          }
          withCompactionOptions(base) {
            LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(1))
          }
          assert(getFileList(path).length == 3)
        }
        val selected = getTableInfo(paths.head)
        assert(selected.short_table_name.contains(names.head))
        val originalPaths = paths.map(p => getFileList(p).map(_.path).toSet)
        val options = base ++ Map(
          LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "true",
          LakeSoulSQLConf.COMPACTION_L1_ROLLING_TABLES.key ->
            s"${selected.namespace}.${selected.short_table_name.get}"
        )
        withCompactionOptions(options) {
          // An unmatched table before AND after the selected table detects shared-config leaks.
          Seq(1, 0, 1).foreach { index =>
            val path = paths(index)
            LakeSoulTable.forPath(path).newCompaction()
            LakeSoulTable.uncached(path)
            checkAnswer(LakeSoulTable.forPath(path).toDF.select("id", "value"),
              Seq(Row(1, "v2"), Row(100, "unique0"), Row(101, "unique1"), Row(102, "unique2")))
            val files = getFileList(path).map(_.path).toSet
            if (index == 0) {
              assert(files.size == 1 && files.intersect(originalPaths(index)).isEmpty)
            } else assert(files == originalPaths(index))
          }
        }
      }
    }
  }

  private def withCompactionOptions(options: Map[String, String])(body: => Unit): Unit = {
    val session = SparkSession.active
    val hadoop = options.map { case (key, value) => ("spark.hadoop." + key) -> value }
    val previous = hadoop.keys.map(key => key -> session.conf.getOption(key)).toMap
    withSQLConf(options.toSeq: _*) {
      try {
        hadoop.foreach { case (key, value) => session.conf.set(key, value) }
        body
      } finally {
        previous.foreach {
          case (key, Some(value)) => session.conf.set(key, value)
          case (key, None) => session.conf.unset(key)
        }
      }
    }
  }

  private def checkGroupedUpserts(
      scenario: String,
      payloadLengths: Seq[Int],
      retainedIndices: Set[Int],
      expectedFileCount: Int): Unit = {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val expected = scala.collection.mutable.Map.empty[Long, (Int, String)]
      val random = new scala.util.Random(47)
      payloadLengths.zipWithIndex.foreach { case (payloadLength, batch) =>
        // Overlapping keys have different last writers; unique keys detect lost intervals.
        // Each pair has a dedicated key, updated ONLY by those two files.
        // Thus later files cannot mask a wrong winner inside the connected interval
        // or across either retained boundary. Per-file keys also detect lost files.
        val pairKeys = if (scenario == "connection") {
          for {
            earlier <- payloadLengths.indices
            later <- (earlier + 1) until payloadLengths.length
            if batch == earlier || batch == later
          } yield 30000L + earlier * 100L + later
        } else Seq.empty[Long]
        val keys = (0L until 64L).filter(id => (id + batch) % 3 != 0) ++
          (0L until 16L).map(id => 1000L + batch * 100L + id) ++ pairKeys
        val rows = keys.map { id =>
          val payload = random.alphanumeric.take(payloadLength).mkString
          expected(id) = (batch, payload)
          (id, batch, payload)
        }
        val frame = rows.toDF("id", "batch", "payload").coalesce(1)
        if (batch == 0) {
          frame.write.format("lakesoul").option("hashPartitions", "id")
            .option("hashBucketNum", "1").save(path)
        } else {
          LakeSoulTable.forPath(path).upsert(frame)
        }
      }
      val expectedRows = expected.toSeq.map { case (id, (batch, payload)) =>
        Row(id, batch, payload)
      }
      def verifyData(): Unit = {
        LakeSoulTable.uncached(path)
        checkAnswer(LakeSoulTable.forPath(path).toDF.select("id", "batch", "payload"),
          expectedRows)
      }
      verifyData()
      val seedOptions = Map(
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "false",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "2",
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key -> "1000",
        LakeSoulSQLConf.COMPACTION_MAX_BYTES_FOR_LEVEL_BASE.key -> "1GB",
        // Isolate L1 ordering from the separate promotion policy.
        LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "1"
      )
      withCompactionOptions(seedOptions) {
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(1))
      }
      verifyData()
      val inputs = getFileList(path)
      assert(inputs.length == payloadLengths.length,
        s"$scenario fixture must produce one L1 per write, got ${inputs.length}")
      assert(inputs.forall(_.path.contains("/compactdir1/")))
      val smallIndices = payloadLengths.indices.filter(payloadLengths(_) == 8)
      val largestSmall = smallIndices.map(inputs(_).size).max
      val target = largestSmall * 4
      val ordinaryLarge = payloadLengths.indices.filter(payloadLengths(_) == 512)
      assert(ordinaryLarge.forall(i => inputs(i).size >= target),
        s"$scenario fixture did not produce the required large/small boundaries")
      val maxBytes = if (ordinaryLarge.nonEmpty) {
        ordinaryLarge.map(inputs(_).size).max + largestSmall * 4
      } else target * 8
      assert(payloadLengths.indices.filter(payloadLengths(_) == 8192)
        .forall(i => inputs(i).size > maxBytes), "Barrier files must exceed the input budget")
      val pressure = scenario == "pressure"
      val options = seedOptions ++ Map(
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "true",
        LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> s"${target}B",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_BYTES.key -> s"${maxBytes}B",
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_MAX_INPUT_FILES.key ->
          (if (pressure) "2" else "20"),
        LakeSoulSQLConf.COMPACTION_L1_ROLLING_SMALL_FILE_LIMIT.key ->
          (if (Set("absorption", "connection").contains(scenario)) "2" else "1000"),
        LakeSoulSQLConf.COMPACTION_MAX_LEVEL_FILE_NUM_LIMIT.key ->
          (if (pressure) "2" else "1000"),
        LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT.key -> "2"
      )
      println(s"[L1-correctness] scenario=$scenario, inputBytes=${inputs.map(_.size).mkString(",")}")
      withCompactionOptions(options) {
        LakeSoulTable.forPath(path).newCompaction()
      }
      verifyData()
      val outputs = getFileList(path)
      val inputPaths = inputs.map(_.path).toSet
      val retainedPaths = retainedIndices.map(i => inputs(i).path)
      assert(outputs.length == expectedFileCount,
        s"$scenario: expected $expectedFileCount files, got ${outputs.length}")
      assert(outputs.map(_.path).toSet.intersect(inputPaths) == retainedPaths,
        s"$scenario: wrong files retained/replaced; intended merge branch was not exercised")
      // Writes after compaction also need to override values from every output interval.
      val later = (0L until 64L).map(id => (id, 99, s"latest-$id"))
      LakeSoulTable.forPath(path).upsert(later.toDF("id", "batch", "payload").coalesce(1))
      LakeSoulTable.uncached(path)
      val finalExpected = expectedRows.filter(_.getLong(0) >= 1000L) ++
        later.map { case (id, batch, payload) => Row(id, batch, payload) }
      checkAnswer(LakeSoulTable.forPath(path).toDF.select("id", "batch", "payload"), finalExpected)
      println(s"[L1-correctness] PASS $scenario: retained paths, full rows, and later upserts")
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
