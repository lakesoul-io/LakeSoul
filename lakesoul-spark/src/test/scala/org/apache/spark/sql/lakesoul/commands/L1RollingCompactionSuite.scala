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

  private val simpleOptions = Map(
    LakeSoulSQLConf.COMPACTION_L1_ROLLING_ENABLED.key -> "true",
    LakeSoulSQLConf.COMPACTION_MAX_LEVEL0_FILE_NUM_LIMIT.key -> "4",
    LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1GB",
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
    "multiple retained L0 batches preserve last-write-wins and later updates"
  ) {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withCompactionOptions(simpleOptions) {
        (0 to 3).foreach(v => writeBatch(path, v))
        val before = getFileList(path).map(_.path).toSet
        // Force four independent outputs for the same keys. Their metadata order,
        // rather than directory listing or filename order, must preserve v3.
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(1))
        val after = getFileList(path)
        assert(after.length == 4)
        assert(after.forall(f => !f.path.contains("/compactdir")))
        assert(after.forall(f => !before.contains(f.path)))
        verifyBatch(path, 3)
        writeBatch(path, 4)
        LakeSoulTable.forPath(path).newCompaction(fileNumLimit = Some(2))
        assert(getFileList(path).length == 3)
        verifyBatch(path, 4)
      }
    }
  }

  test("new L1 files immediately merge with an existing unfinished L1 file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withCompactionOptions(
        simpleOptions +
          (LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B")
      ) {
        (0 to 3).foreach(v => writeBatch(path, v))
        LakeSoulTable.forPath(path).newCompaction()
        val first = getFileList(path)
        assert(first.length == 1 && first.head.path.contains("/compactdir1/"))
        verifyBatch(path, 3)
        // No new L1 means no rewrite.
        LakeSoulTable.forPath(path).newCompaction()
        assert(getFileList(path).map(_.path).toSet == first.map(_.path).toSet)
        (4 to 7).foreach(v => writeBatch(path, v))
        LakeSoulTable.forPath(path).newCompaction()
        val second = getFileList(path)
        assert(second.length == 1 && second.head.path.contains("/compactdir1/"))
        assert(second.head.path != first.head.path)
        verifyBatch(path, 7)
      }
    }
  }

  test("completed L1 files stop participating in regular rolling") {
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
        assert(second.size == 2)
        verifyBatch(path, 7)
      }
    }
  }

  test("L1 completed files still participate in existing promotion to L2") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withCompactionOptions(
        simpleOptions ++ Map(
          LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B",
          LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> "1B",
          LakeSoulSQLConf.MAX_NUM_LEVELS_LIMIT.key -> "2"
        )
      ) {
        (0 to 3).foreach(v => writeBatch(path, v))
        LakeSoulTable.forPath(path).newCompaction()
        assert(getFileList(path).forall(_.path.contains("/compactdir2/")))
        verifyBatch(path, 3)
      }
    }
  }

  test("CDC deletes survive repeated L0 accumulation and L1 merges") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val expected = scala.collection.mutable.Map.empty[Int, String]
      withCompactionOptions(
        simpleOptions +
          (LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B")
      ) {
        (0 until 12).foreach { round =>
          val events =
            if (round == 4) Seq((1, "deleted", "delete"))
            else if (round == 8) Seq((1, "reinserted", "insert"))
            else if (round == 0) Seq((1, "original", "insert"))
            else Seq((round + 10, s"v$round", "insert"))
          events.foreach { case (id, value, op) =>
            if (op == "delete") expected.remove(id)
            else expected.update(id, value)
          }
          val frame = events.toDF("id", "value", "op").coalesce(1)
          if (round == 0)
            frame.write
              .format("lakesoul")
              .option("hashPartitions", "id")
              .option("hashBucketNum", "1")
              .option("lakesoul_cdc_change_column", "op")
              .save(path)
          else LakeSoulTable.forPath(path).upsert(frame)
          def verify(): Unit = {
            LakeSoulTable.uncached(path)
            checkAnswer(
              LakeSoulTable.forPath(path).toDF.select("id", "value"),
              expected.toSeq.map { case (id, value) => Row(id, value) }
            )
          }
          verify()
          LakeSoulTable.forPath(path).newCompaction()
          verify()
        }
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
        deletes: Set[Int] = Set.empty
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
      verify()
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

  // Measure Native output in a separate table; production test thresholds remain
  // fixed for every write/compaction of the actual workload, including promotion.
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
        assert(
          !data.files.exists(_.path == oldL1),
          "The next L1 merge must consume the old L1"
        )
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
    "a completed middle L1 file separates rolling intervals without losing old rows"
  ) {
    def batch(round: Int, index: Int): Seq[(Int, Int, String)] = {
      val count = if (round == 1) 512 else 16
      val first = 10000 + round * 100000 + index * count
      val version = round * 4 + index
      (first until first + count).map(id => (id, version, payload(id))) :+
        (1, version, s"middle-boundary-$version")
    }
    val smallSize = calibrationSize((0 until 4).map(index => batch(0, index)))
    val target = smallSize * 4
    withTempDir { dir =>
      val data = new CorrectnessData(dir.getCanonicalPath)
      withCompactionOptions(
        simpleOptions ++ Map(
          LakeSoulSQLConf.COMPACTION_PICK_NEXT_LEVEL_MIN_FILE_SIZE.key -> "1B",
          LakeSoulSQLConf.COMPACTION_L1_TARGET_FILE_SIZE.key -> s"${target}B"
        )
      ) {
        (0 until 4).foreach(index => data.write(batch(0, index)))
        data.compact()
        assert(data.files.length == 1 && data.files.head.size < target)
        val firstSmall = data.files.head.path
        (0 until 4).foreach(index => data.write(batch(1, index)))
        data.compact()
        val large = data.files.filter(_.size >= target)
        assert(large.length == 1)
        assert(data.files.exists(_.path == firstSmall))
        (0 until 4).foreach(index => data.write(batch(2, index)))
        data.compact()
        val tail = data.files.filterNot(f =>
          Set(firstSmall, large.head.path).contains(f.path)
        )
        assert(tail.length == 1 && tail.head.size < target)
        (0 until 4).foreach(index => data.write(batch(3, index)))
        data.compact()
        assert(data.files.length == 3)
        assert(data.files.exists(_.path == firstSmall))
        assert(data.files.exists(_.path == large.head.path))
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
          assert(
            after.exists(_.path.contains("/compactdir2/")) || round == 0,
            s"Round $round must have produced L2 through actual L1 accumulation"
          )
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
