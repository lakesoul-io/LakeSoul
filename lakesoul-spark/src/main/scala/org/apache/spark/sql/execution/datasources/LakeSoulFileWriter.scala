// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

// This file is adopted from https://github.com/apache/spark/blob/branch-3.3/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatWriter.scala

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FileFormatWriter.ConcurrentOutputWriterSpec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.rules.withPartitionAndOrdering
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.vectorized.ArrowFakeRowAdaptor
import org.apache.spark.util.{SerializableConfiguration, Utils}
import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_NON_PARTITION_TABLE_PART_DESC, LAKESOUL_RANGE_PARTITION_SPLITTER}

import java.util.{Date, UUID}

/** A helper object for writing FileFormat data out to a location. */
object LakeSoulFileWriter extends Logging {
  /**
   * Basic work flow of this command is:
   * 1. Driver side setup, including output committer initialization and data source specific
   * preparation work for the write job to be issued.
   * 2. Issues a write job consists of one or more executor side tasks, each of which writes all
   * rows within an RDD partition.
   * 3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
   * exception is thrown during task commitment, also aborts that task.
   * 4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
   * thrown during job commitment, also aborts the job.
   * 5. If the job is successfully committed, perform post-commit operations such as
   * processing statistics.
   *
   * @return The set of all partition paths that were updated during this write job.
   */
  def write(
             sparkSession: SparkSession,
             plan: SparkPlan,
             fileFormat: FileFormat,
             committer: FileCommitProtocol,
             outputSpec: OutputSpec,
             hadoopConf: Configuration,
             partitionColumns: Seq[Attribute],
             bucketSpec: Option[BucketSpec],
             statsTrackers: Seq[WriteJobStatsTracker],
             options: Map[String, String])
  : Set[String] = {

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
      .map(FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation))
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    // empty/null strings has already been handled in
    // org.apache.spark.sql.lakesoul.TransactionalWrite.writeFiles
    val empty2NullPlan = plan

    val writerBucketSpec = bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)

      if (options.getOrElse(BucketingUtils.optionForHiveCompatibleBucketWrite, "false") ==
        "true") {
        // Hive bucketed table: use `HiveHash` and bitwise-and as bucket id expression.
        // Without the extra bitwise-and operation, we can get wrong bucket id when hash value of
        // columns is negative. See Hive implementation in
        // `org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils#getBucketNumber()`.
        val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
        val bucketIdExpression = Pmod(hashId, Literal(spec.numBuckets))

        // The bucket file name prefix is following Hive, Presto and Trino conversion, so this
        // makes sure Hive bucketed table written by Spark, can be read by other SQL engines.
        //
        // Hive: `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`.
        // Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`.
        val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
        WriterBucketSpec(bucketIdExpression, fileNamePrefix)
      } else {
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` as bucket id
        // expression, so that we can guarantee the data distribution is same between shuffle and
        // bucketed data source, which enables us to only shuffle one side when join a bucketed
        // table and a normal one.
        val bucketIdExpression = HashPartitioning(bucketColumns, spec.numBuckets)
          .partitionIdExpression
        WriterBucketSpec(bucketIdExpression, (_: Int) => "")
      }
    }
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    DataSourceUtils.verifySchema(fileFormat, dataSchema)
    // Note: prepareWrite has side effect. It sets "job".
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new WriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = finalOutputSpec.outputPath,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
        .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers
    )

    val isCDC = caseInsensitiveOptions.getOrElse("isCDC", "false").toBoolean
    val isCompaction = caseInsensitiveOptions.getOrElse("isCompaction", "false").toBoolean

    // We should first sort by partition columns, then bucket id, and finally sorting columns.
    val requiredOrdering =
      partitionColumns ++ writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = empty2NullPlan.outputOrdering.map(_.child)
    val orderingMatched = if (isCompaction) {
      true
    } else if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    val nativeIOEnable = sparkSession.sessionState.conf.getConf(LakeSoulSQLConf.NATIVE_IO_ENABLE)
    def nativeWrap(plan: SparkPlan): RDD[InternalRow] = {
      if (isCompaction && !isCDC && nativeIOEnable) {
        plan match {
          case withPartitionAndOrdering(_, _, child) =>
            return nativeWrap(child)
          case _ =>
        }
        // in this case, we drop columnar to row
        // and use columnar batch directly as input
        // this takes effect no matter gluten enabled or not
        ArrowFakeRowAdaptor(plan match {
          case ColumnarToRowExec(child) => child
          case UnaryExecNode(plan, child)
            if plan.getClass.getName == "io.glutenproject.execution.VeloxColumnarToRowExec" => child
          case WholeStageCodegenExec(ColumnarToRowExec(child)) => child
          case WholeStageCodegenExec(ProjectExec(_, child)) => child
          case _ => plan
        }).execute()
      } else {
        plan.execute()
      }
    }

    try {
      // for compaction, we won't break ordering from batch scan
      val (rdd, concurrentOutputWriterSpec) = if (orderingMatched || isCompaction) {
        (nativeWrap(empty2NullPlan), None)
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        val orderingExpr = bindReferences(
          requiredOrdering.map(SortOrder(_, Ascending)), finalOutputSpec.outputColumns)
        val sortPlan = SortExec(
          orderingExpr,
          global = false,
          child = empty2NullPlan)

        val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
        val concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
        if (concurrentWritersEnabled) {
          (empty2NullPlan.execute(),
            Some(ConcurrentOutputWriterSpec(maxWriters, () => sortPlan.createSorter())))
        } else {
          (nativeWrap(sortPlan), None)
        }
      }

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        rdd
      }

      val jobIdInstant = new Date().getTime
      val ret = new Array[WriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[InternalRow]) => {
          executeTask(
            description = description,
            jobIdInstant = jobIdInstant,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec,
            bucketSpec,
            caseInsensitiveOptions
          )
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: WriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        })

      val commitMsgs = ret.map(_.commitMsg)

      logInfo(s"Start to commit write Job ${description.uuid}.")
      val (_, duration) = Utils.timeTakenMs {
        committer.commitJob(job, commitMsgs)
      }
      logInfo(s"Write Job ${description.uuid} committed. Elapsed time: $duration ms.")

      processStats(description.statsTrackers, ret.map(_.summary.stats), duration)
      logInfo(s"Finished processing stats for write job ${description.uuid}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${description.uuid}.", cause)
        committer.abortJob(job)
        throw QueryExecutionErrors.jobAbortedError(cause)
    }
  }

  /** Writes data out in a single Spark task. */
  private def executeTask(
                           description: WriteJobDescription,
                           jobIdInstant: Long,
                           sparkStageId: Int,
                           sparkPartitionId: Int,
                           sparkAttemptNumber: Int,
                           committer: FileCommitProtocol,
                           iterator: Iterator[InternalRow],
                           concurrentOutputWriterSpec: Option[ConcurrentOutputWriterSpec],
                           bucketSpec: Option[BucketSpec],
                           options: Map[String, String]
                         ): WriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(new Date(jobIdInstant), sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val isCompaction = options.getOrElse("isCompaction", "false").toBoolean

    val dataWriter =
      if (!iterator.hasNext) {
        new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty && !isCompaction) {
        new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (isCompaction) {
        new StaticPartitionedDataWriter(description, taskAttemptContext, committer, options, sparkPartitionId, bucketSpec)
      } else {
        concurrentOutputWriterSpec match {
          case Some(spec) =>
            new DynamicPartitionDataConcurrentWriter(
              description, taskAttemptContext, committer, spec)
          case _ =>
            new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
        }
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        dataWriter.writeWithIterator(iterator)
        dataWriter.commit()
      })(catchBlock = {
        // If there is an error, abort the task
        dataWriter.abort()
        logError(s"Job $jobId aborted.")
      }, finallyBlock = {
        dataWriter.close()
      })
    } catch {
      case e: FetchFailedException =>
        throw e
      case f: FileAlreadyExistsException if SQLConf.get.fastFailFileFormatOutput =>
        // If any output file to write already exists, it does not make sense to re-run this task.
        // We throw the exception and let Executor throw ExceptionFailure to abort the job.
        throw new TaskOutputFileAlreadyExistException(f)
      case t: Throwable =>
        throw QueryExecutionErrors.taskFailedWhileWritingRowsError(t)
    }
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  private[datasources] def processStats(
                                         statsTrackers: Seq[WriteJobStatsTracker],
                                         statsPerTask: Seq[Seq[WriteTaskStats]],
                                         jobCommitDuration: Long)
  : Unit = {

    val numStatsTrackers = statsTrackers.length
    assert(statsPerTask.forall(_.length == numStatsTrackers),
      s"""Every WriteTask should have produced one `WriteTaskStats` object for every tracker.
         |There are $numStatsTrackers statsTrackers, but some task returned
         |${statsPerTask.find(_.length != numStatsTrackers).get.length} results instead.
       """.stripMargin)

    val statsPerTracker = if (statsPerTask.nonEmpty) {
      statsPerTask.transpose
    } else {
      statsTrackers.map(_ => Seq.empty)
    }

    statsTrackers.zip(statsPerTracker).foreach {
      case (statsTracker, stats) => statsTracker.processStats(stats, jobCommitDuration)
    }
  }

  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
                         outputPath: String,
                         customPartitionLocations: Map[TablePartitionSpec, String],
                         outputColumns: Seq[Attribute])

  private class StaticPartitionedDataWriter(
                                   description: WriteJobDescription,
                                   taskAttemptContext: TaskAttemptContext,
                                   committer: FileCommitProtocol,
                                   options: Map[String, String],
                                   partitionId: Int,
                                   bucketSpec: Option[BucketSpec],
                                   customMetrics: Map[String, SQLMetric] = Map.empty)
    extends FileFormatDataWriter(description, taskAttemptContext, committer, customMetrics) {
    private var fileCounter: Int = _
    private var recordsInFile: Long = _
    private val partValue: Option[String] = options.get("partValue").filter(_ != LAKESOUL_NON_PARTITION_TABLE_PART_DESC)
      .map(_.replace(LAKESOUL_RANGE_PARTITION_SPLITTER, "/"))

    private def newOutputWriter(): Unit = {
      recordsInFile = 0
      releaseResources()

      val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
      val suffix = if (bucketSpec.isDefined) {
        val bucketIdStr = BucketingUtils.bucketIdToString(partitionId)
        f"$bucketIdStr.c$fileCounter%03d" + ext
      } else {
        f"-c$fileCounter%03d" + ext
      }

      val currentPath = committer.newTaskTempFile(
        taskAttemptContext,
        partValue,
        suffix)

      currentWriter = description.outputWriterFactory.newInstance(
        path = currentPath,
        dataSchema = description.dataColumns.toStructType,
        context = taskAttemptContext)

      statsTrackers.foreach(_.newFile(currentPath))
    }

    override def write(record: InternalRow): Unit = {
      if (currentWriter == null) {
        newOutputWriter()
      } else if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
        fileCounter += 1
        assert(fileCounter < MAX_FILE_COUNTER,
          s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

        newOutputWriter()
      }

      currentWriter.write(record)
      statsTrackers.foreach(_.newRow(currentWriter.path, record))
      recordsInFile += 1
    }
  }
}
