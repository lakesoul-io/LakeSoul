package org.apache.spark.sql.lakesoul

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class TestListener extends SparkListener with Logging {
    override def onApplicationStart(applicationStart:SparkListenerApplicationStart): Unit = {
        println("onApplicationStart")
    }

    /**
     * @param jobStart
     */
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        println("onJobStart")
        println("number of stage is " + jobStart.stageIds.length)
        println("properties = "+ jobStart.properties)
    }

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

//        println("[onStageSubmitted]stageId = "+stageSubmitted.stageInfo.stageId + " submitted")

    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        println(s"-----------metrics of task ${taskEnd.taskInfo.taskId} onTaskEnd begin----------")
        println("stageId = "+taskEnd.stageId+",taskId = "+taskEnd.taskInfo.taskId)
        println("TaskType = " + taskEnd.taskType)
        /**
         * if TaskType=ShuffleMapTask
         * internal.metrics.shuffle.write.recordsWritten = 9
         * internal.metrics.shuffle.write.bytesWritten = 636
          *
         * if TaskType=ResultTask
         * internal.metrics.output.recordsWritten = 3
         * internal.metrics.output.bytesWritten = 22
         * internal.metrics.shuffle.read.recordsRead = 3
         */
        taskEnd.taskInfo.accumulables.foreach(
                task => {
                println(task.name.getOrElse("") +" = "+ task.value.getOrElse(""))
        }
    )
        println(s"-----------metrics of task ${taskEnd.taskInfo.taskId} onTaskEnd end----------\n")
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        println(s"[onStageCompleted]stage_id = ${stageCompleted.stageInfo.stageId} for ${stageCompleted.stageInfo.completionTime.get-stageCompleted.stageInfo.submissionTime.get}")
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        println(s"[onJobEnd] job_id = ${jobEnd.jobId}")
    }
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        println("onApplicationEnd")
    }

}
