package org.apache.spark.sql.lakesoul

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class TestListener extends SparkListener with Logging {
    override def onApplicationStart(applicationStart:SparkListenerApplicationStart): Unit = {
        println("onApplicationStart")
    }

    /**
     * 一个Application可能会有多个job，一个action操作就是一个job
     * 所以如果代码中，执行了count和save，那么起码会有2个job
     * 每个job又有各自的stage
     * @param jobStart
     */
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        println("onJobStart")
        println("该任务总共有 : " + jobStart.stageIds.length+" 个stage")
        println("properties = "+ jobStart.properties)
    }

    /**
     * 第一个stage提交，task开始，task结束，stage结束
     * 然后开始下一个stage的提交。。。
     * 如果有多个task，会执行多次task开始和task结束
     */
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

//        println("[onStageSubmitted]stageId = "+stageSubmitted.stageInfo.stageId + " 已经提交")

    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
//        println(s"-----------metrics of task ${taskStart.taskInfo.taskId} onTaskStart begin----------")
//
//        println("在executorld = "+taskStart.taskInfo.executorId + "(" +taskStart.taskInfo.host +") " +
//                "上启动了一个task = "+taskStart.taskInfo.taskId +"," +
//                "taskLocality = "+taskStart.taskInfo.taskLocality)
//        println(s"-----------metrics of task ${taskStart.taskInfo.taskId} onTaskStart end----------\n")
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
//        if (taskEnd.taskType != "ResultTask") return
        println(s"-----------metrics of task ${taskEnd.taskInfo.taskId} onTaskEnd begin----------")
        //区分task归属的stageId，taskid全局递增，不同stage的taskid也不会重复
        println("stageId = "+taskEnd.stageId+",taskId = "+taskEnd.taskInfo.taskId)
        println("TaskType = " + taskEnd.taskType)
        /**
         * taskinfo中会有一些internal的metrics
         * 如果TaskType=ShuffleMapTask，证明是上游的task，那么就会有
         * internal.metrics.shuffle.write.recordsWritten = 9
         * internal.metrics.shuffle.write.bytesWritten = 636
         * 如果TaskType=ResultTask，那就是下游的task，那么就会有
         * internal.metrics.output.recordsWritten = 3
         * internal.metrics.output.bytesWritten = 22
         * internal.metrics.shuffle.read.recordsRead = 3
         * 这些指标均是每个task处理的数据量的情况
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
