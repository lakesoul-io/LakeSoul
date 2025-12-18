package org.apache.spark.sql.lakesoul.entry

import org.apache.spark.sql.SparkSession

import scala.sys.exit

object SqlSubmitter {
  val sqlFilePathParam = "sql-file"
  val scheduleTimeParam = "scheduleTime"

  def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
    val pathParam = s"--$sqlFilePathParam"
    val timeParam = s"--$scheduleTimeParam"
    list match {
      case Nil => map
      case `pathParam` :: value :: tail =>
        nextArg(map ++ Map(sqlFilePathParam -> value), tail)
      case `timeParam` :: value :: tail =>
        nextArg(map ++ Map(scheduleTimeParam -> value), tail)
      case unknown :: _ =>
        println("Unknown option " + unknown)
        exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    val usage = s"""
        Usage: spark-submit --class org.apache.spark.sql.lakesoul.entry.SqlSubmitter lakesoul-spark.jar --$sqlFilePathParam s3/hdfs --$scheduleTimeParam=timestamp_in_milliseconds
      """

    if (args.length == 0) {
      println(usage)
      exit(1)
    }

    val options = nextArg(Map(), args.toList)
    if (!options.contains(sqlFilePathParam) || !options.contains(scheduleTimeParam)) {
      println(usage)
      exit(1)
    }
    println(options)

    val spark = SparkSession.builder().getOrCreate()

    val sqlContent = spark.sparkContext.wholeTextFiles(options(sqlFilePathParam).toString).take(1)(0)._2
    println(s"==============SQL file content:\n$sqlContent\n============================\n")

    def isEmptyLine(sql: String): Boolean = {
      if (sql.isEmpty || sql.split("\n").iterator.forall(p => p.startsWith("--"))) true else false
    }

    val sqlStatement = sqlContent.split(";")
    sqlStatement.foreach(sql => {
      val sqlStr = sql.trim
      if (!isEmptyLine(sqlStr)) {
        val sqlReplaced = sqlStr.replaceAll("\\$\\{scheduleTime}", options(scheduleTimeParam).toString)
        println(s"==========Executing:\n$sqlReplaced\n=================================")
        spark.sql(sqlReplaced).show()
      } else {
        println(s"==========Ignoring comments:\n$sqlStr\n==============================")
      }
    })
  }
}
