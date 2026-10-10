package org.apache.spark.sql.lakesoul.entry

import org.apache.spark.sql.SparkSession

import scala.sys.exit

object SqlSubmitter {
  val sqlFilePathParam = "sql-file"
  val scheduleTimeParam = "scheduleTime"

  def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] =
    nextArg(
      map,
      list,
      message => {
        println(message)
        exit(1)
      }
    )

  private[lakesoul] def nextArg(
      map: Map[String, Any],
      list: List[String],
      onArgumentError: String => Nothing
  ): Map[String, Any] = {
    val pathParam = s"--$sqlFilePathParam"
    val timeParam = s"--$scheduleTimeParam"
    list match {
      case Nil         => map
      case arg :: tail =>
        splitOption(arg) match {
          case (`pathParam`, Some(value)) =>
            nextArg(map + (sqlFilePathParam -> value), tail, onArgumentError)
          case (`timeParam`, Some(value)) =>
            nextArg(map + (scheduleTimeParam -> value), tail, onArgumentError)
          case (`pathParam`, None) =>
            consumeValue(
              map,
              sqlFilePathParam,
              pathParam,
              tail,
              onArgumentError
            )
          case (`timeParam`, None) =>
            consumeValue(
              map,
              scheduleTimeParam,
              timeParam,
              tail,
              onArgumentError
            )
          case _ =>
            onArgumentError(unknownOptionMessage(arg))
        }
    }
  }

  /** Consumes the following argument as the value of ``name``. */
  private def consumeValue(
      map: Map[String, Any],
      key: String,
      name: String,
      tail: List[String],
      onArgumentError: String => Nothing
  ): Map[String, Any] =
    tail match {
      case value :: remaining =>
        nextArg(map + (key -> value), remaining, onArgumentError)
      case Nil => onArgumentError(s"Missing value for $name")
    }

  /** Splits ``--name=value`` into its name and inline value; ``--name value``
    * has no inline value.
    */
  private def splitOption(argument: String): (String, Option[String]) = {
    val separator = argument.indexOf('=')
    if (separator < 0) (argument, None)
    else
      (
        argument.substring(0, separator),
        Some(argument.substring(separator + 1))
      )
  }

  /** Names a rejected argument without echoing a value that may hold
    * credentials. Only arguments that look like options (``-...``) expose their
    * name up to ``=``; a bare value is reported generically because it cannot
    * be told apart from a secret.
    */
  private[lakesoul] def unknownOptionMessage(argument: String): String = {
    val name = argument.takeWhile(_ != '=')
    if (name.startsWith("-")) s"Unknown option $name" else "Unknown option"
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
    if (
      !options
        .contains(sqlFilePathParam) || !options.contains(scheduleTimeParam)
    ) {
      println(usage)
      exit(1)
    }
    println("SQL submission options parsed")

    val spark = SparkSession.builder().getOrCreate()

    val sqlContent = spark.sparkContext
      .wholeTextFiles(options(sqlFilePathParam).toString)
      .take(1)(0)
      ._2
    println("SQL file loaded")
    executeScript(
      sqlContent,
      options(scheduleTimeParam).toString,
      sql => {
        // SET and SHOW results can themselves contain credentials.
        spark.sql(sql).take(1)
        ()
      }
    )
  }

  private[lakesoul] def executeScript(
      sqlContent: String,
      scheduleTime: String,
      execute: String => Unit
  ): Unit = {
    def isEmptyLine(sql: String): Boolean = {
      if (
        sql.isEmpty || sql
          .split("\n")
          .iterator
          .map(_.trim)
          .forall(p => p.isEmpty || p.startsWith("--"))
      ) true
      else false
    }

    val sqlStatement = sqlContent.split(";")
    sqlStatement.zipWithIndex.foreach { case (sql, index) =>
      val sqlStr = sql.trim
      if (!isEmptyLine(sqlStr)) {
        val sqlReplaced = sqlStr.replaceAll(
          "\\$\\{scheduleTime}",
          scheduleTime
        )
        // Do not log SQL text or comments: credentials may appear anywhere in the script.
        println(s"Executing SQL statement #${index + 1}")
        execute(sqlReplaced)
      } else {
        println(s"Ignoring empty/comment SQL statement #${index + 1}")
      }
    }
  }
}
