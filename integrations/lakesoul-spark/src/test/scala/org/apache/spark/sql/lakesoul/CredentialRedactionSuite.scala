// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import java.io.ByteArrayOutputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.lakesoul.entry.SqlSubmitter
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class CredentialRedactionSuite extends AnyFunSuite {
  test(
    "property mismatch errors redact both specified and existing credentials"
  ) {
    val specified = Map(
      "spark.hadoop.fs.s3a.access.key" -> "new-access-sentinel",
      "s3.secret-key" -> "new-secret-sentinel",
      "fs.s3a.endpoint" -> "http://localhost:9000"
    )
    val existing = Map(
      "fs.s3a.access.key" -> "old-access-sentinel",
      "fs.s3a.secret.key" -> "old-secret-sentinel",
      "lakesoul.pg.password" -> "password-sentinel"
    )
    val message = LakeSoulErrors
      .createTableWithDifferentPropertiesException(
        new Path("s3a://test-bucket/table"),
        specified,
        existing
      )
      .getMessage

    Seq(
      "new-access-sentinel",
      "new-secret-sentinel",
      "old-access-sentinel",
      "old-secret-sentinel",
      "password-sentinel"
    ).foreach(secret => assert(!message.contains(secret)))
    assert(message.contains("spark.hadoop.fs.s3a.access.key=[REDACTED]"))
    assert(message.contains("fs.s3a.secret.key=[REDACTED]"))
    assert(message.contains("http://localhost:9000"))
    assert(specified("s3.secret-key") == "new-secret-sentinel")
    assert(existing("fs.s3a.secret.key") == "old-secret-sentinel")
  }

  test("SQL submission logs neither SQL literals nor comments") {
    val script =
      "-- comment-secret-sentinel;\n" +
        "SET spark.hadoop.fs.s3a.access.key=access-sentinel;\n" +
        "SET spark.hadoop.fs.s3a.secret.key=secret-sentinel;\n" +
        "SELECT '${scheduleTime}';"
    val executed = ArrayBuffer.empty[String]
    val output = new ByteArrayOutputStream()

    Console.withOut(output) {
      SqlSubmitter.executeScript(
        script,
        "42",
        sql => {
          executed += sql
          ()
        }
      )
    }

    val logged = output.toString("UTF-8")
    Seq("comment-secret-sentinel", "access-sentinel", "secret-sentinel")
      .foreach(secret => assert(!logged.contains(secret)))
    assert(logged.contains("Executing SQL statement #2"))
    assert(logged.contains("Executing SQL statement #4"))
    assert(executed.size == 3)
    assert(executed.head.contains("access-sentinel"))
    assert(executed(1).contains("secret-sentinel"))
    assert(executed.last == "SELECT '42'")
    assert(script.contains("${scheduleTime}"))
  }

  test("argument parsing names the unknown option without echoing its value") {
    val reported = intercept[IllegalArgumentException] {
      parseOptions("--sql-file", "path", "--passwrod=secret-sentinel")
    }
    assert(reported.getMessage === "Unknown option --passwrod")
    assert(!reported.getMessage.contains("secret-sentinel"))
  }

  test("argument parsing accepts both spaced and inline values") {
    val spaced = parseOptions(
      "--sql-file",
      "s3://bucket/table.sql",
      "--scheduleTime",
      "42"
    )
    val inline = parseOptions(
      "--sql-file=s3://bucket/table.sql",
      "--scheduleTime=42"
    )
    val mixed = parseOptions(
      "--sql-file=s3://bucket/table.sql",
      "--scheduleTime",
      "42"
    )
    assert(spaced(SqlSubmitter.sqlFilePathParam) === "s3://bucket/table.sql")
    assert(spaced(SqlSubmitter.scheduleTimeParam) === "42")
    assert(inline === spaced)
    assert(mixed === spaced)
  }

  test("inline values may contain equals signs") {
    val options = parseOptions(
      "--sql-file=s3://bucket/x.sql?token=a=b",
      "--scheduleTime=42"
    )
    assert(
      options(SqlSubmitter.sqlFilePathParam) === "s3://bucket/x.sql?token=a=b"
    )
  }

  test("a known option without a value is reported by name") {
    val reported = intercept[IllegalArgumentException] {
      parseOptions("--sql-file")
    }
    assert(reported.getMessage === "Missing value for --sql-file")
  }

  test("unknown option messages keep names and hide values") {
    assert(
      SqlSubmitter.unknownOptionMessage(
        "--passwrod"
      ) === "Unknown option --passwrod"
    )
    assert(
      SqlSubmitter.unknownOptionMessage(
        "--passwrod=secret-sentinel"
      ) === "Unknown option --passwrod"
    )
    assert(
      SqlSubmitter.unknownOptionMessage(
        "-Ds3.secret.key=secret-sentinel"
      ) === "Unknown option -Ds3.secret.key"
    )
    // A bare argument is not known to be an option, so it is never echoed back.
    val bare = SqlSubmitter.unknownOptionMessage("secret-sentinel")
    assert(bare === "Unknown option")
    assert(!bare.contains("secret-sentinel"))
  }

  test("empty and comment-only scripts do not execute statements") {
    val output = new ByteArrayOutputStream()
    Console.withOut(output) {
      SqlSubmitter.executeScript(
        " \n-- secret-comment-sentinel",
        "42",
        _ => {
          fail("a comment-only script must not execute")
        }
      )
    }
    assert(!output.toString("UTF-8").contains("secret-comment-sentinel"))
  }

  private def parseOptions(arguments: String*): Map[String, Any] =
    SqlSubmitter.nextArg(
      Map(),
      arguments.toList,
      message => throw new IllegalArgumentException(message)
    )
}
