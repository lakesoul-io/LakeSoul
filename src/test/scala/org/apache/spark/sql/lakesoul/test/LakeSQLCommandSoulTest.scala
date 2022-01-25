/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul.test

import com.dmetasoul.lakesoul.meta.MetaVersion
import com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.util.Utils

import java.io.File


trait LakeSoulTestUtils extends Logging {
  self: SharedSparkSession =>

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set(LakeSoulSQLConf.META_DATABASE_NAME.key, "lakesoul_meta")
    session.sparkContext.setLogLevel("ERROR")
    session
  }

  override def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
        val lakeSoulName = if (name.startsWith("lakesoul.")) name else s"lakesoul.$name"
        spark.sql(s"DROP TABLE IF EXISTS $lakeSoulName")
      }
    }
  }

  override def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try {
      f(dir)
      waitForTasksToFinish()
    } finally {
      Utils.deleteRecursively(dir)
      try {
        LakeSoulTable.forPath(dir.getCanonicalPath).dropTable()
      } catch {
        case e: Exception =>
      }
    }
  }
}

/**
  * Because `TestSparkSession` doesn't pick up the conf `spark.sql.extensions` in Spark 2.4.x, we use
  * this class to inject LakeSoul's extension in our tests.
  *
  * @see https://issues.apache.org/jira/browse/SPARK-25003
  */
class LakeSoulTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new LakeSoulSparkSessionExtension().apply(extensions)
    extensions
  }
}

/**
  * A trait for tests that are testing a fully set up SparkSession with all of LakeSoul's requirements,
  * such as the configuration of the LakeSoulCatalog and the addition of all LakeSoul extensions.
  */
trait LakeSQLCommandSoulTest extends LakeSoulTestUtils {
  self: SharedSparkSession =>

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set(LakeSoulSQLConf.META_DATABASE_NAME.key, "lakesoul_meta")
    session.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[LakeSoulCatalog].getName)
    session.sparkContext.setLogLevel("ERROR")

    session
  }
}

