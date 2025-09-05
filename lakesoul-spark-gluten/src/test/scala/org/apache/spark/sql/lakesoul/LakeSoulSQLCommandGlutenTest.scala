// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import org.apache.gluten.config.GlutenConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}

trait LakeSoulSQLCommandGlutenTest
  extends QueryTest
    with SharedSparkSession
    with LakeSoulSQLCommandTest {
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.adaptive.logLevel", "info")
      .set("spark.network.timeout", "10000000")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "false")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .set(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
  }

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new TestSparkSession(sparkConf)
    session.sparkContext.setLogLevel("INFO")
    session
  }
}
