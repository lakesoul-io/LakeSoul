// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.exception.MetaException
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataOperationSuite extends QueryTest
  with SharedSparkSession with LakeSoulTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }
}
