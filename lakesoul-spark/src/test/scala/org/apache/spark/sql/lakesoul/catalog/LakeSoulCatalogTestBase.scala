// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.catalog

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SQLTestUtils

abstract class LakeSoulCatalogTestBase extends QueryTest
  with SQLTestUtils{

}