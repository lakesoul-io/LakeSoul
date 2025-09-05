// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import org.apache.spark.sql.lakesoul.commands.MergeIntoSQLSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MergeIntoSQLGlutenTestSuite extends MergeIntoSQLSuite with LakeSoulSQLCommandGlutenTest {

}
