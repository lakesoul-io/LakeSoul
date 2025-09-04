// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import org.apache.spark.sql.lakesoul.commands.DeleteSQLSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeleteSQLGlutenTestSuite extends DeleteSQLSuite with LakeSoulSQLCommandGlutenTest {

}
