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

package org.apache.spark.sql.lakesoul.material_view

import org.apache.spark.sql.types.DataType

trait ConstructProperties {

  def addRangeInfo(dataType: DataType, colName: String, limit: Any, rangeType: String): Unit

  def addConditionEqualInfo(left: String, right: String): Unit

  def addColumnEqualInfo(left: String, right: String): Unit

  def addConditionOrInfo(orInfo: OrInfo): Unit

  def addOtherInfo(info: String): Unit

}
