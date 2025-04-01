/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.utils

object CommonConstantsAndUtils {
  val separator = '\t'

  case class NameTermValue(name: String, term: String, value: Double)

  def getFeatureKey(name: String, term: String, separator: Char = separator): String = {
    name + separator + term
  }
}