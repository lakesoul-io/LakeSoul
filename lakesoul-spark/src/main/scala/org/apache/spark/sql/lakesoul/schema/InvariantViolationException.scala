// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.schema

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

/** Thrown when the given data doesn't match the rules defined on the table. */
case class InvariantViolationException(msg: String) extends IllegalArgumentException(msg)

object InvariantViolationException {
  def apply(invariant: Invariant,
            msg: String): InvariantViolationException = {
    new InvariantViolationException(s"Invariant ${invariant.rule.name} violated for column: " +
      s"${UnresolvedAttribute(invariant.column).name}.\n$msg")
  }
}
