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

package com.dmetasoul.lakesoul.meta
import java.util.Locale

object CommitState extends Enumeration {
  val Committing = Value("Committing")
  val RollBacking = Value("RollBacking")
  val CommitTimeout = Value("CommitTimeout")
  val RollBackTimeout = Value("RollBackTimeout")


  //Clean state is rarely appear, in which commit type undo log has been delete, but undo log with other type exist
  val Clean = Value("Clean")

  //in these state, commit should be performed successfully, while rollback is forbidden
  val Redoing = Value("Redoing")
  val RedoTimeout = Value("RedoTimeout")
}

object UndoLogType extends Enumeration {
  val Commit = Value("commit")
  val Partition = Value("partition")
  val AddFile = Value("addFile")
  val ExpireFile = Value("expireFile")
  val Schema = Value("schema")
  val ShortTableName = Value("shortTableName")
  val DropTable = Value("dropTable")
  val DropPartition = Value("dropPartition")

  def getAllType: Seq[String] = {
    Seq(
      Commit.toString,
      Partition.toString,
      AddFile.toString,
      ExpireFile.toString,
      Schema.toString,
      ShortTableName.toString,
      DropTable.toString,
      DropPartition.toString)
  }
}

sealed abstract class CommitType {
  def name: String
}

object CommitType {
  def apply(typ: String): CommitType = typ.toLowerCase(Locale.ROOT) match {
    case "append" | "AppendCommit" => AppendCommit
    case "merge" | "MergeCommit" => MergeCommit
    case "compaction" | "compact" | "CompactionCommit" => CompactionCommit
    case "update" | "UpdateCommit" => UpdateCommit
    case _ =>
      val supported = Seq("simple", "delta", "compaction", "compact", "part_compaction", "part_compact")
      throw new IllegalArgumentException(s"Unsupported commit type '$typ'. " +
        "Supported commit types include: " + supported.mkString("'", "', '", "'") + ".")
  }
}

case object AppendCommit extends CommitType {
  override def name: String = "AppendCommit"
}

case object MergeCommit extends CommitType {
  override def name: String = "MergeCommit"
}

case object CompactionCommit extends CommitType {
  override def name: String = "CompactionCommit"
}

case object UpdateCommit extends CommitType {
  override def name: String = "UpdateCommit"
}