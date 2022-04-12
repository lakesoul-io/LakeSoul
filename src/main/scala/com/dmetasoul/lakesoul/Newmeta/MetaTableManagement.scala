/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.Newmeta
object MetaTableManagement {
  private val cassandraConnector = MetaCommon.cassandraConnector
  private val database = MetaCommon.DATA_BASE

  def initDatabaseAndTables(): Unit = {
    initDatabase()
    initTableInfo()
    initPartitionInfo()
    initDataInfo()
    initFragmentValue()
    initUndoLog()
    initLockInfo()
    initStreamingInfo()
    initTableRelation()
    initMaterialRelation()
    initMaterialView()
  }

  def initDatabase(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS $database
           |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
           |AND durable_writes = true;
        """.stripMargin)
    })
  }

  def initTableInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.table_info")
      session.execute(
        s"""
           |CREATE TABLE $database.table_info (
           |  table_name text,
           |	table_id text,
           |	table_schema text,
           |	range_column text,
           |	hash_column text,
           |	setting map<text,text>,
           |	read_version int,
           |	pre_write_version int,
           |	bucket_num int,
           |  short_table_name text,
           |  is_material_view boolean,
           |	PRIMARY KEY (table_name)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'meta service table version control'
        """.stripMargin)
    })
  }

  def initPartitionInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.partition_info")
      session.execute(
        s"""
           |CREATE TABLE $database.partition_info (
           |	table_id text,
           |  range_value text,
           |	range_id text,
           |  table_name text,
           |	read_version bigint,
           |	pre_write_version bigint,
           |	last_update_timestamp bigint,
           |	delta_file_num int,
           |	be_compacted boolean,
           |	PRIMARY KEY (table_id,range_value)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'meta service partition Info control'
        """.stripMargin)
    })
  }

  def initDataInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.data_info")
      session.execute(
        s"""
           |CREATE TABLE $database.data_info (
           |  table_id text,
           |  range_id text,
           |	file_path text,
           |	hash_value map<text,text>,
           |	write_version bigint,
           |	expire_version bigint,
           |	commit_id text,
           |	size bigint,
           |	modification_time bigint,
           |	file_exist_cols text,
           |	is_base_file boolean,
           |	PRIMARY KEY ((table_id,range_id), file_path)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'meta service table data info'
        """.stripMargin)
    })
  }

  def initUndoLog(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.undo_log")
      session.execute(
        s"""
           |CREATE TABLE $database.undo_log (
           |	commit_type text,
           |  table_id text,
           |	commit_id text,
           |  range_id text,
           |	file_path text,
           |  table_name text,
           |  range_value text,
           |	tag int,
           |	write_version bigint,
           |	timestamp bigint,
           |	size bigint,
           |  modification_time bigint,
           |  table_schema text,
           |  setting map<text,text>,
           |  file_exist_cols text,
           |	delta_file_num int,
           |	be_compacted boolean,
           |	is_base_file boolean,
           |	query_id text,
           |  batch_id bigint,
           |  short_table_name text,
           |  sql_text text,
           |  relation_tables text,
           |  auto_update boolean,
           |  is_creating_view boolean,
           |  view_info text,
           |	PRIMARY KEY ((commit_type,table_id), commit_id, range_id, file_path)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'meta service table data info'
        """.stripMargin)
    })
  }

  def initFragmentValue(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.fragment_value")
      session.execute(
        s"""
           |CREATE TABLE $database.fragment_value (
           |  table_id text,
           |	key_id text,
           |  value text,
           |	timestamp bigint,
           |	PRIMARY KEY (table_id,key_id)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'split large column value into fragment values'
        """.stripMargin)
    })
  }

  def initLockInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.lock_info")
      session.execute(
        s"""
           |CREATE TABLE $database.lock_info (
           |  lock_id text,
           |	commit_id text,
           |	PRIMARY KEY (lock_id)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'meta service lock info'
        """.stripMargin)
    })
  }

  def initStreamingInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.streaming_info")
      session.execute(
        s"""
           |CREATE TABLE $database.streaming_info (
           |  table_id text,
           |	query_id text,
           |  batch_id bigint,
           |	timestamp bigint,
           |	PRIMARY KEY (table_id,query_id)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'record streaming info for recovery'
        """.stripMargin)
    })
  }


  def initTableRelation(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.table_relation")
      session.execute(
        s"""
           |CREATE TABLE $database.table_relation (
           |  short_table_name text,
           |	table_name text,
           |	PRIMARY KEY (short_table_name)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'matching short table name'
        """.stripMargin)
    })
  }

  def initMaterialView(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.material_view")
      session.execute(
        s"""
           |CREATE TABLE $database.material_view (
           |  view_name text,
           |	table_name text,
           |  table_id text,
           |  relation_tables text,
           |  sql_text text,
           |  auto_update boolean,
           |  view_info text,
           |	PRIMARY KEY (view_name)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'material view info'
        """.stripMargin)
    })
  }

  def initMaterialRelation(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(s"drop table if exists $database.material_relation")
      session.execute(
        s"""
           |CREATE TABLE $database.material_relation (
           |  table_id text,
           |  table_name text,
           |  material_views text,
           |	PRIMARY KEY (table_id)
           |) WITH bloom_filter_fp_chance = 0.01
           |    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
           |    AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 32}
           |    AND gc_grace_seconds = 3600
           |    AND comment = 'material views of table'
        """.stripMargin)
    })
  }

  def cleanTableInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.table_info
        """.stripMargin)
    })
  }

  def cleanPartitionVersion(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.partition_info
        """.stripMargin)
    })
  }

  def cleanDataInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.data_info
        """.stripMargin)
    })
  }

  def cleanUndoLog(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.undo_log
        """.stripMargin)
    })
  }

  def cleanFragmentValue(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.fragment_value
        """.stripMargin)
    })
  }

  def cleanLockInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.lock_info
        """.stripMargin)
    })
  }

  def cleanStreamingInfo(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.streaming_info
        """.stripMargin)
    })
  }

  def cleanTableRelation(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.table_relation
        """.stripMargin)
    })
  }

  def cleanMaterialView(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.material_view
        """.stripMargin)
    })
  }

  def cleanMaterialRelation(): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |truncate $database.material_relation
        """.stripMargin)
    })
  }

}
