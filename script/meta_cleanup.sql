-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

delete from namespace;
insert into namespace(namespace, properties, comment) values ('default', '{}', '');
delete from data_commit_info;
delete from table_info;
delete from table_path_id;
delete from table_name_id;
delete from partition_info;
delete from discard_compressed_file_info;
delete from index_shard;
delete from table_snapshot_tag;
delete from snapshot_commit;
delete from table_snapshot;
delete from subscription.mysql_state;
delete from subscription.progress;
delete from subscription.tables;
delete from subscription.subscriptions;
delete from federation.foreign_tables;
delete from federation.user_mappings;
delete from federation.servers;
delete from job.queries;
delete from ivm.view_locks;
delete from ivm.view_commits;
delete from ivm.view_partitions;
delete from ivm.consumers;
delete from ivm.states;
delete from ivm.epochs;
delete from ivm.cursors;
delete from ivm.views;
