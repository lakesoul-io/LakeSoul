-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE table_info
    ADD COLUMN IF NOT EXISTS table_schema_arrow_ipc bytea;
ALTER TABLE table_info
    ADD COLUMN IF NOT EXISTS table_schema_arrow_ipc_json_hash text;
ALTER TABLE data_commit_info REPLICA IDENTITY FULL;
