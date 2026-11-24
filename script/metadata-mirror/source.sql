-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
-- SPDX-License-Identifier: Apache-2.0

BEGIN;

ALTER TABLE public.table_info REPLICA IDENTITY FULL;
ALTER TABLE public.partition_info REPLICA IDENTITY FULL;
ALTER TABLE public.data_commit_info REPLICA IDENTITY FULL;

CREATE PUBLICATION lakesoul_meta_publication
    FOR TABLE public.table_info, public.partition_info, public.data_commit_info
    WITH (publish = 'insert,update,delete');

COMMIT;
