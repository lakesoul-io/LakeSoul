#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${LAKESOUL_MIRROR_DESTINATION_URL:?Set a disposable postgres-lakesoul URL for an existing namespace}"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mirror_source_url=$(python3 "$script_dir/source_url.py")

source_sql() {
  psql -X -v ON_ERROR_STOP=1 -d "$mirror_source_url" "$@"
}
mirror_sql() {
  psql -X -v ON_ERROR_STOP=1 -d "$LAKESOUL_MIRROR_DESTINATION_URL" "$@"
}
mirror_query() {
  mirror_sql -qAt -c '\connect lakesoul_meta' -c "$1"
}
wait_for_value() {
  local query=$1 expected=$2 actual deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    actual=$(mirror_query "$query")
    if [[ "$actual" == "$expected" ]]; then
      return
    fi
    sleep 1
  done
  printf 'Mirror did not converge for: %s\nExpected: %s\nActual: %s\n' \
    "$query" "$expected" "$actual" >&2
  return 1
}

source_sql -f "$script_dir/source.sql"

source_sql <<'SQL'
INSERT INTO public.table_info (table_id, table_namespace, table_name, table_path)
VALUES ('metadata_mirror_smoke', 'default', 'mirror_smoke', 'file:///mirror_smoke');

INSERT INTO public.data_commit_info
    (table_id, partition_desc, commit_id, file_ops, commit_op, committed, timestamp)
VALUES
    ('metadata_mirror_smoke', '-5', '00000000-0000-0000-0000-000000000002',
     ARRAY[ROW('file:///old', 'add', 3, 'id')::public.data_file_op],
     'AppendCommit', true, 1),
    ('metadata_mirror_smoke', '-5', '00000000-0000-0000-0000-000000000001',
     ARRAY[
         ROW('file:///old', 'del', 3, 'id')::public.data_file_op,
         ROW('file:///new,part', 'add', 8, 'id,value')::public.data_file_op
     ],
     'MergeCommit', true, 2);

INSERT INTO public.partition_info (table_id, partition_desc, version, commit_op, snapshot)
VALUES ('metadata_mirror_smoke', '-5', 0, 'AppendCommit', ARRAY[
    '00000000-0000-0000-0000-000000000002'::uuid,
    '00000000-0000-0000-0000-000000000001'::uuid
]);
SQL

lakesoul_mirror_source_dsn="$mirror_source_url" mirror_sql -f "$script_dir/subscribe.sql"

table_filter="table_id = 'metadata_mirror_smoke'"
partition_filter="$table_filter AND partition_desc = '-5'"
wait_for_value "SELECT table_path FROM table_info WHERE $table_filter" 'file:///mirror_smoke'
wait_for_value "SELECT snapshot FROM partition_info WHERE $partition_filter AND version = 0" \
  '["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000001"]'
wait_for_value "SELECT count(*) FROM data_commit_info WHERE $partition_filter" 2
mirror_query "SELECT file_ops FROM data_commit_info WHERE $partition_filter AND commit_id = '00000000-0000-0000-0000-000000000001'" |
  python3 -c 'import json, sys; assert json.load(sys.stdin) == [
    {"path": "file:///old", "file_op": "del", "size": 3, "file_exist_cols": "id"},
    {"path": "file:///new,part", "file_op": "add", "size": 8, "file_exist_cols": "id,value"}
]'

source_sql <<'SQL'
INSERT INTO public.data_commit_info
    (table_id, partition_desc, commit_id, file_ops, commit_op, committed, timestamp)
VALUES ('metadata_mirror_smoke', '-5', '00000000-0000-0000-0000-000000000003',
        ARRAY[]::public.data_file_op[], 'AppendCommit', false, 3);
SQL

commit_filter="$partition_filter AND commit_id = '00000000-0000-0000-0000-000000000003'"
wait_for_value "SELECT count(*) FROM data_commit_info WHERE $commit_filter AND NOT committed" 1

source_sql <<'SQL'
BEGIN;
UPDATE public.table_info SET table_path = 'file:///mirror_smoke_updated'
WHERE table_id = 'metadata_mirror_smoke';

UPDATE public.data_commit_info SET committed = true
WHERE table_id = 'metadata_mirror_smoke' AND partition_desc = '-5'
  AND commit_id = '00000000-0000-0000-0000-000000000003';

INSERT INTO public.partition_info (table_id, partition_desc, version, commit_op, snapshot)
VALUES ('metadata_mirror_smoke', '-5', 1, 'AppendCommit', ARRAY[
    '00000000-0000-0000-0000-000000000002'::uuid,
    '00000000-0000-0000-0000-000000000001'::uuid,
    '00000000-0000-0000-0000-000000000003'::uuid
]);
COMMIT;
SQL

wait_for_value "SELECT table_path FROM table_info WHERE $table_filter" 'file:///mirror_smoke_updated'
wait_for_value "SELECT count(*) FROM data_commit_info WHERE $commit_filter AND committed" 1
wait_for_value "SELECT count(*) FROM data_commit_info WHERE $commit_filter" 1
wait_for_value "SELECT file_ops FROM data_commit_info WHERE $commit_filter" '[]'
wait_for_value "SELECT snapshot FROM partition_info WHERE $partition_filter AND version = 1" \
  '["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000001","00000000-0000-0000-0000-000000000003"]'
wait_for_value "SELECT count(*) FROM partition_info WHERE $partition_filter" 2

source_sql <<'SQL'
BEGIN;
DELETE FROM public.partition_info
WHERE table_id = 'metadata_mirror_smoke' AND partition_desc = '-5'
  AND version = 1;
DELETE FROM public.data_commit_info
WHERE table_id = 'metadata_mirror_smoke' AND partition_desc = '-5'
  AND commit_id = '00000000-0000-0000-0000-000000000003';
COMMIT;
SQL

wait_for_value "SELECT count(*) FROM partition_info WHERE $partition_filter AND version = 1" 0
wait_for_value "SELECT count(*) FROM data_commit_info WHERE $commit_filter" 0
wait_for_value "SELECT count(*) FROM partition_info WHERE $partition_filter AND version = 0" 1
wait_for_value "SELECT count(*) FROM data_commit_info WHERE $partition_filter" 2
printf 'LakeSoul metadata mirror smoke test passed.\n'
