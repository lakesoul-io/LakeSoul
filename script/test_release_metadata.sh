#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

baseline_tag="${LAKESOUL_MIGRATION_BASELINE:-v3.0.0}"
host="${PGHOST:-127.0.0.1}"
port="${PGPORT:-5432}"
user="${PGUSER:-lakesoul_test}"
suffix="${GITHUB_RUN_ID:-$$}_${GITHUB_RUN_ATTEMPT:-1}"
suffix="${suffix//[^a-zA-Z0-9_]/_}"
migration_db="lakesoul_migration_${suffix}"
fresh_db="lakesoul_fresh_${suffix}"
work_dir="$(mktemp -d)"

connection=(-h "$host" -p "$port" -U "$user")
cleanup() {
  dropdb "${connection[@]}" --if-exists "$migration_db" >/dev/null 2>&1 || true
  dropdb "${connection[@]}" --if-exists "$fresh_db" >/dev/null 2>&1 || true
  rm -rf "$work_dir"
}
trap cleanup EXIT

if ! git cat-file -e "$baseline_tag:script/meta_init.sql"; then
  echo "Metadata migration baseline $baseline_tag is unavailable" >&2
  exit 1
fi

git show "$baseline_tag:script/meta_init.sql" > "$work_dir/legacy.sql"
createdb "${connection[@]}" "$migration_db"
createdb "${connection[@]}" "$fresh_db"

psql "${connection[@]}" -v ON_ERROR_STOP=1 -d "$migration_db" -f "$work_dir/legacy.sql" >/dev/null
psql "${connection[@]}" -v ON_ERROR_STOP=1 -d "$migration_db" -c \
  "insert into namespace(namespace, properties, comment) values ('release_migration', '{\"source\":\"$baseline_tag\"}', 'preserve me');" >/dev/null
psql "${connection[@]}" -v ON_ERROR_STOP=1 -d "$migration_db" -f script/meta_init.sql >/dev/null
psql "${connection[@]}" -v ON_ERROR_STOP=1 -d "$fresh_db" -f script/meta_init.sql >/dev/null

schema_query="
select table_name || '|' || column_name || '|' || data_type || '|' || is_nullable
from information_schema.columns
where table_schema = 'public'
order by table_name, ordinal_position;
select c.relname || '|replica_identity|' || c.relreplident
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relkind = 'r'
order by c.relname;
"
psql "${connection[@]}" -At -d "$migration_db" -c "$schema_query" > "$work_dir/migrated.schema"
psql "${connection[@]}" -At -d "$fresh_db" -c "$schema_query" > "$work_dir/fresh.schema"
diff -u "$work_dir/fresh.schema" "$work_dir/migrated.schema"

migrated_row="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select properties->>'source' || '|' || comment from namespace where namespace = 'release_migration';")"
test "$migrated_row" = "$baseline_tag|preserve me"
new_columns="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select count(*) from information_schema.columns where table_name = 'table_info' and column_name in ('table_schema_arrow_ipc', 'table_schema_arrow_ipc_json_hash');")"
test "$new_columns" = "2"
replica_identity="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select relreplident from pg_class where relname = 'data_commit_info';")"
test "$replica_identity" = "f"

pg_dump "${connection[@]}" --format=custom --file="$work_dir/metadata.dump" "$migration_db"
dropdb "${connection[@]}" "$migration_db"
createdb "${connection[@]}" "$migration_db"
pg_restore "${connection[@]}" --exit-on-error --dbname="$migration_db" "$work_dir/metadata.dump"
restored_row="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select properties->>'source' || '|' || comment from namespace where namespace = 'release_migration';")"
test "$restored_row" = "$baseline_tag|preserve me"

printf 'Metadata migration from %s and PostgreSQL backup recovery passed.\n' "$baseline_tag"
