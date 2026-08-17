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
python_command="${PYTHON:-python3}"
migration_runner=( "$python_command" script/metadata_migrate.py )

database_url() {
  printf 'postgresql://%s@%s:%s/%s' "$user" "$host" "$port" "$1"
}

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
"${migration_runner[@]}" migrate --database-url "$(database_url "$migration_db")"
"${migration_runner[@]}" migrate --database-url "$(database_url "$migration_db")"
psql "${connection[@]}" -v ON_ERROR_STOP=1 -d "$fresh_db" -f script/meta_init.sql >/dev/null
"${migration_runner[@]}" migrate --database-url "$(database_url "$fresh_db")"

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
migration_records="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select count(*) from lakesoul_schema_migrations where version = 4000000;")"
test "$migration_records" = "1"

cp -R script/metadata-migrations "$work_dir/changed-migrations"
printf '\n-- checksum regression\n' >> "$work_dir/changed-migrations/V4000000__core_4_0_0.sql"
if "${migration_runner[@]}" check \
  --database-url "$(database_url "$migration_db")" \
  --migrations-dir "$work_dir/changed-migrations" >/dev/null 2>&1; then
  echo "Modified applied migration unexpectedly passed checksum validation" >&2
  exit 1
fi
cp -R script/metadata-migrations "$work_dir/failing-migrations"
printf 'ALTER TABLE table_that_does_not_exist ADD COLUMN value text;\n' \
  > "$work_dir/failing-migrations/V4000001__intentional_failure.sql"
if "${migration_runner[@]}" migrate \
  --database-url "$(database_url "$migration_db")" \
  --migrations-dir "$work_dir/failing-migrations" >/dev/null 2>&1; then
  echo "Failed migration unexpectedly succeeded" >&2
  exit 1
fi
failed_migration_records="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select count(*) from lakesoul_schema_migrations where version = 4000001;")"
test "$failed_migration_records" = "0"

pg_dump "${connection[@]}" --format=custom --file="$work_dir/metadata.dump" "$migration_db"
dropdb "${connection[@]}" "$migration_db"
createdb "${connection[@]}" "$migration_db"
pg_restore "${connection[@]}" --exit-on-error --dbname="$migration_db" "$work_dir/metadata.dump"
restored_row="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select properties->>'source' || '|' || comment from namespace where namespace = 'release_migration';")"
test "$restored_row" = "$baseline_tag|preserve me"
"${migration_runner[@]}" check --database-url "$(database_url "$migration_db")"
restored_migration_records="$(psql "${connection[@]}" -At -d "$migration_db" -c \
  "select count(*) from lakesoul_schema_migrations where version = 4000000;")"
test "$restored_migration_records" = "1"

printf 'Metadata migration from %s and PostgreSQL backup recovery passed.\n' "$baseline_tag"
