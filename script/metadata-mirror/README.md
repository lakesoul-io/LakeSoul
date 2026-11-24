# Mirror LakeSoul metadata

Use a PostgreSQL source with the LakeSoul metadata schema, logical WAL and a replication user that can read the three source tables. Start the destination `postgres-lakesoul` endpoint with a warehouse prefix and a **separate PostgreSQL metadata database**. If the source and destination share a metadata database, creating and writing the mirror tables emits metadata changes that can feed back into the subscription. Separate databases on the same PostgreSQL server are supported by this setup.

Run the scripts from the repository root. Use the existing `LAKESOUL_PG_URL`, `LAKESOUL_PG_USERNAME` and `LAKESOUL_PG_PASSWORD` variables in the provisioning shell for the **source**. For example, `LAKESOUL_PG_URL` can be `jdbc:postgresql://localhost:5432/mirror_source?stringtype=unspecified`. The helper converts it to a PostgreSQL URI for the subscriber. It also accepts a PostgreSQL URI with credentials. The separate environment of the already running destination server points to the **destination** metadata database.

Set `LAKESOUL_MIRROR_DESTINATION_URL` to the destination LakeSoul SQL endpoint and an existing namespace such as `default`. Connect as a user allowed to create databases and subscriptions. The SQL script creates `lakesoul_meta` using `CREATE DATABASE IF NOT EXISTS` and maps the subscription to its tables from the same connection.

```bash
mirror_source_url=$(python3 script/metadata-mirror/source_url.py) &&
psql -X -v ON_ERROR_STOP=1 -d "$mirror_source_url" \
  -f script/metadata-mirror/source.sql &&
lakesoul_mirror_source_dsn="$mirror_source_url" \
  psql -X -v ON_ERROR_STOP=1 -d "$LAKESOUL_MIRROR_DESTINATION_URL" \
  -f script/metadata-mirror/subscribe.sql
```

The managed subscription creates the empty targets with the source primary keys and then copies the initial rows before applying WAL changes.

| Target | Primary key |
| --- | --- |
| `lakesoul_meta.table_info` | `table_id` |
| `lakesoul_meta.partition_info` | `table_id, partition_desc, version` |
| `lakesoul_meta.data_commit_info` | `table_id, partition_desc, commit_id` |

The mirror stores UUIDs as text. It stores `snapshot` and `file_ops` as JSON text in source array order, including empty arrays. Each target commits independently. A reader that consumes a partition snapshot must wait until all referenced commit rows have arrived and are marked committed.

For an end-to-end smoke test, point the same environment variables at **fresh disposable** source and destination databases and use an existing namespace such as `default` in the destination endpoint URL. Initialize both metadata databases with the repository's metadata setup and use a `postgres-lakesoul` build that includes the metadata array decoders and `CREATE DATABASE` support. The test connects to `lakesoul_meta` to verify the data after creating the subscription.

The smoke test creates the publication and subscription and verifies the initial load, inserts, updates and deletes. It leaves the publication, subscription, targets and remaining fixtures for inspection, including on failure. Use fresh source and destination databases and a source PostgreSQL instance where the `lakesoul_meta_mirror` replication slot name is available. PostgreSQL reserves slot names across all databases in the instance.

```bash
bash script/metadata-mirror/smoke_test.sh
```
