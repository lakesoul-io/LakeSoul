# Mirror LakeSoul metadata

Use a PostgreSQL source with the LakeSoul metadata schema, logical WAL and a replication user that can read the three source tables. Start the destination `postgres-lakesoul` endpoint with a warehouse prefix and a **separate PostgreSQL metadata database**. If the source and destination share a metadata database, creating and writing the mirror tables emits metadata changes that can feed back into the subscription. Separate databases on the same PostgreSQL server are supported by this setup.

Run the scripts from the repository root. Supply the three connection URLs in the environment. Use a PostgreSQL URI for `LAKESOUL_MIRROR_SOURCE_URL`, such as `postgresql://user:password@localhost:5432/mirror_source`. The `CONNECTION` clause requires a `postgresql://` or `postgres://` URI. Connect the destination endpoint as a user allowed to create subscriptions.

```bash
psql -X -v ON_ERROR_STOP=1 -d "$LAKESOUL_MIRROR_SOURCE_URL" \
  -f script/metadata-mirror/source.sql
psql -X -v ON_ERROR_STOP=1 -d "$LAKESOUL_MIRROR_DESTINATION_METADATA_URL" \
  -f script/metadata-mirror/target_namespace.sql
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

For an end-to-end smoke test, point the same environment variables at **fresh disposable** source and destination databases and use the `lakesoul_meta` namespace in the destination endpoint URL. Initialize both metadata databases with the repository's metadata setup and use a `postgres-lakesoul` build that includes the metadata array decoders.

The smoke test creates the publication and subscription and verifies the initial load, inserts, updates and deletes. It leaves the publication, subscription, targets and remaining fixtures for inspection, including on failure. Use a fresh source and destination pair on every run so that these objects and fixture keys are available.

```bash
bash script/metadata-mirror/smoke_test.sh
```
