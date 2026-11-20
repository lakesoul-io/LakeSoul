// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL-side vector/text index DDL against the real server.

pub mod support;

use anyhow::{Result, ensure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_ddl_creates_and_rebuilds() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            h.lake
                .batch_execute(
                    "CREATE TABLE index_ddl_docs (\
                       id BIGINT NOT NULL PRIMARY KEY,\
                       body TEXT,\
                       embedding vector(4))\
                     WITH (vector_index_columns = '[{\"column\":\"embedding\",\"dim\":4}]',\
                           text_index_columns = '[{\"column\":\"body\"}]')",
                )
                .await?;
            h.lake
                .batch_execute(
                    "INSERT INTO index_ddl_docs VALUES \
                       (1, 'apple apple banana', '[1,0,0,0]'),\
                       (2, 'banana cherry', '[0,1,0,0]'::vector(4)),\
                       (3, 'cherry apple', CAST(ARRAY[0,0,1,0] AS FLOAT[]))",
                )
                .await?;

            let vector = h
                .lake
                .query(
                    "SELECT id FROM index_ddl_docs \
                     ORDER BY embedding <-> '[1,0,0,0]'::vector(4) LIMIT 1",
                    &[],
                )
                .await?;
            ensure!(
                vector.iter().map(|row| row.get::<_, i64>(0)).collect::<Vec<_>>()
                    == vec![1],
                "unexpected vector order"
            );
            let text = h
                .lake
                .query(
                    "SELECT id, text_score(body, 'apple') AS score \
                     FROM index_ddl_docs WHERE text_match(body, 'apple') \
                     ORDER BY score DESC LIMIT 2",
                    &[],
                )
                .await?;
            ensure!(
                text.iter().map(|row| row.get::<_, i64>(0)).collect::<Vec<_>>()
                    == vec![1, 3],
                "unexpected text order"
            );

            // The access method and its tuning knobs are accepted; LakeSoul
            // builds the same IVF index either way.
            h.lake
                .batch_execute(
                    "CREATE TABLE index_ddl_late (\
                       id BIGINT NOT NULL PRIMARY KEY,\
                       body TEXT,\
                       embedding vector(4))",
                )
                .await?;
            h.lake
                .batch_execute(
                    "INSERT INTO index_ddl_late VALUES \
                       (1, 'kiwi', CAST(ARRAY[1,0,0,0] AS FLOAT[])),\
                       (2, 'pear', CAST(ARRAY[0,1,0,0] AS FLOAT[]))",
                )
                .await?;
            h.lake
                .batch_execute(
                    "CREATE INDEX index_ddl_late_vec ON index_ddl_late \
                     USING hnsw (embedding) \
                     WITH (m = 16, ef_construction = 64)",
                )
                .await?;
            h.lake
                .execute(
                    "CREATE INDEX index_ddl_late_text ON index_ddl_late \
                     USING gin (body) WITH (fastupdate = on)",
                    &[],
                )
                .await?;
            let late = h
                .lake
                .query(
                    "SELECT id FROM index_ddl_late \
                     ORDER BY embedding <-> '[1,0,0,0]'::vector(4) LIMIT 1",
                    &[],
                )
                .await?;
            ensure!(late[0].get::<_, i64>(0) == 1, "late vector index");

            // A deferred declaration starts without an index.  The planner
            // must keep the exact distance sort until the runner builds it.
            h.lake
                .batch_execute(
                    "CREATE TABLE index_ddl_deferred (\
                       id BIGINT NOT NULL PRIMARY KEY,\
                       v vector(4))\
                     WITH (vector_index_columns = '[{\"column\":\"v\",\"dim\":4}]',\
                           index_maintenance = 'deferred')",
                )
                .await?;
            h.lake
                .batch_execute(
                    "INSERT INTO index_ddl_deferred VALUES \
                       (1, '[1,0,0,0]'), (2, '[0,1,0,0]')",
                )
                .await?;
            let early = h
                .lake
                .query(
                    "SELECT id FROM index_ddl_deferred \
                     ORDER BY v <-> '[1,0,0,0]'::vector(4) LIMIT 1",
                    &[],
                )
                .await?;
            ensure!(
                early[0].get::<_, i64>(0) == 1,
                "deferred table must fall back to the exact ordered scan"
            );

            h.lake
                .batch_execute(
                    "ALTER TABLE index_ddl_late SET (index_maintenance = 'deferred')",
                )
                .await?;
            h.lake.batch_execute("REINDEX TABLE index_ddl_late").await?;
            // The extended protocol takes the same rewrite path.
            h.lake
                .execute("REINDEX TABLE index_ddl_late", &[])
                .await?;
            let report = h
                .lake
                .query(
                    "SELECT lakesoul_rebuild_index('index_ddl_late', 'all')",
                    &[],
                )
                .await?;
            ensure!(
                report[0].get::<_, String>(0).contains("\"vector_shards\""),
                "rebuild report"
            );

            // `ALTER TABLE SET` only declares the index; the exact ordered
            // scan answers until REINDEX builds it.
            h.lake
                .batch_execute(
                    "CREATE TABLE index_ddl_alter (\
                       id BIGINT NOT NULL PRIMARY KEY, v vector(4))",
                )
                .await?;
            h.lake
                .batch_execute(
                    "INSERT INTO index_ddl_alter VALUES \
                       (1, '[1,0,0,0]'), (2, '[0,1,0,0]')",
                )
                .await?;
            h.lake
                .batch_execute(
                    "ALTER TABLE index_ddl_alter SET \
                     (vector_index_columns = '[{\"column\":\"v\",\"dim\":4}]')",
                )
                .await?;
            let declared = h
                .lake
                .query(
                    "SELECT id FROM index_ddl_alter \
                     ORDER BY v <-> '[1,0,0,0]'::vector(4) LIMIT 1",
                    &[],
                )
                .await?;
            ensure!(
                declared[0].get::<_, i64>(0) == 1,
                "ALTER declaration must use the exact ordered scan"
            );
            h.lake
                .batch_execute("REINDEX TABLE index_ddl_alter")
                .await?;

            h.lake
                .batch_execute(
                    "CREATE TABLE index_ddl_deferred_index (\
                       id BIGINT NOT NULL PRIMARY KEY, v vector(4))",
                )
                .await?;
            h.lake
                .batch_execute(
                    "INSERT INTO index_ddl_deferred_index VALUES \
                       (1, '[1,0,0,0]'), (2, '[0,1,0,0]')",
                )
                .await?;
            h.lake
                .batch_execute(
                    "ALTER TABLE index_ddl_deferred_index \
                     SET (index_maintenance = 'deferred')",
                )
                .await?;
            h.lake
                .execute(
                    "CREATE INDEX index_ddl_deferred_index_v \
                     ON index_ddl_deferred_index USING hnsw (v)",
                    &[],
                )
                .await?;
            let deferred_index = h
                .lake
                .query(
                    "SELECT id FROM index_ddl_deferred_index \
                     ORDER BY v <-> '[1,0,0,0]'::vector(4) LIMIT 1",
                    &[],
                )
                .await?;
            ensure!(
                deferred_index[0].get::<_, i64>(0) == 1,
                "deferred CREATE INDEX must use the exact ordered scan"
            );

            // Refusals keep their PostgreSQL SQLSTATEs.
            let error = h
                .lake
                .execute("CREATE INDEX i ON index_ddl_late USING btree (id)", &[])
                .await
                .expect_err("btree must be refused");
            ensure!(
                format!("{error:?}").contains("0A000"),
                "unexpected btree error: {error:?}"
            );
            let error = h
                .lake
                .execute("REINDEX INDEX index_ddl_late_vec", &[])
                .await
                .expect_err("named REINDEX must be refused");
            ensure!(
                format!("{error:?}").contains("0A000"),
                "unexpected REINDEX error: {error:?}"
            );
            let error = h
                .lake
                .execute(
                    "ALTER TABLE index_ddl_late RESET (vector_index_columns)",
                    &[],
                )
                .await
                .expect_err("removing a vector declaration must be refused");
            ensure!(
                format!("{error:?}").contains("0A000"),
                "unexpected RESET error: {error:?}"
            );
            Ok(())
        })
    })
    .await
}
