// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use arrow_array::record_batch;
use chrono::Days;
use datafusion::assert_batches_eq;
use futures::StreamExt;
use lakesoul_datafusion::{cli::CoreArgs, create_lakesoul_session_ctx};
use lakesoul_metadata::{Claims, MetaDataClient, QueryType};
use test_utils::{TestServer, build_client, handle_sql, ingest};
use tokio::time::sleep;
use tracing::info;

mod test_utils;

async fn test_flight_sql_lfs() {
    let mut client = build_client(None).await;
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let core_args = CoreArgs::default();
    let ctx = create_lakesoul_session_ctx(meta_client.clone(), &core_args).unwrap();
    // drop table
    {
        let drop_sql = "DROP TABLE IF EXISTS test_lfs";
        let df = ctx.sql(drop_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }
    // create table
    {
        let create_sql = "
            CREATE EXTERNAL TABLE
            test_lfs
            (c1 VARCHAR NOT NULL, c2 INT NOT NULL, c3 DOUBLE , PRIMARY KEY(c1))
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION 'file:///tmp/LAKESOUL/test_lfs_data'
        ";
        let df = ctx.sql(create_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }

    {
        let batches = {
            let batch = record_batch!(
                ("c1", Utf8, ["a", "b", "c"]),
                ("c2", Int32, [1, 2, 3]),
                ("c3", Float64, [Some(4.0), None, Some(5.0)])
            )
            .unwrap();

            vec![batch]
        };

        {
            let num = ingest(&mut client, batches.clone(), "test_lfs", None)
                .await
                .unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
            // can not ingest to non-exist table
            let res = ingest(
                &mut client,
                batches.clone(),
                "test_lfs",
                Some("🤖".to_string()),
            )
            .await;
            assert!(res.is_err());
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| a  | 1  | 4.0 |",
                "| b  | 2  |     |",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }

        {
            // no jwt so this is fine
            client.set_header("authorization", "dummy");
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();
            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| a  | 1  | 4.0 |",
                "| b  | 2  |     |",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }

        {
            // with filter
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs
                WHERE c2 > 1 AND c3 > 4.0
                ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }
    }
}

async fn test_jwt() {
    let claims = Claims {
        sub: "lake-iam-001".to_string(),
        group: "lake-czods".to_string(),
        exp: chrono::Utc::now()
            .checked_add_days(Days::new(1))
            .unwrap()
            .timestamp() as usize,
    };
    let mut client = build_client(Some(claims)).await;
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let core_args = CoreArgs::default();
    let ctx = create_lakesoul_session_ctx(meta_client.clone(), &core_args).unwrap();
    // drop table
    {
        let drop_sql = "DROP TABLE IF EXISTS test_lfs";
        let df = ctx.sql(drop_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }
    // create table
    {
        let create_sql = "
            CREATE EXTERNAL TABLE
            test_lfs
            (c1 VARCHAR NOT NULL, c2 INT NOT NULL, c3 DOUBLE , PRIMARY KEY(c1))
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION 'file:///tmp/LAKESOUL/test_lfs_data'
        ";
        let df = ctx.sql(create_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }

    {
        let batches = {
            let batch = record_batch!(
                ("c1", Utf8, ["a", "b", "c"]),
                ("c2", Int32, [1, 2, 3]),
                ("c3", Float64, [Some(4.0), None, Some(5.0)])
            )
            .unwrap();

            vec![batch]
        };

        {
            let num = ingest(&mut client, batches.clone(), "test_lfs", None)
                .await
                .unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
            // can not ingest to non-exist table
            let res = ingest(
                &mut client,
                batches.clone(),
                "test_lfs",
                Some("🤖".to_string()),
            )
            .await;
            assert!(res.is_err());
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| a  | 1  | 4.0 |",
                "| b  | 2  |     |",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }

        {
            // no jwt so this is should fail
            client.set_header("authorization", "dummy");
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await;
            assert!(result.is_err());
            let result = ingest(&mut client, batches.clone(), "test_lfs", None).await;
            assert!(result.is_err())
        }
    }
}

async fn test_rbac() {
    let claims = Claims {
        sub: "lake-iam-001".to_string(),
        group: "lake-czods".to_string(),
        exp: chrono::Utc::now()
            .checked_add_days(Days::new(1))
            .unwrap()
            .timestamp() as usize,
    };
    let mut client = build_client(Some(claims)).await;
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let core_args = CoreArgs::default();
    let ctx = create_lakesoul_session_ctx(meta_client.clone(), &core_args).unwrap();
    // drop table
    {
        let drop_sql = "DROP TABLE IF EXISTS test_lfs";
        let df = ctx.sql(drop_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }
    // create table
    {
        let create_sql = "
            CREATE EXTERNAL TABLE
            test_lfs
            (c1 VARCHAR NOT NULL, c2 INT NOT NULL, c3 DOUBLE , PRIMARY KEY(c1))
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION 'file:///tmp/LAKESOUL/test_lfs_data'
        ";
        let df = ctx.sql(create_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }

    {
        let batches = {
            let batch = record_batch!(
                ("c1", Utf8, ["a", "b", "c"]),
                ("c2", Int32, [1, 2, 3]),
                ("c3", Float64, [Some(4.0), None, Some(5.0)])
            )
            .unwrap();

            vec![batch]
        };

        {
            let num = ingest(&mut client, batches.clone(), "test_lfs", None)
                .await
                .unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
            // can not ingest to non-exist table
            let res = ingest(
                &mut client,
                batches.clone(),
                "test_lfs",
                Some("🤖".to_string()),
            )
            .await;
            assert!(res.is_err());
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| a  | 1  | 4.0 |",
                "| b  | 2  |     |",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }
        {
            // no such table
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_bababaa ORDER BY c2;
            ";

            let res = handle_sql(&mut client, query_sql).await;
            assert!(res.is_err());
            print!("{}", res.err().unwrap())
        }
    }
}
async fn test_flight_sql_obj_store() {
    let mut client = build_client(None).await;

    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());

    let core_args = CoreArgs {
        warehouse_prefix: Some("s3://lakesoul-bucket/flight-test".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        s3_bucket: Some("lakesoul-test-bucket".to_string()),
        s3_access_key: Some("rustfsadmin".to_string()),
        s3_secret_key: Some("rustfsadmin".to_string()),
        worker_threads: 2,
        s3_virtual_host_style: false,
    };
    let ctx = create_lakesoul_session_ctx(meta_client.clone(), &core_args).unwrap();

    {
        let drop_sql = "DROP TABLE IF EXISTS test_s3";
        let df = ctx.sql(drop_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }

    {
        let create_sql = "
            CREATE EXTERNAL TABLE
            IF NOT EXISTS test_s3
            (c1 VARCHAR NOT NULL , c2 INT NOT NULL, c3 DOUBLE , PRIMARY KEY(c1))
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION 's3://default/test_s3_data'
        ";
        let df = ctx.sql(create_sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(batches.is_empty());
    }
    {
        let batches = {
            let batch = record_batch!(
                ("c1", Utf8, ["a", "b", "c"]),
                ("c2", Int32, [1, 2, 3]),
                ("c3", Float64, [Some(4.0), None, Some(5.0)])
            )
            .unwrap();

            vec![batch]
        };

        {
            let num = ingest(&mut client, batches.clone(), "test_s3", None)
                .await
                .unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_s3 ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| a  | 1  | 4.0 |",
                "| b  | 2  |     |",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }

        {
            // with filter
            let query_sql = "
                SELECT c1,c2,c3 FROM test_s3
                WHERE c2 > 1 AND c3 > 4.0
                ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = [
                "+----+----+-----+",
                "| c1 | c2 | c3  |",
                "+----+----+-----+",
                "| c  | 3  | 5.0 |",
                "+----+----+-----+",
            ];
            assert_batches_eq!(expected, &result);
        }
    }
}

async fn test_rbac_cross_domain_execution() {
    let meta_client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let core_args = CoreArgs::default();
    let ctx = create_lakesoul_session_ctx(meta_client.clone(), &core_args).unwrap();
    let name = format!("flight_acl_{}", uuid::Uuid::new_v4().simple());
    let create_sql = format!(
        "CREATE EXTERNAL TABLE {name} (id BIGINT) STORED AS LAKESOUL \
         LOCATION 'file:///tmp/{name}'"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    // Move the table into a private domain without changing its data path.
    {
        let pooled = meta_client.pooled_client();
        let locked = pooled.lock().await;
        let conn = locked.get(QueryType::RW).await.unwrap();
        // Setting the domain at creation time is done by the job's env; this
        // test moves it after creation, so bypass the immutability trigger.
        conn.batch_execute("set session_replication_role = replica")
            .await
            .unwrap();
        for sql in [
            "update table_info set domain = 'team_a' where table_name = $1",
            "update table_name_id set domain = 'team_a' where table_name = $1",
        ] {
            conn.execute(sql, &[&name]).await.unwrap();
        }
        conn.batch_execute("set session_replication_role = origin")
            .await
            .unwrap();
    }

    let claims = |group: &str| Claims {
        sub: "flight-acl-user".to_string(),
        group: group.to_string(),
        exp: chrono::Utc::now()
            .checked_add_days(Days::new(1))
            .unwrap()
            .timestamp() as usize,
    };

    // A token for another domain must be refused at both discovery and the
    // DoGet/DoPut execution paths.
    let mut other = build_client(Some(claims("team_b"))).await;
    let select = format!("SELECT * FROM {name}");
    assert!(
        handle_sql(&mut other, &select).await.is_err(),
        "cross-domain Flight read must be denied"
    );
    let batch = record_batch!(("id", Int64, [1_i64, 2_i64])).unwrap();
    assert!(
        ingest(&mut other, vec![batch.clone()], &name, None)
            .await
            .is_err(),
        "cross-domain Flight write must be denied"
    );
    // The prepared-statement execution path must enforce the same ACL.
    if let Ok(mut stmt) = other.prepare(select.clone(), None).await {
        assert!(
            stmt.execute().await.is_err(),
            "cross-domain prepared execute must be denied"
        );
    }

    // The same domain remains able to read the (still empty) table, including
    // through a prepared statement's DoGet.
    let mut same = build_client(Some(claims("team_a"))).await;
    let batches = handle_sql(&mut same, &select).await.unwrap();
    assert!(batches.is_empty(), "same-domain read should see no rows");
    let mut stmt = same.prepare(select.clone(), None).await.unwrap();
    let info = stmt.execute().await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut stream = same.do_get(ticket).await.unwrap();
    let mut prepared_batches = Vec::new();
    while let Some(batch) = stream.next().await {
        prepared_batches.push(batch.unwrap());
    }
    assert!(
        prepared_batches.is_empty(),
        "same-domain prepared execute/DoGet should see no rows"
    );

    let pooled = meta_client.pooled_client();
    let locked = pooled.lock().await;
    let conn = locked.get(QueryType::RW).await.unwrap();
    for sql in [
        "delete from table_info where table_name = $1",
        "delete from table_name_id where table_name = $1",
    ] {
        conn.execute(sql, &[&name]).await.unwrap();
    }
    conn.execute(
        "delete from table_path_id where table_path = $1",
        &[&format!("file:///tmp/{name}")],
    )
    .await
    .unwrap();
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_flight_sql_server() {
    // FIXME: s3 related args will ignore local file system
    // LFS
    {
        // start server no jwt and no rbac
        let _server = TestServer::new(
            &[],
            vec![
                ("JWT_AUTH_ENABLED", "false"),
                ("RBAC_AUTH_ENABLED", "false"),
            ],
        )
        .unwrap();
        test_flight_sql_lfs().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(5)).await;
    // S3
    {
        let _server = TestServer::new(
            &[
                "--warehouse-prefix",
                "s3://lakesoul-bucket/flight-test",
                "--endpoint",
                "http://localhost:9000",
                "--s3-bucket",
                "lakesoul-test-bucket",
                "--s3-access-key",
                "rustfsadmin",
                "--s3-secret-key",
                "rustfsadmin",
            ],
            vec![
                ("JWT_AUTH_ENABLED", "false"),
                ("RBAC_AUTH_ENABLED", "false"),
            ],
        );
        test_flight_sql_obj_store().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(1)).await;
    {
        // start server with jwt
        let _server = TestServer::new(
            &[],
            vec![("JWT_AUTH_ENABLED", "true"), ("RBAC_AUTH_ENABLED", "false")],
        );
        test_jwt().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(1)).await;
    // rbac
    {
        let _server = TestServer::new(
            &[],
            vec![("JWT_AUTH_ENABLED", "true"), ("RBAC_AUTH_ENABLED", "true")],
        )
        .unwrap();
        test_rbac().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(1)).await;
    {
        let _server = TestServer::new(
            &[],
            vec![("JWT_AUTH_ENABLED", "true"), ("RBAC_AUTH_ENABLED", "true")],
        )
        .unwrap();
        test_rbac_cross_domain_execution().await;
    }
}

#[test]
#[should_panic]
fn test_rbac_panic() {
    let _server = TestServer::new(
        &["-a", "0.0.0.0:50052", "-m", "0.0.0.0:19001"],
        vec![("JWT_AUTH_ENABLED", "false"), ("RBAC_AUTH_ENABLED", "true")],
    )
    .unwrap();
}
