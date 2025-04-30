// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use arrow_array::record_batch;
use chrono::Days;
use datafusion::assert_batches_eq;
use lakesoul_datafusion::{cli::CoreArgs, create_lakesoul_session_ctx};
use lakesoul_flight::Claims;
use lakesoul_metadata::MetaDataClient;
use test_utils::{build_client, handle_sql, ingest, TestServer};
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
            let num = ingest(&mut client, batches.clone(), "test_lfs", None).await.unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
            // can not ingest to non-exist table
            let res = ingest(&mut client, batches.clone(), "test_lfs", Some("🤖".to_string())).await;
            assert!(res.is_err());
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = vec![
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
            let expected = vec![
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

            let expected = vec![
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
        exp: chrono::Utc::now().checked_add_days(Days::new(1)).unwrap().timestamp() as usize,
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
            let num = ingest(&mut client, batches.clone(), "test_lfs", None).await.unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
            // can not ingest to non-exist table
            let res = ingest(&mut client, batches.clone(), "test_lfs", Some("🤖".to_string())).await;
            assert!(res.is_err());
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = vec![
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
        exp: chrono::Utc::now().checked_add_days(Days::new(1)).unwrap().timestamp() as usize,
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
            let num = ingest(&mut client, batches.clone(), "test_lfs", None).await.unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
            // can not ingest to non-exist table
            let res = ingest(&mut client, batches.clone(), "test_lfs", Some("🤖".to_string())).await;
            assert!(res.is_err());
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_lfs ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = vec![
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
        lakesoul_home: "".to_string(),
        warehouse_prefix: Some("s3://lakesoul-bucket/flight-test".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        s3_bucket: Some("lakesoul-test-bucket".to_string()),
        s3_access_key: Some("minioadmin1".to_string()),
        s3_secret_key: Some("minioadmin1".to_string()),
        worker_threads: 2,
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
            let num = ingest(&mut client, batches.clone(), "test_s3", None).await.unwrap();
            assert_eq!(num, 3);
            info!("ingest num: {num}");
        }

        {
            // basic
            let query_sql = "
                SELECT c1,c2,c3 FROM test_s3 ORDER BY c2;
            ";

            let result = handle_sql(&mut client, query_sql).await.unwrap();

            let expected = vec![
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

            let expected = vec![
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

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_flight_sql_server() {
    // FIXME: s3 related args will ignore local file system
    // LFS
    {
        // start server no jwt and no rbac
        let _server =
            TestServer::new(&[], vec![("JWT_AUTH_ENABLED", "false"), ("RBAC_AUTH_ENABLED", "false")]).unwrap();
        test_flight_sql_lfs().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(1)).await;
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
                "minioadmin1",
                "--s3-secret-key",
                "minioadmin1",
            ],
            vec![("JWT_AUTH_ENABLED", "false"), ("RBAC_AUTH_ENABLED", "false")],
        );
        test_flight_sql_obj_store().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(1)).await;
    {
        // start server with jwt
        let _server = TestServer::new(&[], vec![("JWT_AUTH_ENABLED", "true"), ("RBAC_AUTH_ENABLED", "false")]);
        test_jwt().await;
    }

    // wait server shutdown
    sleep(Duration::from_secs(1)).await;
    // rbac
    {
        let _server = TestServer::new(&[], vec![("JWT_AUTH_ENABLED", "true"), ("RBAC_AUTH_ENABLED", "true")]).unwrap();
        test_rbac().await;
    }
}

#[test]
#[should_panic]
fn rabc_panic() {
    let _server = TestServer::new(&[], vec![("JWT_AUTH_ENABLED", "false"), ("RBAC_AUTH_ENABLED", "true")]).unwrap();
}
