// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::assert_batches_eq;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::session::create_session_context;
use lakesoul_metadata::MetaDataClient;
use proto::proto::entity::{Namespace, TableInfo};
use rand::Rng;
use rand::distr::Alphanumeric;

use tokio::runtime::Runtime;

use crate::catalog::{LakeSoulCatalog, LakeSoulNamespace, LakeSoulTableProperty};
use crate::cli::CoreArgs;
use crate::create_lakesoul_session_ctx;
use crate::lakesoul_table::LakeSoulTable;
use lakesoul_common::ser::arrow_java::schema_to_metadata_parts;

fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
    let values = values
        .into_iter()
        .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
        .collect::<Vec<ArrayRef>>();
    let iter = names
        .into_iter()
        .zip(values)
        .map(|(name, array)| (name, array, true))
        .collect::<Vec<_>>();
    RecordBatch::try_from_iter_with_nullable(iter).unwrap()
}

fn random_namespace(prefix: &str, hash_bucket_num: usize) -> Vec<Namespace> {
    let rng = &mut rand::rng();
    (0..rng.random_range(1..10))
        .map(|_| Namespace {
            namespace: {
                let mut v = String::with_capacity(5);
                for _ in 0..10 {
                    v.push(rng.random_range('a'..='z'));
                }
                format!("{prefix}_{v}")
            },
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(hash_bucket_num.to_string()),
                ..Default::default()
            })
            .unwrap(),
            comment: "this is comment".to_string(),
            domain: "public".to_string(),
        })
        .collect()
}

fn random_tables(
    nps: Vec<Namespace>,
    schema: SchemaRef,
) -> Vec<(Namespace, Vec<TableInfo>)> {
    let mut ret = Vec::with_capacity(nps.len());
    let rng = &mut rand::rng();
    let (schema, schema_ipc, schema_hash) = schema_to_metadata_parts(schema.as_ref());
    for np in nps {
        let n = rng.random_range(1usize..10);
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let table_name = {
                let mut v = String::with_capacity(8);
                for _ in 0..10 {
                    v.push(rng.random_range('a'..='z'));
                }
                v
            };
            let path = format!(
                "{}/test_data/{}/{}",
                env::current_dir()
                    .unwrap_or(env::temp_dir())
                    .to_str()
                    .unwrap(),
                &np.namespace,
                &table_name
            );
            let table_id = format!(
                "table_{}",
                rng.sample_iter(&Alphanumeric)
                    .take(22)
                    .map(char::from)
                    .collect::<String>()
            );
            v.push(TableInfo {
                table_id,
                table_namespace: np.namespace.clone(),
                table_name,
                table_path: format!("file://{}", path.clone()),
                table_schema: schema.clone(),
                table_schema_arrow_ipc: schema_ipc.clone(),
                table_schema_arrow_ipc_json_hash: schema_hash.clone(),
                properties: np.properties.clone(),
                partitions: ";range,hash".to_string(),
                domain: np.domain.clone(),
            })
        }
        ret.push((np, v));
    }
    ret
}

fn test_catalog_api() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        // insert data;
        let batch = create_batch_i32(
            vec!["range", "hash", "value"],
            vec![
                &[20201101, 20201101, 20201101, 20201102],
                &[1, 2, 3, 4],
                &[1, 2, 3, 4],
            ],
        );
        let pks = vec!["range".to_string(), "hash".to_string()];
        let schema = SchemaRef::new(Schema::new(
            ["range", "hash", "value"]
                .into_iter()
                .map(|name| Field::new(name, DataType::Int32, true))
                .collect::<Vec<Field>>(),
        ));

        let mut config = LakeSoulIOConfigBuilder::new()
            .with_schema(schema.clone())
            .with_primary_keys(pks)
            .build();

        let sc = Arc::new(create_session_context(&mut config).unwrap());
        let data = random_tables(random_namespace("api", 4), schema.clone());

        let catalog = Arc::new(LakeSoulCatalog::new(client.clone(), sc.clone()));
        let dummy_schema_provider =
            Arc::new(LakeSoulNamespace::new(client.clone(), sc.clone(), "dummy"));
        // id, path, name must be unique
        for (np, tables) in data.iter() {
            // client.create_namespace(np.clone()).await.unwrap();
            let old = catalog
                .register_schema(&np.namespace, dummy_schema_provider.clone())
                .unwrap();
            assert!(old.is_none());
            for t in tables {
                client.create_table(t.clone()).await.unwrap();
                let lakesoul_table = LakeSoulTable::for_namespace_and_name(
                    &np.namespace,
                    &t.table_name,
                    Some(client.clone()),
                )
                .await
                .unwrap();
                lakesoul_table.execute_upsert(batch.clone()).await.unwrap();
            }
        }
        assert!(
            sc.register_catalog("test_catalog_api", catalog.clone())
                .is_none()
        );
        for (np, tables) in data.iter() {
            let schema =
                LakeSoulNamespace::new(client.clone(), sc.clone(), &np.namespace);
            let names = schema.table_names();
            debug!("{names:?}");
            assert_eq!(names.len(), tables.len());
            for name in names {
                assert!(schema.table_exist(&name));
                assert!(schema.table(&name).await.unwrap().is_some());
                assert!(schema.deregister_table(&name).unwrap().is_some());
            }
        }
    });
}

fn test_catalog_sql() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        // insert data;
        let batch = create_batch_i32(
            vec!["range", "hash", "value"],
            vec![
                &[20201101, 20201101, 20201101, 20201102],
                &[1, 2, 3, 4],
                &[1, 2, 3, 4],
            ],
        );
        let pks = vec!["range".to_string(), "hash".to_string()];
        let schema = SchemaRef::new(Schema::new(
            ["range", "hash", "value"]
                .into_iter()
                .map(|name| Field::new(name, DataType::Int32, true))
                .collect::<Vec<Field>>(),
        ));

        let mut config = LakeSoulIOConfigBuilder::new()
            .with_schema(schema.clone())
            .with_primary_keys(pks)
            .build();

        let sc = Arc::new(create_session_context(&mut config).unwrap());

        let expected = &[
            "+----------+------+-------+",
            "| range    | hash | value |",
            "+----------+------+-------+",
            "| 20201101 | 1    | 1     |",
            "| 20201101 | 2    | 2     |",
            "| 20201101 | 3    | 3     |",
            "| 20201102 | 4    | 4     |",
            "+----------+------+-------+",
        ];

        let catalog = Arc::new(LakeSoulCatalog::new(client.clone(), sc.clone()));
        {
            let before = {
                let sql = "show tables";
                let df = sc.sql(sql).await.unwrap();
                df.collect().await.unwrap()
            };
            sc.register_catalog("test_catalog_sql", catalog.clone());
            let after = {
                let sql = "show tables";
                let df = sc.sql(sql).await.unwrap();
                df.collect().await.unwrap()
            };
            assert_ne!(after, before);
        }
        let data = random_tables(random_namespace("sql", 4), schema.clone());
        for (np, tables) in data.iter() {
            {
                // create schema
                let sql = format!("create schema test_catalog_sql.{}", np.namespace);
                let df = sc.sql(&sql).await.unwrap();
                df.collect().await.unwrap();
                let ret = client
                    .get_namespace_by_namespace(&np.namespace)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(np.namespace, ret.namespace);
            }
            for t in tables {
                client.create_table(t.clone()).await.unwrap();
                let lakesoul_table = LakeSoulTable::for_namespace_and_name(
                    &np.namespace,
                    &t.table_name,
                    Some(client.clone()),
                )
                .await
                .unwrap();
                lakesoul_table.execute_upsert(batch.clone()).await.unwrap();
            }
        }
        for (np, tables) in data.iter() {
            let schema =
                LakeSoulNamespace::new(client.clone(), sc.clone(), &np.namespace);
            let names = schema.table_names();
            debug!("{names:?}");
            assert_eq!(names.len(), tables.len());
            for name in names {
                {
                    // test show columns
                    let q = format!(
                        "show columns from test_catalog_sql.{}.{}",
                        np.namespace, name
                    );
                    let df = sc.sql(&q).await.unwrap();
                    let record = df.collect().await.unwrap();
                    assert!(!record.is_empty());
                }
                {
                    // test select
                    let q = format!(
                        "select * from test_catalog_sql.{}.{}",
                        np.namespace, name
                    );
                    let df = sc.sql(&q).await.unwrap();
                    let record = df.collect().await.unwrap();
                    assert_batches_eq!(expected, &record);
                }
                {
                    // drop table
                    let sql =
                        format!("drop table test_catalog_sql.{}.{}", np.namespace, name);
                    let df = sc.sql(&sql).await.unwrap();
                    assert!(df.collect().await.is_ok())
                }
            }
        }
    });
}

fn test_catalog_sql_partitioned_insert_column_order() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let sc = create_lakesoul_session_ctx(client, &CoreArgs::default()).unwrap();

        let rng = &mut rand::rng();
        let namespace = format!("issue_758_{}", rng.random::<u32>());
        let table_name = format!("test_{}", rng.random::<u32>());
        let table_path = format!(
            "file://{}/test_data/{}/{}",
            env::current_dir()
                .unwrap_or(env::temp_dir())
                .to_str()
                .unwrap(),
            namespace,
            table_name
        );

        let create_schema = format!("create schema \"LAKESOUL\".{namespace}");
        sc.sql(&create_schema)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let create_table = format!(
            "CREATE EXTERNAL TABLE \"LAKESOUL\".{namespace}.{table_name} (
                c1 VARCHAR NOT NULL,
                c2 INT NOT NULL,
                c3 DOUBLE
            )
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION '{table_path}'"
        );
        sc.sql(&create_table)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let insert_sql = format!(
            "INSERT INTO \"LAKESOUL\".{namespace}.{table_name} VALUES
                ('test', 1, 1.0),
                ('hello', 2, 2.5)"
        );
        sc.sql(&insert_sql).await.unwrap().collect().await.unwrap();

        let select_all_sql =
            format!("SELECT * FROM \"LAKESOUL\".{namespace}.{table_name} ORDER BY c1");
        let select_all = sc
            .sql(&select_all_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(
            &[
                "+-------+----+-----+",
                "| c1    | c2 | c3  |",
                "+-------+----+-----+",
                "| hello | 2  | 2.5 |",
                "| test  | 1  | 1.0 |",
                "+-------+----+-----+",
            ],
            &select_all
        );

        let projected_sql = format!(
            "SELECT c1, c3 FROM \"LAKESOUL\".{namespace}.{table_name} ORDER BY c1"
        );
        let projected = sc
            .sql(&projected_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(
            &[
                "+-------+-----+",
                "| c1    | c3  |",
                "+-------+-----+",
                "| hello | 2.5 |",
                "| test  | 1.0 |",
                "+-------+-----+",
            ],
            &projected
        );

        let partition_only_sql =
            format!("SELECT c2 FROM \"LAKESOUL\".{namespace}.{table_name} ORDER BY c2");
        let partition_only = sc
            .sql(&partition_only_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        #[rustfmt::skip]
        let expected = [
            "+----+",
            "| c2 |",
            "+----+",
            "| 1  |",
            "| 2  |",
            "+----+",
        ];
        assert_batches_eq!(&expected, &partition_only);

        let reordered_sql = format!(
            "SELECT c3, c2 FROM \"LAKESOUL\".{namespace}.{table_name} ORDER BY c3"
        );
        let reordered = sc
            .sql(&reordered_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(
            &[
                "+-----+----+",
                "| c3  | c2 |",
                "+-----+----+",
                "| 1.0 | 1  |",
                "| 2.5 | 2  |",
                "+-----+----+",
            ],
            &reordered
        );

        let filter_sql = format!(
            "SELECT c1, c2, c3 FROM \"LAKESOUL\".{namespace}.{table_name} WHERE c2 = 1"
        );
        let filtered = sc.sql(&filter_sql).await.unwrap().collect().await.unwrap();
        assert_batches_eq!(
            &[
                "+------+----+-----+",
                "| c1   | c2 | c3  |",
                "+------+----+-----+",
                "| test | 1  | 1.0 |",
                "+------+----+-----+",
            ],
            &filtered
        );

        let select_star_filter_sql =
            format!("SELECT * FROM \"LAKESOUL\".{namespace}.{table_name} WHERE c2 = 2");
        let select_star_filtered = sc
            .sql(&select_star_filter_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(
            &[
                "+-------+----+-----+",
                "| c1    | c2 | c3  |",
                "+-------+----+-----+",
                "| hello | 2  | 2.5 |",
                "+-------+----+-----+",
            ],
            &select_star_filtered
        );

        let show_columns_sql =
            format!("show columns from \"LAKESOUL\".{namespace}.{table_name}");
        let show_columns = sc
            .sql(&show_columns_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let column_names = show_columns
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("column_name")
                    .into_iter()
                    .flat_map(|column| {
                        let array =
                            column.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..array.len())
                            .map(|idx| array.value(idx).to_string())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            column_names,
            vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]
        );
    });
}

#[test]
fn test_all_cases() {
    test_catalog_api();
    test_catalog_sql();
    test_catalog_sql_partitioned_insert_column_order();
}
