// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod catalog_tests {
    use crate::catalog::LakeSoulTableProperty;
    use crate::catalog::{LakeSoulCatalog, LakeSoulNamespace};
    use crate::lakesoul_table::LakeSoulTable;
    use crate::serialize::arrow_java::ArrowJavaSchema;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::assert_batches_eq;
    use datafusion::catalog::schema::SchemaProvider;
    use datafusion::catalog::{CatalogList, CatalogProvider};
    use lakesoul_io::lakesoul_io_config::create_session_context;
    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
    use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
    use proto::proto::entity::{Namespace, TableInfo, TableNameId};
    use rand::distributions::{Alphanumeric, Standard};
    use rand::{thread_rng, Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::env;
    use std::sync::Arc;
    use test_log::test;
    use tokio::runtime::Runtime;
    use tracing::debug;

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

    async fn get_client() -> MetaDataClientRef {
        Arc::new(MetaDataClient::from_env().await.unwrap())
    }

    fn random_namespace(hash_bucket_num: usize) -> Vec<Namespace> {
        let mut rng = ChaCha8Rng::from_rng(thread_rng()).unwrap();
        (0..rng.gen_range(1..10))
            .map(|_| Namespace {
                namespace: {
                    let mut v = String::with_capacity(5);
                    for _ in 0..5 {
                        v.push((&mut rng).gen_range('a'..'z'));
                    }
                    v
                },
                properties: serde_json::to_string(&LakeSoulTableProperty {
                    hash_bucket_num: Some(hash_bucket_num),
                })
                .unwrap(),
                comment: "this is comment".to_string(),
                domain: "public".to_string(),
            })
            .collect()
    }

    fn random_tables(nps: Vec<Namespace>, schema: SchemaRef) -> Vec<(Namespace, Vec<TableInfo>)> {
        let mut ret = Vec::with_capacity(nps.len());
        let mut rng = ChaCha8Rng::from_rng(thread_rng()).unwrap();
        let schema = serde_json::to_string::<ArrowJavaSchema>(&schema.into()).unwrap();
        for np in nps {
            let n = rng.gen_range(1usize..10);
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                let table_name = {
                    let mut v = String::with_capacity(8);
                    for _ in 0..5 {
                        v.push((&mut rng).gen_range('a'..'z'));
                    }
                    v
                };
                let path = format!("{}{}/{}", env::temp_dir().to_str().unwrap(), &np.namespace, &table_name);
                v.push(TableInfo {
                    table_id: (&mut rng).sample_iter(&Alphanumeric).take(12).map(char::from).collect(),
                    table_namespace: np.namespace.clone(),
                    table_name,
                    table_path: format!("file://{}", path.clone()),
                    table_schema: schema.clone(),
                    properties: np.properties.clone(),
                    partitions: ";range,hash".to_string(),
                    domain: np.domain.clone(),
                })
            }
            ret.push((np, v));
        }
        ret
    }

    #[test]
    fn test_lakesoul_catalog() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = Arc::new(MetaDataClient::from_env().await.unwrap());
            let mut config = LakeSoulIOConfigBuilder::new().build();
            // insert data;
            let batch = create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]],
            );
            let table_name = "test_table_01";
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
            let data = random_tables(random_namespace(4), schema.clone());

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

            // id, path, name must unique
            for (np, tables) in data.iter() {
                client.create_namespace(np.clone()).await.unwrap();
                for t in tables {
                    client.create_table(t.clone()).await.unwrap();
                    let lakesoul_table = LakeSoulTable::for_namespace_and_name(&np.namespace, &t.table_name)
                        .await
                        .unwrap();
                    lakesoul_table.execute_upsert(batch.clone()).await.unwrap();
                }
            }

            let catalog = Arc::new(LakeSoulCatalog::new(client.clone(), sc.clone()));

            // test show_tables
            {
                let before = {
                    let df = sc.sql("show tables").await.unwrap();
                    df.collect().await.unwrap()
                };
                sc.register_catalog("lakesoul", catalog.clone());
                let after = {
                    let df = sc.sql("show tables").await.unwrap();
                    df.collect().await.unwrap()
                };
                assert_ne!(before, after);
            }
            for (np, tables) in data.iter() {
                let schema = LakeSoulNamespace::new(client.clone(), sc.clone(), &np.namespace);
                let names = schema.table_names();
                debug!("{names:?}");
                assert_eq!(names.len(), tables.len());
                for name in names {
                    assert!(schema.table(&name).await.is_some());
                    {
                        // test select
                        let q = format!("select * from lakesoul.{}.{}", np.namespace, name);
                        let df = sc.sql(&q).await.unwrap();
                        let record = df.collect().await.unwrap();
                        assert_batches_eq!(expected, &record);
                    }
                    {
                        // test show columns
                        let q = format!("show columns from lakesoul.{}.{}", np.namespace, name);
                        let df = sc.sql(&q).await.unwrap();
                        let record = df.collect().await.unwrap();
                        assert!(record.len() > 0);
                    }
                }
            }
        });
    }
}
