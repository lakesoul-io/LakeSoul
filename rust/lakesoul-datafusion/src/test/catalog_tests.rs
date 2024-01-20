mod catalog_tests {
    use lakesoul_io::lakesoul_io_config::create_session_context;
    use crate::catalog::{LakeSoulCatalog, LakeSoulNamespace};
    use datafusion::catalog::schema::SchemaProvider;
    use crate::catalog::LakeSoulTableProperty;
    use crate::serialize::arrow_java::ArrowJavaSchema;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::{CatalogList, CatalogProvider};
    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
    use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
    use proto::proto::entity::{Namespace, TableInfo, TableNameId};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::env;
    use std::fs::{remove_dir, remove_dir_all};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::SystemTime;
    use arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion::assert_batches_eq;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use futures::TryFutureExt;
    use tracing::{debug, info};
    use lakesoul_io::datasource::file_format::LakeSoulParquetFormat;
    use lakesoul_io::datasource::listing::LakeSoulListingTable;
    use lakesoul_io::lakesoul_writer::{AsyncBatchWriter, MultiPartAsyncWriter, SortAsyncWriter, SyncSendableMutableLakeSoulWriter};
    use crate::lakesoul_table::LakeSoulTable;

    fn table_name_id_from_table_info(table_info: &TableInfo) -> TableNameId {
        TableNameId {
            table_name: table_info.table_name.clone(),
            table_id: table_info.table_id.clone(),
            table_namespace: table_info.table_namespace.clone(),
            domain: table_info.domain.clone(),
        }
    }

    fn random_table_info(namespace: &Namespace, config: &LakeSoulIOConfig) -> TableInfo {
        let mut rng = ChaCha8Rng::from_rng(thread_rng()).unwrap();
        let table_name = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(5)
            .map(char::from)
            .collect::<String>().to_lowercase();
        TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_path: format!("file://{}default/{}", env::temp_dir().to_str().unwrap(), &table_name),
            table_name,
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&config.schema().into()).unwrap(),
            table_namespace: namespace.namespace.clone(),
            properties: namespace.properties.clone(),
            partitions: format!(
                "{};{}",
                "",
                config
                    .primary_keys_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            domain: "public".to_string(),
        }
    }

    fn random_namespace(name: Option<String>) -> Namespace {
        let mut rng = ChaCha8Rng::from_rng(thread_rng()).unwrap();
        let name = name.unwrap_or_else(|| {
            (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect::<String>()
        });
        let num = rng.gen_range(1..10);
        let comment = (&mut rng)
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();
        Namespace {
            namespace: name,
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(num),
            })
                .unwrap(),
            comment,
            domain: "public".to_string(),
        }
    }

    async fn create_namespaces_with_random(client: MetaDataClientRef, nps: Vec<Option<String>>) -> Vec<Namespace> {
        let mut res = vec![];
        for a in nps {
            let np = random_namespace(a);
            client.create_namespace(np.clone()).await.unwrap();
            res.push(np);
        }
        res
    }

    async fn create_tables_with_random(
        client: MetaDataClientRef,
        config: &LakeSoulIOConfig,
        ng: Vec<(Namespace, i32)>,
    ) -> Vec<TableInfo> {
        let mut res = vec![];
        for (a, num) in ng {
            for _ in 0..num {
                let table_info = random_table_info(&a, config);
                client.create_table(table_info.clone()).await.unwrap();
                res.push(table_info);
            }
        }
        res
    }

    // in catalog test, we don't care about what to insert
    async fn init_table(
        client: MetaDataClientRef,
        config: &LakeSoulIOConfig,
        namespace: &Namespace,
    ) -> TableInfo {
        let pks = vec!["range".to_string(), "hash".to_string()];
        let schema = SchemaRef::new(Schema::new(
            ["range", "hash", "value"]
                .into_iter()
                .map(|name| Field::new(name, DataType::Int32, true))
                .collect::<Vec<Field>>(),
        ));
        let builder = LakeSoulIOConfigBuilder::from(config.clone())
            .with_schema(schema)
            .with_primary_keys(pks);
        let config = builder.clone().build();
        let info = random_table_info(namespace, &config);
        let batch = create_batch_i32(
            vec!["range", "hash", "value"],
            vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]],
        );
        let file = [
            env::temp_dir().to_str().unwrap(),
            &info.table_name.clone(),
            format!(
                "{}.parquet",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            )
                .as_str(),
        ]
            .iter()
            .collect::<PathBuf>()
            .to_str()
            .unwrap()
            .to_string();


        let builder = builder
            .with_file(file.clone());

        let config = builder.clone().build();

        client.create_table(info.clone()).await.expect("in test create table failed");
        let mut writer = MultiPartAsyncWriter::try_new(config).await.unwrap();
        writer.write_record_batch(batch.clone()).await.unwrap();
        let lakesoul_table = LakeSoulTable::for_name(&info.table_name).await.unwrap();
        lakesoul_table.execute_upsert(batch.clone()).await.unwrap();
        info
    }

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

    async fn test_get_all_table_name_id_by_namespace() {
        let client = get_client().await;
        client.meta_cleanup().await.unwrap();
        let v = vec![
            Some(String::from("lakesoul01")),
            None,
            Some(String::from("lakesoul02")),
            None,
            Some(String::from("lakesoul03")),
        ];
        let len = v.len();
        let nps = create_namespaces_with_random(client.clone(), v)
            .await
            .into_iter()
            .zip(1..=len as i32)
            .collect::<Vec<(Namespace, i32)>>();
        debug!("{nps:#?}");
        let config = LakeSoulIOConfigBuilder::new().build();
        let mut expected = create_tables_with_random(Arc::clone(&client), &config, nps).await;
        expected.sort_by(|a, b| a.table_namespace.cmp(&b.table_namespace));
        let expected = expected
            .into_iter()
            .map(|info| table_name_id_from_table_info(&info))
            .collect::<Vec<TableNameId>>();
        let mut actual = vec![];
        let namespace_set = expected
            .iter()
            .map(|t| t.table_namespace.clone())
            .collect::<BTreeSet<String>>();
        for ns in namespace_set {
            let tmp = client.get_all_table_name_id_by_namespace(&ns).await.unwrap();
            actual.extend(tmp);
        }
        debug!("expected:\n{:#?}", expected);
        debug!("actual:\n{:#?}", actual);
        assert_eq!(expected, actual);
    }

    async fn test_schema_provider() {
        let client = get_client().await;
        client.meta_cleanup().await.unwrap();
        let mut config = LakeSoulIOConfigBuilder::new().build();
        let sc = Arc::new(create_session_context(&mut config).unwrap());
        let n = String::from("lakesoul_up");
        let v = vec![
            None,
            None,
            None,
            Some(n.clone()),
        ];
        let len = v.len();
        let nps = create_namespaces_with_random(client.clone(), v)
            .await
            .into_iter()
            .zip(1..=len as i32)
            .collect::<Vec<(Namespace, i32)>>();
        create_tables_with_random(client.clone(), &config, nps).await;
        let schema = LakeSoulNamespace::new(
            client.clone(),
            sc.clone(),
            &n,
        );
        assert_eq!(schema.table_names().len(), len)
    }

    async fn test_catalog_provider() {
        let client = get_client().await;
        client.meta_cleanup().await.unwrap();
        let v = vec![
            Some(String::from("lakesoul01")),
            None,
            Some(String::from("lakesoul02")),
            None,
            Some(String::from("lakesoul03")),
        ];
        let len = v.len();
        let nps = create_namespaces_with_random(client.clone(), v)
            .await
            .into_iter()
            .zip(1..=len as i32)
            .collect::<Vec<(Namespace, i32)>>();
        debug!("{nps:#?}");
        let mut config = LakeSoulIOConfigBuilder::new().build();
        let mut expected = create_tables_with_random(Arc::clone(&client), &config, nps).await;

        let sc = Arc::new(create_session_context(&mut config).unwrap());

        let catalog = Arc::new(
            LakeSoulCatalog::new(
                get_client().await,
                sc.clone(),
            ));
        assert_eq!(catalog.schema_names().len(), len);
    }


    async fn simple_sql_test() {
        let client = get_client().await;
        client.meta_cleanup().await.unwrap();
        // create namespace
        let np = Namespace {
            namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(4),
            })
                .unwrap(),
            comment: "this is comment".to_string(),
            domain: "public".to_string(),
        };

        client.create_namespace(np.clone()).await.unwrap();
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
            .with_schema(schema)
            .with_primary_keys(pks)
            .build();

        let path = format!("{}default/{}", env::temp_dir().to_str().unwrap(), &table_name);
        let info = TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_path: format!("file://{}", path.clone()),
            table_name: table_name.to_string(),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&config.schema().into()).unwrap(),
            table_namespace: np.namespace.clone(),
            properties: np.properties.clone(),
            partitions: format!(
                "{};{}",
                "",
                config
                    .primary_keys_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            domain: "public".to_string(),
        };

        client.create_table(info.clone()).await.expect("in test create table failed");
        let lakesoul_table = LakeSoulTable::for_name(&info.table_name).await.unwrap();
        lakesoul_table.execute_upsert(batch.clone()).await.unwrap();

        // use sql
        let mut sc = Arc::new(create_session_context(&mut config).unwrap());

        let catalog = Arc::new(LakeSoulCatalog::new(client.clone(), sc.clone()));
        sc.register_catalog("lakesoul", catalog.clone());
        let q = "select * from lakesoul.default.test_table_01".to_string();
        let df = sc.sql(&q).await.unwrap();
        let record = df.collect().await.unwrap();

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

        assert_batches_eq!(expected,&record);
        remove_dir_all(path).unwrap();
    }


    #[test_log::test]
    fn test_all() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            test_get_all_table_name_id_by_namespace().await;
            test_schema_provider().await;
            test_catalog_provider().await;
            simple_sql_test().await;
        });
    }
}

