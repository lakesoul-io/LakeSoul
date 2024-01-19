use std::collections::BTreeSet;
use std::env;
use std::sync::Arc;
use rand::{Rng, SeedableRng, thread_rng};
use rand::distributions::Alphanumeric;
use rand_chacha::ChaCha8Rng;
use tokio::runtime::Runtime;
use lakesoul_io::lakesoul_io_config::{create_session_context, LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use proto::proto::entity::{TableInfo, TableNameId};
use crate::catalog::{LakeSoulNamespace, LakeSoulTableProperty};
use crate::serialize::arrow_java::ArrowJavaSchema;
use tracing::debug;

fn table_name_id_from_table_info(table_info: &TableInfo) -> TableNameId {
    TableNameId {
        table_name: table_info.table_name.clone(),
        table_id: table_info.table_id.clone(),
        table_namespace: table_info.table_namespace.clone(),
        domain: table_info.domain.clone(),
    }
}

fn random_table_info(table_namespace: Option<String>, config: &LakeSoulIOConfig) -> TableInfo {
    let mut rng = ChaCha8Rng::from_rng(thread_rng()).unwrap();
    let table_name = (&mut rng).sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect::<String>();
    let table_namespace = table_namespace.unwrap_or_else(|| {
        (&mut rng).sample_iter(&Alphanumeric)
            .take(30).
            map(char::from)
            .collect::<String>()
    });
    let num = rng.gen_range(0..10);
    TableInfo {
        table_id: format!("table_{}", uuid::Uuid::new_v4()),
        table_path: format!("file://{}default/{}", env::temp_dir().to_str().unwrap(), &table_name),
        table_name,
        table_schema: serde_json::to_string::<ArrowJavaSchema>(&config.schema().into()).unwrap(),
        table_namespace,
        properties: serde_json::to_string(&LakeSoulTableProperty {
            hash_bucket_num: Some(num),
        }).unwrap(),
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


// async fn init_table(client: MetaDataClientRef, schema: SchemaRef, table_name: &str) -> crate::error::Result<()> {
//     let builder = LakeSoulIOConfigBuilder::new().with_schema(schema.clone());
//     // .with_primary_keys(pks);
//     crate::catalog::create_table(client, table_name, builder.build()).await
// }


async fn create_tables_with_random(client: MetaDataClientRef, config: &LakeSoulIOConfig, ng: Vec<(Option<String>, i32)>) -> Vec<TableInfo> {
    let mut res = vec![];
    for (a, num) in ng {
        for _ in 0..num {
            let table_info = random_table_info(a.clone(), config);
            client.create_table(table_info.clone()).await.unwrap();
            res.push(table_info);
        }
    }
    res
}

async fn get_client() -> MetaDataClientRef {
    Arc::new(MetaDataClient::from_env().await.unwrap())
}

#[test_log::test(tokio::test)]
async fn test_get_all_table_name_id_by_namespace() {
    let client = get_client().await;
    let ng = vec![
        (Some("lakesoul01".into()), 1),
        (None, 1),
        (Some("lakesoul02".into()), 2),
        (None, 2),
        (Some("lakesoul01".into()), 3),
        (None, 3),
    ];
    let config = LakeSoulIOConfigBuilder::new().build();
    let mut expected = create_tables_with_random(Arc::clone(&client), &config, ng).await;
    expected.sort_by(|a, b| { a.table_namespace.cmp(&b.table_namespace) });
    let expected = expected.into_iter()
        .map(|info| {
            table_name_id_from_table_info(&info)
        })
        .collect::<Vec<TableNameId>>();
    let mut actual = vec![];
    let namespace_set = expected.iter()
        .map(|t| t.table_namespace.clone())
        .collect::<BTreeSet<String>>();
    for ns in namespace_set {
        let tmp = client.get_all_table_name_id_by_namespace(&ns).await.unwrap();
        actual.extend(tmp);
    }
    debug!("expected:\n{:#?}",expected);
    debug!("actual:\n{:#?}",actual);
    assert_eq!(expected, actual);
}

#[test_log::test]
fn test_catalog_provider() {
    let rt = Arc::new(Runtime::new().unwrap());
    rt.clone().block_on(async {
        let mut config = LakeSoulIOConfigBuilder::new().build();
        let sc = Arc::new(create_session_context(&mut config).unwrap());

        let ln = Arc::new(
            LakeSoulNamespace::new(
                get_client().await,
                rt.clone(),
                sc,
                "lakesoul01",
            ));
        assert!(sc.register_catalog(ln.namespace(), ln.clone()).is_none());
        let df = sc.sql(format!("select * from {}", ln.namespace())).await.unwrap();
        df.show().unwrap();
    }
    )
}
