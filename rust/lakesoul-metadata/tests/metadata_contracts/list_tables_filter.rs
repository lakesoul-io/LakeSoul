// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Internal IVM tables (`lakesoul.ivm.internal=true`) are hidden from the
//! table listings used by the engines.

use lakesoul_metadata::{DaoType, MetaDataClient};
use lakesoul_metadata_proto::entity::TableInfo;

const NAMESPACE: &str = "default";

fn unique(tag: &str) -> String {
    format!("list_filter_{tag}_{}", uuid::Uuid::new_v4().simple())
}

async fn create_table(client: &MetaDataClient, name: &str, internal: bool) -> String {
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    let properties = if internal {
        "{\"lakesoul.ivm.internal\":\"true\"}"
    } else {
        "{}"
    };
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.to_string(),
            table_namespace: NAMESPACE.to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema: "[]".to_string(),
            properties: properties.to_string(),
            partitions: ";".to_string(),
            domain: "public".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    table_id
}

async fn names(client: &MetaDataClient) -> Vec<String> {
    client
        .get_all_table_name_id_by_namespace(NAMESPACE)
        .await
        .unwrap()
        .into_iter()
        .map(|name| name.table_name)
        .collect()
}

async fn query_paths(
    client: &MetaDataClient,
    query_type: DaoType,
    params: &str,
) -> Vec<String> {
    client
        .execute_query(query_type as i32, params.to_string())
        .await
        .unwrap()
        .table_path_id
        .into_iter()
        .map(|path| path.table_path)
        .collect()
}

async fn query_names(
    client: &MetaDataClient,
    query_type: DaoType,
    params: &str,
) -> Vec<String> {
    client
        .execute_query(query_type as i32, params.to_string())
        .await
        .unwrap()
        .table_name_id
        .into_iter()
        .map(|name| name.table_name)
        .collect()
}

#[tokio::test]
async fn internal_tables_are_hidden_from_the_listings() {
    let client = MetaDataClient::from_env().await.unwrap();
    let normal = unique("normal");
    let internal = unique("internal");
    create_table(&client, &normal, false).await;
    create_table(&client, &internal, true).await;

    let listed = names(&client).await;
    assert!(listed.contains(&normal), "normal table missing: {listed:?}");
    assert!(
        !listed.contains(&internal),
        "internal table listed by namespace: {listed:?}"
    );

    let paths = query_paths(&client, DaoType::ListAllTablePath, "").await;
    assert!(paths.iter().any(|path| path.ends_with(&normal)));
    assert!(
        !paths.iter().any(|path| path.ends_with(&internal)),
        "internal table listed by all paths"
    );

    let paths =
        query_paths(&client, DaoType::ListAllPathTablePathByNamespace, NAMESPACE).await;
    assert!(paths.iter().any(|path| path.ends_with(&normal)));
    assert!(
        !paths.iter().any(|path| path.ends_with(&internal)),
        "internal table listed by namespace paths"
    );

    let listed = query_names(&client, DaoType::ListTableNamesByDomain, "public").await;
    assert!(listed.contains(&normal));
    assert!(
        !listed.contains(&internal),
        "internal table listed by domain: {listed:?}"
    );

    client.drop_table(&normal, NAMESPACE).await.unwrap();
    client.drop_table(&internal, NAMESPACE).await.unwrap();
}
