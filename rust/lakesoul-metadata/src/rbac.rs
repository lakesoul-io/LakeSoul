// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::LakeSoulMetaDataError;
use crate::MetaDataClientRef;
use cached::proc_macro::cached;

#[cached(
    size = 1000,
    time = 600,
    result = true,
    sync_writes = true,
    time_refresh = true,
    key = "String",
    convert = r##"{ format!("{}/{}@{}.{}", user, group, ns, table) }"##
)]
pub async fn verify_permission_by_table_name(
    user: &str,
    group: &str,
    ns: &str,
    table: &str,
    meta_data_client: MetaDataClientRef,
) -> crate::Result<()> {
    // jwt token verification has ensured user belongs to the group
    // we only need to verify this table belongs to the group
    let table_name_id = meta_data_client
        .as_ref()
        .get_table_name_id_by_table_name(table, ns)
        .await?;
    log::debug!("table {}.{} in domain {}", ns, table, table_name_id.domain);
    match table_name_id.domain.as_str() {
        "public" | "lake-public" => Ok(()),
        domain if domain == group => Ok(()),
        _ => Err(LakeSoulMetaDataError::Other(
            format!(
                "permission denied to access {}.{} from user {} in group {}",
                ns, table, user, group
            )
            .into(),
        )),
    }
}

#[cached(
    size = 1000,
    time = 600,
    result = true,
    sync_writes = true,
    time_refresh = true,
    key = "String",
    convert = r##"{ format!("{}/{}@{}", user, group, path) }"##
)]
pub async fn verify_permission_by_table_path(
    user: &str,
    group: &str,
    path: &str,
    meta_data_client: MetaDataClientRef,
) -> crate::Result<()> {
    // jwt token verification has ensured user belongs to the group
    // we only need to verify this table path belongs to the group
    let table_path_id = meta_data_client.as_ref().get_table_path_id_by_table_path(path).await?;
    log::debug!("table {} in domain {}", path, table_path_id.domain);
    match table_path_id.domain.as_str() {
        "public" | "lake-public" => Ok(()),
        domain if domain == group => Ok(()),
        _ => Err(LakeSoulMetaDataError::Other(
            format!(
                "permission denied to access {} from user {} in group {}",
                path, user, group
            )
            .into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaDataClient;
    use cached::Cached;
    use proto::proto::entity::TableInfo;
    use std::sync::Arc;

    async fn create_table(
        table_name: &str,
        path: &str,
        domain: &str,
        meta_data_client: MetaDataClientRef,
    ) -> crate::Result<String> {
        let table_id = format!("table_{}", uuid::Uuid::new_v4());
        let ti = TableInfo {
            table_id: table_id.clone(),
            table_name: table_name.to_string(),
            table_path: path.to_string(),
            table_schema: String::new(),
            table_namespace: "default".to_string(),
            properties: "{}".to_string(),
            partitions: "id;range".to_string(),
            domain: domain.to_string(),
        };
        meta_data_client.create_table(ti).await?;
        Ok(table_id)
    }

    async fn drop_table(table_id: &str, path: &str, meta_data_client: MetaDataClientRef) -> crate::Result<()> {
        meta_data_client.delete_table_by_table_id_cascade(table_id, path).await
    }

    async fn clear_cache() {
        let mut cache = VERIFY_PERMISSION_BY_TABLE_NAME.lock().await;
        cache.cache_clear();
    }

    #[tokio::test]
    async fn test_verify_permission() -> crate::Result<()> {
        env_logger::init();
        let metadata_client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_rbac_table";
        let table_path = "file:///tmp/table";
        let uuid = create_table(table_name, table_path, "lake-public", metadata_client.clone()).await?;
        let r = verify_permission_by_table_name(
            "lake-iam-001",
            "lake-czods",
            "default",
            table_name,
            metadata_client.clone(),
        )
        .await;
        assert!(r.is_ok());
        drop_table(uuid.as_str(), table_path, metadata_client.clone()).await?;

        clear_cache().await;

        let uuid = create_table(table_name, table_path, "lake-czads", metadata_client.clone()).await?;
        let r = verify_permission_by_table_name(
            "lake-iam-001",
            "lake-czods",
            "default",
            table_name,
            metadata_client.clone(),
        )
        .await;
        assert!(r.is_err());
        assert!(r.err().unwrap().to_string().contains(
            "permission denied to access default.test_rbac_table from user lake-iam-001 in group lake-czods"
        ));
        drop_table(uuid.as_str(), table_path, metadata_client).await?;
        Ok(())
    }
}
