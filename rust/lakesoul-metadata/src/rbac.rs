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
    use std::sync::Arc;
    #[tokio::test]
    async fn test_verify_permission() -> crate::Result<()> {
        let metadata_client = Arc::new(MetaDataClient::from_env().await?);
        verify_permission_by_table_name("lake-iam-001", "lake-czods", "default", "sink_table", metadata_client).await?;
        Ok(())
    }
}
