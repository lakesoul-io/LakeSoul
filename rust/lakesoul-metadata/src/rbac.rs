// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::LakeSoulMetaDataError;
use crate::MetaDataClientRef;
use cached::proc_macro::cached;

#[cached(
    size = 1000,
    time = 5,
    result = true,
    sync_writes = true,
    time_refresh = true,
    key = "String",
    convert = r##"{ format!("{}/{}@{}.{}:{}", user, group, ns, table, write) }"##
)]
#[instrument(skip(meta_data_client))]
pub async fn verify_permission_by_table_name(
    user: &str,
    group: &str,
    ns: &str,
    table: &str,
    write: bool,
    meta_data_client: MetaDataClientRef,
) -> crate::Result<()> {
    // The caller supplies the effective identity: the PG connection user and
    // env domain for a direct job, or the token's sub/group for a session
    // service. Both paths must resolve the effective ACL, not just existence.
    let table_name_id = meta_data_client
        .as_ref()
        .get_table_name_id_by_table_name(table, ns)
        .await?
        .ok_or_else(|| {
            LakeSoulMetaDataError::NotFound(format!(
                "Table {}.{} not found, user {}, group {}",
                table, ns, user, group
            ))
        })?;
    let privilege = meta_data_client
        .as_ref()
        .table_privilege_by_id(&table_name_id.table_id, user, group)
        .await?
        .ok_or_else(|| {
            LakeSoulMetaDataError::NotFound(format!(
                "Table {}.{} has no ACL entry, user {}, group {}",
                table, ns, user, group
            ))
        })?;
    debug!(
        "table {}.{} in domain {}, privilege {:?}",
        ns, table, table_name_id.domain, privilege
    );
    if (write && privilege.can_write()) || (!write && privilege.can_read()) {
        Ok(())
    } else {
        Err(LakeSoulMetaDataError::PermissionDenied(format!(
            "user {user} in group {group} may not {} table {ns}.{table}",
            if write { "write" } else { "read" }
        )))
    }
}

/// Verify that `user`/`group` may read or write the table at `path`.
///
/// The ACL resolver reads the table's effective privilege (owner/admin,
/// public, same-domain default, explicit user grant, cross-domain read-only
/// grant). A path that is not a table is `NotFound`, preserving the S3 proxy's
/// existing "unknown path" handling. The short cache bounds revocation
/// latency to a few seconds.
#[cached(
    size = 1000,
    time = 5,
    result = true,
    sync_writes = true,
    time_refresh = true,
    key = "String",
    convert = r##"{ format!("{}/{}@{}:{}", user, group, path, write) }"##
)]
#[instrument(skip(meta_data_client))]
pub async fn verify_permission_by_table_path(
    user: &str,
    group: &str,
    path: &str,
    write: bool,
    meta_data_client: MetaDataClientRef,
) -> crate::Result<()> {
    let privilege = meta_data_client
        .table_privilege_by_path(path, user, group)
        .await?
        .ok_or_else(|| {
            LakeSoulMetaDataError::NotFound(format!(
                "Path is not a LakeSoul table {path}, user {user}, group {group}"
            ))
        })?;
    if (write && privilege.can_write()) || (!write && privilege.can_read()) {
        Ok(())
    } else {
        Err(LakeSoulMetaDataError::PermissionDenied(format!(
            "user {user} in group {group} may not {} table {path}",
            if write { "write" } else { "read" }
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaDataClient;
    use cached::Cached;
    use lakesoul_metadata_proto::entity::TableInfo;
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
            table_schema_arrow_ipc: vec![],
            table_schema_arrow_ipc_json_hash: String::new(),
            table_namespace: "default".to_string(),
            properties: "{}".to_string(),
            partitions: "id;range".to_string(),
            domain: domain.to_string(),
            creator: String::new(),
        };
        meta_data_client.create_table(ti).await?;
        Ok(table_id)
    }

    async fn drop_table(
        table_id: &str,
        path: &str,
        meta_data_client: MetaDataClientRef,
    ) -> crate::Result<()> {
        meta_data_client
            .delete_table_by_table_id_cascade(table_id, path)
            .await
    }

    async fn clear_cache() {
        let mut cache = VERIFY_PERMISSION_BY_TABLE_NAME.lock().await;
        cache.cache_clear();
    }

    #[tokio::test]
    async fn test_verify_permission() -> crate::Result<()> {
        let metadata_client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = format!("test_rbac_table_{}", uuid::Uuid::new_v4().simple());
        let table_path = format!("file:///tmp/{}", table_name);
        let uuid = create_table(
            &table_name,
            &table_path,
            "lake-public",
            metadata_client.clone(),
        )
        .await?;
        // Public tables are open to every domain.
        assert!(
            verify_permission_by_table_name(
                "lake-iam-001",
                "lake-czods",
                "default",
                &table_name,
                false,
                metadata_client.clone(),
            )
            .await
            .is_ok()
        );
        drop_table(uuid.as_str(), &table_path, metadata_client.clone()).await?;

        clear_cache().await;

        let uuid = create_table(
            &table_name,
            &table_path,
            "lake-czads",
            metadata_client.clone(),
        )
        .await?;
        // A different domain cannot read the private table, even though the
        // table exists; this used to be a no-op check.
        assert!(
            verify_permission_by_table_name(
                "lake-iam-001",
                "lake-czods",
                "default",
                &table_name,
                false,
                metadata_client.clone(),
            )
            .await
            .is_err()
        );
        // The owner's own domain still can.
        assert!(
            verify_permission_by_table_name(
                "lake-iam-001",
                "lake-czads",
                "default",
                &table_name,
                false,
                metadata_client.clone(),
            )
            .await
            .is_ok()
        );
        assert!(
            verify_permission_by_table_path(
                "lake-iam-001",
                "lake-czads",
                &table_path,
                false,
                metadata_client.clone(),
            )
            .await
            .is_ok()
        );
        assert!(
            verify_permission_by_table_path(
                "lake-iam-001",
                "lake-czods",
                &table_path,
                false,
                metadata_client.clone(),
            )
            .await
            .is_err()
        );

        drop_table(uuid.as_str(), &table_path, metadata_client).await?;
        Ok(())
    }
}
