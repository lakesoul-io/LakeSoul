use crate::jwt::Claims;
use lakesoul_datafusion::{LakeSoulError, Result};
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::MetaDataClientRef;

pub(crate) async fn verify_permission(
    claims: Claims,
    ns: &str,
    table: &str,
    meta_data_client: MetaDataClientRef,
) -> Result<()> {
    // jwt token verification has ensured user belongs to the group
    // we only need to verify this table belongs to the group
    let table_name_id = meta_data_client
        .as_ref()
        .get_table_name_id_by_table_name(table, ns)
        .await?;
    log::debug!("table {}.{} in domain {}", ns, table, table_name_id.domain);
    match table_name_id.domain.as_str() {
        "public" | "lake-public" => Ok(()),
        domain if domain == claims.group => Ok(()),
        _ => Err(LakeSoulError::MetaDataError(LakeSoulMetaDataError::Other(
            format!(
                "permission denied to access {}.{} from user {} in group {}",
                ns, table, claims.sub, claims.group
            )
            .into(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jwt::JwtServer;
    use chrono::Days;
    use lakesoul_metadata::MetaDataClient;
    use std::sync::Arc;
    #[tokio::test]
    async fn test_verify_permission() -> Result<()> {
        let metadata_client = Arc::new(MetaDataClient::from_env().await?);
        let jwt_server = JwtServer::new(metadata_client.get_client_secret().as_str());
        let claims = Claims {
            sub: "lake-iam-001".to_string(),
            group: "lake-czods".to_string(),
            exp: chrono::Utc::now().checked_add_days(Days::new(1)).unwrap().timestamp() as usize,
        };
        let token = jwt_server
            .create_token(claims)
            .map_err(|e| LakeSoulError::MetaDataError(LakeSoulMetaDataError::Other(Box::new(e))))?;
        println!("{:?}", token);
        let decoded_claims = jwt_server
            .decode_token(token.as_str())
            .map_err(|e| LakeSoulError::MetaDataError(LakeSoulMetaDataError::Other(Box::new(e))))?;
        println!("{:?}", decoded_claims);
        verify_permission(decoded_claims, "default", "sink_table", metadata_client).await?;
        Ok(())
    }
}
