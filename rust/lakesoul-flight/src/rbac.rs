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
    match table_name_id.domain.as_str() {
        "public" | "lake-public" => Ok(()),
        domain if domain == claims.group => Ok(()),
        _ => Err(LakeSoulError::from(LakeSoulMetaDataError::Other(
            format!(
                "permission denied to access {}.{} from user {} in group {}",
                ns, table, claims.sub, claims.group
            )
            .into(),
        ))),
    }
}
