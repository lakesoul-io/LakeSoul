use crate::jwt::Claims;
use lakesoul_datafusion::{LakeSoulError, Result};
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::MetaDataClientRef;

pub(crate) async fn verify_permission(
    claims: Claims,
    ns: String,
    table: String,
    meta_data_client: MetaDataClientRef,
) -> Result<()> {
    // jwt token verification has ensured user belong to group
    // we only need to verify this table belongs to the group
    let table_name_id = meta_data_client
        .as_ref()
        .get_table_name_id_by_table_name(table.as_str(), ns.as_str())
        .await?;
    if (table_name_id.domain != claims.group) {
        Err(LakeSoulError::from(LakeSoulMetaDataError::Other(
            format!("permission denied to access {}.{} from user {}", ns, table, claims.sub).into(),
        )))
    } else {
        Ok(())
    }
}
