// ... 在文件开头添加以下代码 ...

use std::sync::Arc;
use datafusion::error::DataFusionError;
use lakesoul_metadata::MetaDataClientRef;
use log::{debug, info};
use datafusion::datasource::provider::TableProviderFactory;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::CreateExternalTable;

use crate::datasource::table_provider::LakeSoulTableProvider;
pub struct LakeSoulTableProviderFactory {
    metadata_client: MetaDataClientRef,
}

impl LakeSoulTableProviderFactory {
    pub fn new(metadata_client: MetaDataClientRef) -> Self {
        Self {
            metadata_client,
        }
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }

}

#[async_trait::async_trait]
impl TableProviderFactory for LakeSoulTableProviderFactory {
    async fn create(
        &self, 
        state: &SessionState,
        cmd: &CreateExternalTable
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        info!("LakeSoulTableProviderFactory::create: {:?}, {:?}", cmd.name, cmd.location);

        Ok(Arc::new(LakeSoulTableProvider::new_from_create_external_table(state, self.metadata_client(), cmd)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?))
    }
}