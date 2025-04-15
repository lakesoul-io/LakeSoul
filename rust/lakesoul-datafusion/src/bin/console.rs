use std::{env, sync::Arc};

use datafusion::{
    execution::{object_store::ObjectStoreUrl, runtime_env::RuntimeEnv, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};
use lakesoul_datafusion::{
    catalog::lakesoul_catalog, datasource::table_factory::LakeSoulTableProviderFactory, LakeSoulQueryPlanner,
};
use lakesoul_metadata::MetaDataClient;
use object_store::local::LocalFileSystem;
use rustyline::{error::ReadlineError, DefaultEditor};
use url::Url;

async fn create_ctx() -> Arc<SessionContext> {
    let mut session_config = SessionConfig::from_env()
        .unwrap()
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(false)
        .with_batch_size(128)
        .with_default_catalog_and_schema("LAKESOUL".to_string(), "default".to_string());
    session_config.options_mut().sql_parser.dialect = "postgresql".to_string();
    session_config.options_mut().optimizer.enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
    session_config.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
    session_config.options_mut().execution.parquet.pushdown_filters = true;
    session_config.options_mut().execution.target_partitions = 1;
    session_config.options_mut().execution.parquet.schema_force_view_types = false;

    let planner = LakeSoulQueryPlanner::new_ref();

    let mut state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(Arc::new(RuntimeEnv::default()))
        .with_query_planner(planner)
        .build();

    let metadata_client = Arc::new(MetaDataClient::from_env().await.unwrap());

    let warehouse_prefix = None;
    let endpoint: Option<String> = None;
    let _s3_bucket: Option<String> = None;
    let s3_access_key: Option<String> = None;
    let s3_secret_key: Option<String> = None;

    state.table_factories_mut().insert(
        "LAKESOUL".to_string(),
        Arc::new(LakeSoulTableProviderFactory::new(
            metadata_client.clone(),
            warehouse_prefix.clone(),
        )),
    );
    let ctx = Arc::new(SessionContext::new_with_state(state));

    let catalog = Arc::new(lakesoul_catalog::LakeSoulCatalog::new(
        metadata_client.clone(),
        ctx.clone(),
    ));

    if let Some(warehouse_prefix) = &warehouse_prefix {
        env::set_var("LAKESOUL_WAREHOUSE_PREFIX", warehouse_prefix);
        let url = Url::parse(warehouse_prefix);
        match url {
            Ok(url) => match url.scheme() {
                "s3" | "s3a" => {
                    if let Some(s3_secret_key) = &s3_secret_key {
                        env::set_var("AWS_SECRET_ACCESS_KEY", s3_secret_key);
                    }
                    if let Some(s3_access_key) = &s3_access_key {
                        env::set_var("AWS_ACCESS_KEY_ID", s3_access_key);
                    }
                    if let Some(endpoint) = &endpoint {
                        env::set_var("AWS_ENDPOINT", endpoint);
                    }

                    if ctx
                        .runtime_env()
                        .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).unwrap())
                        .is_ok()
                    {
                        panic!("Object store already registered")
                    }

                    todo!()
                    // let config = self.get_io_config_builder().build();
                    // register_s3_object_store(&url, &config, &ctx.runtime_env()).map_err(datafusion_error_to_status)?;
                }
                "hdfs" => {
                    if url.has_host() {
                        if ctx
                            .runtime_env()
                            .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).unwrap())
                            .is_ok()
                        {
                            panic!("Object store already registered");
                        }
                        // let config = self.get_io_config_builder().build();
                        // register_hdfs_object_store(
                        //     &url,
                        //     &url[url::Position::BeforeHost..url::Position::BeforePath],
                        //     &config,
                        //     &ctx.runtime_env(),
                        // ).unw
                        panic!("xxxx")
                    } else {
                        // defaultFS should have been registered with hdfs,
                        // and we convert hdfs://user/hadoop/file to
                        // hdfs://defaultFS/user/hadoop/file
                        todo!()
                    }
                }
                "file" => {
                    ctx.runtime_env()
                        .register_object_store(&url, Arc::new(LocalFileSystem::new()));
                }
                _ => {
                    panic!("Invalid scheme of warehouse prefix");
                }
            },
            Err(_) => {
                panic!("Invalid warehouse prefix");
            }
        }
    } else {
        ctx.runtime_env()
            .register_object_store(&Url::parse("file://").unwrap(), Arc::new(LocalFileSystem::new()));
    }

    ctx.state()
        .catalog_list()
        .register_catalog("LAKESOUL".to_string(), catalog);

    // let df = ctx.sql("select * from user_info_1").await.map_err(datafusion_error_to_status)?;
    // let batch = df.collect().await.map_err(datafusion_error_to_status)?;
    // info!("batch: {}", arrow::util::pretty::pretty_format_batches(&batch).unwrap());
    ctx
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let mut rl = DefaultEditor::new().unwrap();
    let ctx = create_ctx().await;
    loop {
        let readline = rl.readline("lakesoul >> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();

                match ctx.sql(line.as_str()).await {
                    Ok(df) => df.show().await.unwrap(),
                    Err(e) => println!("Error: {:?}", e),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("bye bye! 🫡");

                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
