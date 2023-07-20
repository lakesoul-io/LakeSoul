#![feature(split_array)]
use std::sync::Arc;
use proto::proto::entity;
use prost::Message;

use tokio::runtime::{Builder, Runtime};

use tokio_postgres::{NoTls, Client};

#[derive(Debug)]
pub struct RuntimeClient{
    runtime: Arc<Runtime>,
    pub(crate) client: Arc<Client>,
}

impl RuntimeClient {
}

enum QueryType {
    FindByNamespace(String, usize),
}

fn parse_query_type(_query_type: String) -> Result<QueryType, std::io::Error> {
    Ok(QueryType::FindByNamespace("select * from namespace where namespace = '$1::TEXT'".to_string(), 1))
}

pub fn execute_query(
    query_type: String, 
    joined_string: String, 
    delim: String
) -> Result<Vec<u8>, std::io::Error> {
    // println!("{:?}", runtime_client);
    let runtime =  Builder::new_multi_thread()
    .enable_all()
    .worker_threads(2)
    .max_blocking_threads(8)
    .build()
    .unwrap();
    let (client, connection) = match runtime.block_on(async {
        tokio_postgres::connect("host=127.0.0.1 port=5433 dbname=test_lakesoul_meta user=yugabyte password=yugabyte", NoTls).await
    }) {
        Ok((client, connection))=>(client, connection),
        Err(e)=>{
            eprintln!("{}", e);
            return Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused))
        }
    };

    runtime.spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let params = joined_string.split(delim.as_str()).collect::<Vec<&str>>();
    
    match parse_query_type(query_type)? {
        QueryType::FindByNamespace(_, _)=>{
            let params = params.iter().map(|str|str.to_string()).collect::<Vec<String>>();
            let result = runtime.block_on(async{client.query("select namespace, properties, comment, domain from namespace where namespace = $1::TEXT", &[&params[0]]).await});
            let result = match result {
                Ok(rows) => rows,
                Err(e) => return Err(convert_to_io_error(e))
            };
            let namespaces:Vec<entity::Namespace> = 
                result
                    .iter()
                    .map(|row|proto::proto::entity::Namespace { 
                        namespace: row.get(0), 
                        properties: row.get::<_, serde_json_1::Value>(1).to_string(), 
                        comment: row.get(2), 
                        domain: row.get(3)
                    })
                    .collect();

            let wrapper = proto::proto::entity::JniWrapper { 
                namespace: namespaces 
            };
            Ok(wrapper.encode_to_vec())
        }
    }
}

fn convert_to_io_error(err:tokio_postgres::Error) -> std::io::Error {
    match err.code().unwrap() {
        &tokio_postgres::error::SqlState::SUCCESSFUL_COMPLETION => std::io::Error::from(std::io::ErrorKind::Other),
        _ => std::io::Error::from(std::io::ErrorKind::ConnectionRefused)
    }
}

pub fn create_connection(config: String) -> Result<RuntimeClient, std::io::Error>
{
    let runtime = match Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .max_blocking_threads(8)
        .build() {
            Ok(runtime)=>runtime,
            Err(e)=>return Err(e)
        };
    
    let (client, connection) = match runtime.block_on(async {
        tokio_postgres::connect(config.as_str(), NoTls).await
    }) {
        Ok((client, connection))=>(client, connection),
        Err(e)=>{
            eprintln!("{}", e);
            return Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused))
        }
    };

    runtime.spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });


    Ok(RuntimeClient { 
        runtime: Arc::new(runtime),
        client: Arc::new(client) })
}


#[cfg(test)]
mod tests {
    use proto::proto::entity;
    use prost::Message;
    #[tokio::test]
    async fn test_entity() -> std::io::Result<()> {
        let namespace = entity::Namespace {
            namespace:"default".to_owned(), 
            properties:"{}".to_owned(), 
            comment:"".to_owned(),
            domain:"public".to_owned(),
        };
        println!("{:?}", namespace);
        println!("{:?}", entity::Namespace::default());

        let table_info = entity::TableInfo {
            table_id: uuid::Uuid::new_v4().to_string(),
            table_namespace: "default".to_owned(),
            table_name: "test_table_name".to_owned(),
            table_path: "test_table_path".to_owned(),
            table_schema: "StructType {}".to_owned(),
            properties: "{}".to_owned(),
            partitions: "".to_owned(),
            domain:"public".to_owned(),
        };
        println!("{:?}", table_info);
        println!("{:?}", table_info.encode_to_vec());
        println!("{:?}", table_info.encode_length_delimited_to_vec());
        println!("{:?}", table_info.encode_length_delimited_to_vec().len());
        println!("{:?}", entity::TableInfo::default());


        let meta_info = entity::MetaInfo {
            list_partition: vec![],
            table_info: core::option::Option::None,
            read_partition_info: vec![]
        };
        println!("{:?}", meta_info);
        println!("{:?}", entity::MetaInfo::default());
        println!("{:?}", entity::TableNameId::default());
        println!("{:?}", entity::TablePathId::default());
        println!("{:?}", entity::PartitionInfo::default());
        println!("{:?}", entity::DataCommitInfo::default());

        let mut wrapper = entity::JniWrapper::default();
        println!("{:?}", wrapper);
        wrapper.namespace = vec![namespace];
        println!("{:?}", wrapper.namespace);


        Ok(())
    }
}