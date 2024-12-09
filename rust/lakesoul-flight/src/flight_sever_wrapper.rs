use std::{future::Future, pin::Pin, sync::Arc, task::{Context, Poll}};
use arrow::error::ArrowError;
use arrow_flight::{flight_service_server::{FlightService, FlightServiceServer}, SchemaAsIpc};
use arrow_flight::sql::server::FlightSqlService;
use http_body::Body;
use lakesoul_datafusion::serialize::arrow_java::schema_from_metadata_str;
use log::info;
use tonic::{body::BoxBody, codec::EnabledCompressionEncodings, codegen::Service, Response};
use tonic::transport::NamedService;
use tonic::codegen::{StdError, BoxFuture};
use http::Request;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

#[derive(Debug)]
pub struct FlightServiceServerWrapper<T: FlightService> {
    inner: FlightServiceServer<T>,
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
    metadata_client: MetaDataClientRef,
}

impl<T: FlightService> FlightServiceServerWrapper<T> {
    pub fn new(service: T, metadata_client: MetaDataClientRef) -> Self {
        Self {
            inner: FlightServiceServer::new(service),
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            max_decoding_message_size: None,
            max_encoding_message_size: None,
            metadata_client: metadata_client,
        }
    }
}

impl<T, B> tonic::codegen::Service<http::Request<B>> for FlightServiceServerWrapper<T> 
where
    T: FlightService,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        if req.uri().path() == "/arrow.flight.protocol.FlightService/GetSchema" {
            #[allow(non_camel_case_types)]
            struct GetSchemaSvc();
            impl tonic::server::UnaryService<arrow_flight::FlightDescriptor>
            for GetSchemaSvc {
                type Response = arrow_flight::SchemaResult;
                type Future = BoxFuture<
                    tonic::Response<Self::Response>,
                    tonic::Status,
                >;
                fn call(
                    &mut self,
                    request: tonic::Request<arrow_flight::FlightDescriptor>,
                ) -> Self::Future {
                    let request = request.into_inner();
                    let fut = async move {
                        info!("GetSchema request: {:?}", request);
                        let schema = match request.r#type {
                            1 => {
                                if request.path.is_empty() {
                                    return Err(tonic::Status::invalid_argument("Path not provided"));
                                }
                                let table_name = request.path.last().unwrap().to_string();
                                let namespace = request.path[..request.path.len()-1].join(".");
                                let table_info = Arc::new(MetaDataClient::from_env().await.map_err(|e| tonic::Status::internal(e.to_string()))?).get_table_info_by_table_name(&table_name, &namespace).await.map_err(|e| tonic::Status::internal(e.to_string()))?;
                                let schema = schema_from_metadata_str(&table_info.table_schema);
                                schema
                            }
                            _ => return Err(tonic::Status::unimplemented("Only path-based schema lookup supported")),
                        };

                        let options = arrow::ipc::writer::IpcWriteOptions::default();
                        let schema_result = SchemaAsIpc::new(&schema, &options)
                            .try_into()
                            .map_err(|e: ArrowError| tonic::Status::internal(e.to_string()))?;

                        Ok(Response::new(schema_result))

                    };
                    Box::pin(fut)
                }
            }
            let accept_compression_encodings = self.accept_compression_encodings;
            let send_compression_encodings = self.send_compression_encodings;
            let max_decoding_message_size = self.max_decoding_message_size;
            let max_encoding_message_size = self.max_encoding_message_size;
            let fut = async move {
                let method = GetSchemaSvc();
                let codec = tonic::codec::ProstCodec::default();
                let mut grpc = tonic::server::Grpc::new(codec)
                    .apply_compression_config(
                        accept_compression_encodings,
                        send_compression_encodings,
                    )
                    .apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                let res = grpc.unary(method, req).await;
                Ok(res)
            };
            Box::pin(fut)
            // return self.inner.call(req);
        } else {
            self.inner.call(req)
        }
    }
}

impl<T: FlightService> Clone for FlightServiceServerWrapper<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            max_decoding_message_size: self.max_decoding_message_size,
            max_encoding_message_size: self.max_encoding_message_size,
            metadata_client: self.metadata_client.clone(),
        }
    }
}

impl<T: FlightService> NamedService for FlightServiceServerWrapper<T> {
    const NAME: &'static str = "arrow.flight.protocol.FlightService";
}
