use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;

use crate::pg_catalog::context::PgCatalogContextProvider;

#[derive(Debug, Clone)]
pub(crate) struct PgRolesTable<P> {
    schema: SchemaRef,
    context_provider: P,
}

impl<P: PgCatalogContextProvider> PgRolesTable<P> {
    pub(crate) fn new(context_provider: P) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("rolname", DataType::Utf8, true),
            Field::new("rolsuper", DataType::Boolean, true),
            Field::new("rolinherit", DataType::Boolean, true),
            Field::new("rolcreaterole", DataType::Boolean, true),
            Field::new("rolcreatedb", DataType::Boolean, true),
            Field::new("rolcanlogin", DataType::Boolean, true),
            Field::new("rolreplication", DataType::Boolean, true),
            Field::new("rolconnlimit", DataType::Int32, true),
            Field::new("rolpassword", DataType::Utf8, true),
            Field::new(
                "rolvaliduntil",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Microsecond,
                    Some(Arc::from("UTC")),
                ),
                true,
            ),
            Field::new("rolbypassrls", DataType::Boolean, true),
            Field::new(
                "rolconfig",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("oid", DataType::Int32, true),
        ]));

        Self {
            schema,
            context_provider,
        }
    }

    async fn get_data(this: Self) -> Result<RecordBatch> {
        let mut rolname = Vec::new();
        let mut rolsuper = Vec::new();
        let mut rolinherit = Vec::new();
        let mut rolcreaterole = Vec::new();
        let mut rolcreatedb = Vec::new();
        let mut rolcanlogin = Vec::new();
        let mut rolreplication = Vec::new();
        let mut rolconnlimit = Vec::new();
        let mut rolpassword: Vec<Option<String>> = Vec::new();
        let mut rolvaliduntil: Vec<Option<i64>> = Vec::new();
        let mut rolbypassrls = Vec::new();
        let mut rolconfig: Vec<Option<Vec<String>>> = Vec::new();
        let mut oid: Vec<i32> = Vec::new();

        for role_name in &this.context_provider.roles().await {
            let role = &this.context_provider.role(role_name).await.unwrap();
            rolname.push(role.name.clone());
            rolsuper.push(role.is_superuser);
            rolinherit.push(true);
            rolcreaterole.push(role.can_create_role);
            rolcreatedb.push(role.can_create_db);
            rolcanlogin.push(role.can_login);
            rolreplication.push(role.can_replication);
            rolconnlimit.push(-1);
            rolpassword.push(None);
            rolvaliduntil.push(None);
            rolbypassrls.push(None);
            rolconfig.push(None);
            oid.push(0); // TODO: handle oid properly somehow
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(rolname)),
            Arc::new(BooleanArray::from(rolsuper)),
            Arc::new(BooleanArray::from(rolinherit)),
            Arc::new(BooleanArray::from(rolcreaterole)),
            Arc::new(BooleanArray::from(rolcreatedb)),
            Arc::new(BooleanArray::from(rolcanlogin)),
            Arc::new(BooleanArray::from(rolreplication)),
            Arc::new(Int32Array::from(rolconnlimit)),
            Arc::new(StringArray::from(rolpassword)),
            Arc::new({
                let mut builder =
                    TimestampMicrosecondBuilder::with_capacity(rolconfig.len()).with_data_type(
                        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                    );
                for field in &rolvaliduntil {
                    builder.append_option(field.as_ref().copied());
                }
                builder.finish()
            }),
            Arc::new(BooleanArray::from(rolbypassrls)),
            Arc::new({
                let mut builder = ListBuilder::new(StringBuilder::new());
                for field in &rolconfig {
                    match field {
                        Some(values) => {
                            for s in values {
                                builder.values().append_value(s);
                            }
                            builder.append(true);
                        }
                        None => builder.append(false),
                    }
                }
                builder.finish()
            }),
            Arc::new(Int32Array::from(oid)),
        ];

        Ok(RecordBatch::try_new(this.schema.clone(), arrays)?)
    }
}

impl<P: PgCatalogContextProvider> PartitionStream for PgRolesTable<P> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { PgRolesTable::get_data(this).await }),
        ))
    }
}
