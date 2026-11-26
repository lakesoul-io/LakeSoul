// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Server-side cursors for the PostgreSQL wire front end.
//!
//! The upstream cursor hook answers `DECLARE`/`FETCH` from a portal whose row
//! stream is produced after the statement that declared it has finished. A
//! cancel or a `statement_timeout` therefore cannot reach the execution
//! behind the stream. This module keeps the cursor stream in connection-local
//! state instead: `DECLARE` creates it under that statement's cancellation,
//! and every `FETCH` takes the cursor out of the store while it pulls rows
//! under the fetch's own cancellation. A cancelled fetch drops the stream (and
//! with it the execution) instead of leaving distributed work running behind
//! a suspended portal.
//!
//! Only forward-only, non-holdable cursors are accepted for now. `WITH HOLD`,
//! `SCROLL`, binary cursors and backward fetches are refused with an explicit
//! `0A000`/`42000`, never silently downgraded.
//!
//! `pg_cursors` is answered from the same state. The catalog has no such
//! relation, and psycopg2 checks it before issuing `DECLARE`; the hook answers
//! the simple probe and the common column projections directly.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DFSchema, ParamValues};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, CloseCursor, Declare, DeclareType, Expr, FetchDirection, Ident,
    ObjectName, SelectItem, SetExpr, Statement, Value, ValueWithSpan,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::arrow_pg::datatypes::arrow_schema_to_pg_fields;
use datafusion_postgres::arrow_pg::encode_recordbatch;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::portal::{Format, Portal};
use datafusion_postgres::pgwire::api::results::{
    FieldInfo, QueryResponse, Response, Tag,
};
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::Mutex;

/// `duplicate_cursor`: a cursor with this name is already open.
const DUPLICATE_CURSOR: &str = "42P03";
/// `invalid_cursor_name`: the requested cursor does not exist.
const INVALID_CURSOR_NAME: &str = "34000";
/// `feature_not_supported`.
const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// `syntax_error`.
const SYNTAX_ERROR: &str = "42601";
/// PostgreSQL's "cursor can only scan forward".
const CURSOR_ONLY_FORWARD: &str = "42000";

/// One connection's open cursors.
#[derive(Default)]
pub(crate) struct CursorStore {
    cursors: Mutex<BTreeMap<String, CursorState>>,
}

impl CursorStore {
    fn contains(&self, name: &str) -> bool {
        self.cursors.lock().contains_key(name)
    }

    fn insert(&self, name: String, cursor: CursorState) {
        self.cursors.lock().insert(name, cursor);
    }

    fn take(&self, name: &str) -> Option<CursorState> {
        self.cursors.lock().remove(name)
    }

    fn remove(&self, name: &str) {
        self.cursors.lock().remove(name);
    }

    fn clear(&self) {
        self.cursors.lock().clear();
    }

    fn names(&self) -> Vec<String> {
        self.cursors.lock().keys().cloned().collect()
    }

    fn snapshot(&self) -> Vec<CursorInfo> {
        self.cursors
            .lock()
            .iter()
            .map(|(name, cursor)| CursorInfo {
                name: name.clone(),
                statement: cursor.statement.clone(),
                is_holdable: cursor.holdable,
                is_binary: cursor.binary,
                is_scrollable: cursor.scrollable,
                created_at: cursor.created_at,
            })
            .collect()
    }
}

/// One open cursor's result stream and the rows already pulled from it.
struct CursorState {
    statement: String,
    fields: Arc<Vec<FieldInfo>>,
    schema: SchemaRef,
    stream: Option<SendableRecordBatchStream>,
    /// A batch and the index of its first row not returned yet.
    pending: Option<(RecordBatch, usize)>,
    holdable: bool,
    binary: bool,
    scrollable: bool,
    created_at: DateTime<Utc>,
}

impl CursorState {
    fn new(
        statement: String,
        fields: Arc<Vec<FieldInfo>>,
        schema: SchemaRef,
        stream: SendableRecordBatchStream,
    ) -> Self {
        Self {
            statement,
            fields,
            schema,
            stream: Some(stream),
            pending: None,
            holdable: false,
            binary: false,
            scrollable: false,
            created_at: Utc::now(),
        }
    }

    fn done(&self) -> Self {
        Self {
            statement: self.statement.clone(),
            fields: Arc::clone(&self.fields),
            schema: Arc::clone(&self.schema),
            stream: None,
            pending: None,
            holdable: false,
            binary: false,
            scrollable: false,
            created_at: self.created_at,
        }
    }

    /// Pull at most `limit` rows (0 means no rows) from the pending batch and
    /// then from the stream. The stream is polled while the caller's FETCH is
    /// under its cancellation token, so an interrupted fetch drops it.
    async fn fetch_batches(&mut self, limit: usize) -> PgWireResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        let mut remaining = limit;
        if let Some((batch, offset)) = self.pending.take() {
            let available = batch.num_rows() - offset;
            let take = available.min(remaining);
            if take > 0 {
                batches.push(batch.slice(offset, take));
                remaining -= take;
            }
            if take < available {
                self.pending = Some((batch, offset + take));
            }
        }
        while remaining > 0 {
            let Some(stream) = self.stream.as_mut() else {
                break;
            };
            match stream.next().await {
                Some(Ok(batch)) => {
                    let take = batch.num_rows().min(remaining);
                    if take > 0 {
                        batches.push(batch.slice(0, take));
                        remaining -= take;
                    }
                    if take < batch.num_rows() {
                        self.pending = Some((batch, take));
                    }
                }
                Some(Err(error)) => {
                    self.stream = None;
                    return Err(PgWireError::ApiError(Box::new(error)));
                }
                None => {
                    self.stream = None;
                    break;
                }
            }
        }
        Ok(batches)
    }

    /// The rows of the cursor, streamed to the response rather than collected.
    fn into_rows(self) -> BoxStream<'static, PgWireResult<DataRow>> {
        let fields = Arc::clone(&self.fields);
        let pending = self.pending.map(|(batch, offset)| {
            let length = batch.num_rows() - offset;
            batch.slice(offset, length)
        });
        let pending = pending
            .into_iter()
            .map(|batch| {
                let fields = Arc::clone(&fields);
                stream::iter(encode_recordbatch(fields, batch).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let head = stream::iter(pending).flatten();
        match self.stream {
            Some(stream) => {
                let fields = Arc::clone(&fields);
                let tail = stream.flat_map(move |batch| {
                    let fields = Arc::clone(&fields);
                    match batch {
                        Ok(batch) => stream::iter(
                            encode_recordbatch(fields, batch).collect::<Vec<_>>(),
                        ),
                        Err(error) => stream::iter(vec![Err(PgWireError::ApiError(
                            Box::new(error),
                        ))]),
                    }
                });
                head.chain(tail).boxed()
            }
            None => head.boxed(),
        }
    }
}

/// One row of `pg_cursors`.
struct CursorInfo {
    name: String,
    statement: String,
    is_holdable: bool,
    is_binary: bool,
    is_scrollable: bool,
    created_at: DateTime<Utc>,
}

fn store<C>(client: &C) -> Arc<CursorStore>
where
    C: ClientInfo + ?Sized,
{
    client
        .session_extensions()
        .get_or_insert_with(CursorStore::default)
}

/// Drops every cursor at the end of a transaction block.
///
/// Non-holdable cursors do not survive `COMMIT`/`ROLLBACK`; `WITH HOLD` is
/// refused, so the whole store goes.
pub(crate) fn clear(client: &(impl ClientInfo + ?Sized)) {
    if let Some(store) = client.session_extensions().get::<CursorStore>() {
        store.clear();
    }
}

/// Clears the cursors and the portals that carry their described shape.
///
/// `DISCARD ALL` includes `CLOSE ALL`; `COMMIT`/`ROLLBACK` already clear every
/// portal through the transaction hook.
pub(crate) fn clear_with_portals(client: &mut dyn HookClient) {
    let store = store(client);
    for name in store.names() {
        client.portal_store().rm_portal(&name);
    }
    store.clear();
}

fn cursor_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message,
    )))
}

fn duplicate_cursor(name: &str) -> PgWireError {
    cursor_error(
        DUPLICATE_CURSOR,
        format!("cursor \"{name}\" already exists"),
    )
}

fn missing_cursor(name: &str) -> PgWireError {
    cursor_error(
        INVALID_CURSOR_NAME,
        format!("cursor \"{name}\" does not exist"),
    )
}

fn unsupported_cursor(feature: &str) -> PgWireError {
    cursor_error(
        FEATURE_NOT_SUPPORTED,
        format!("{feature} is not supported for server-side cursors"),
    )
}

/// The row limit a FETCH direction asks for: `None` means all rows.
fn fetch_limit(direction: &FetchDirection) -> PgWireResult<Option<usize>> {
    match direction {
        FetchDirection::Next | FetchDirection::Forward { limit: None } => Ok(Some(1)),
        FetchDirection::Forward { limit: Some(value) }
        | FetchDirection::Count { limit: value } => Ok(Some(number(value)?)),
        FetchDirection::ForwardAll | FetchDirection::All => Ok(None),
        FetchDirection::Prior
        | FetchDirection::Backward { .. }
        | FetchDirection::BackwardAll
        | FetchDirection::First
        | FetchDirection::Last
        | FetchDirection::Absolute { .. }
        | FetchDirection::Relative { .. } => Err(cursor_error(
            CURSOR_ONLY_FORWARD,
            "cursor can only scan forward".to_string(),
        )),
    }
}

fn number(value: &ValueWithSpan) -> PgWireResult<usize> {
    match &value.value {
        Value::Number(number, _) => number.parse().map_err(|_| {
            cursor_error(SYNTAX_ERROR, format!("invalid FETCH count: {number}"))
        }),
        other => Err(cursor_error(
            SYNTAX_ERROR,
            format!("invalid FETCH count: {other}"),
        )),
    }
}

async fn declare(
    declarations: &[Declare],
    params: Option<&ParamValues>,
    session_context: &SessionContext,
    client: &mut dyn HookClient,
) -> PgWireResult<Response> {
    for declaration in declarations {
        if declaration.declare_type != Some(DeclareType::Cursor) {
            return Err(cursor_error(
                SYNTAX_ERROR,
                "only DECLARE CURSOR is supported".to_string(),
            ));
        }
        if declaration.binary == Some(true) {
            return Err(unsupported_cursor("BINARY"));
        }
        if declaration.scroll == Some(true) {
            return Err(unsupported_cursor("SCROLL"));
        }
        if declaration.hold == Some(true) {
            return Err(unsupported_cursor("WITH HOLD"));
        }
        if declaration.names.len() != 1 {
            return Err(cursor_error(
                SYNTAX_ERROR,
                "a cursor declaration needs exactly one name".to_string(),
            ));
        }
        let name = &declaration.names[0].value;
        let Some(query) = &declaration.for_query else {
            return Err(cursor_error(
                SYNTAX_ERROR,
                "DECLARE CURSOR requires a FOR query".to_string(),
            ));
        };
        let cursors = store(client);
        if cursors.contains(name) {
            return Err(duplicate_cursor(name));
        }
        let statement = query.to_string();
        let (dataframe, plan) = match params {
            Some(params) => {
                let planned = session_context
                    .state()
                    .statement_to_plan(DfStatement::Statement(Box::new(
                        Statement::Query(query.clone()),
                    )))
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let planned = planned
                    .replace_params_with_values(params)
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let dataframe = session_context
                    .execute_logical_plan(planned.clone())
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                (dataframe, planned)
            }
            _ => {
                let dataframe = session_context
                    .sql(&statement)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let plan = dataframe.logical_plan().clone();
                (dataframe, plan)
            }
        };
        let schema = Arc::new(dataframe.schema().as_arrow().clone());
        let fields = Arc::new(arrow_schema_to_pg_fields(
            &schema,
            &Format::UnifiedText,
            None,
        )?);
        let ast = Statement::Query(query.clone());
        let stream = dataframe
            .execute_stream()
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        cursors.insert(
            name.to_string(),
            CursorState::new(
                statement.clone(),
                Arc::clone(&fields),
                Arc::clone(&schema),
                stream,
            ),
        );
        // psycopg3's ServerCursor describes the cursor portal after DECLARE
        // and expects the driver-side cursor name to resolve. The portal only
        // carries the result shape; FETCH itself still runs through the
        // cancellation-aware cursor state above.
        let stored = Arc::new(StoredStatement::new(
            name.to_string(),
            (statement, Some((ast, plan))),
            Vec::new(),
        ));
        let portal = Portal::new_cursor(name.to_string(), stored);
        portal
            .start(QueryResponse::new(fields, stream::empty()))
            .await;
        client.portal_store().put_portal(Arc::new(portal));
    }
    Ok(Response::Execution(Tag::new("DECLARE CURSOR")))
}

async fn fetch(
    name: &Ident,
    direction: &FetchDirection,
    into: &Option<ObjectName>,
    client: &(impl ClientInfo + ?Sized),
) -> PgWireResult<Response> {
    if into.is_some() {
        return Err(unsupported_cursor("FETCH ... INTO"));
    }
    let limit = fetch_limit(direction)?;
    let cursors = store(client);
    let Some(mut cursor) = cursors.take(&name.value) else {
        return Err(missing_cursor(&name.value));
    };
    match limit {
        None => {
            let fields = Arc::clone(&cursor.fields);
            let done = cursor.done();
            let rows = cursor.into_rows();
            cursors.insert(name.value.clone(), done);
            let mut response = QueryResponse::new(fields, rows);
            response.set_command_tag("FETCH");
            Ok(Response::Query(response))
        }
        Some(limit) => {
            let batches = cursor.fetch_batches(limit).await?;
            let fields = Arc::clone(&cursor.fields);
            let rows = batches
                .into_iter()
                .flat_map(|batch| {
                    encode_recordbatch(Arc::clone(&fields), batch).collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let count = rows.len();
            let done = cursor.stream.is_none() && cursor.pending.is_none();
            cursors.insert(
                name.value.clone(),
                if done { cursor.done() } else { cursor },
            );
            let mut response = QueryResponse::new(fields, stream::iter(rows));
            response.set_command_tag(&format!("FETCH {count}"));
            Ok(Response::Query(response))
        }
    }
}

fn close(cursor: &CloseCursor, client: &mut dyn HookClient) -> Response {
    let cursors = store(client);
    match cursor {
        CloseCursor::All => {
            for name in cursors.names() {
                client.portal_store().rm_portal(&name);
            }
            cursors.clear();
        }
        CloseCursor::Specific { name } => {
            cursors.remove(&name.value);
            client.portal_store().rm_portal(&name.value);
        }
    }
    Response::Execution(Tag::new("CLOSE CURSOR"))
}

/// Cancellation-aware `DECLARE`/`FETCH`/`CLOSE`.
pub(crate) struct CancellableCursorHook;

impl QueryHook for CancellableCursorHook {
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            match statement {
                Statement::Declare { stmts } => {
                    Some(declare(stmts, None, session_context, client).await)
                }
                Statement::Fetch {
                    name,
                    direction,
                    into,
                    ..
                } => Some(fetch(name, direction, into, client).await),
                Statement::Close { cursor } => Some(Ok(close(cursor, client))),
                _ => None,
            }
        })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        statement: &'a Statement,
        _session_context: &'a SessionContext,
        client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<LogicalPlan>>> {
        Box::pin(async move {
            match statement {
                Statement::Declare { .. } | Statement::Close { .. } => {
                    Some(Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(DFSchema::empty()),
                    })))
                }
                Statement::Fetch { name, .. } => {
                    let cursors = store(client);
                    let schemas = cursors.cursors.lock();
                    let schema = schemas
                        .get(&name.value)
                        .map(|cursor| cursor.schema.clone())
                        .unwrap_or_else(|| Arc::new(Schema::empty()));
                    drop(schemas);
                    let schema = DFSchema::try_from(schema).ok()?;
                    Some(Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(schema),
                    })))
                }
                _ => None,
            }
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        _logical_plan: &'a LogicalPlan,
        params: &'a ParamValues,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            match statement {
                Statement::Declare { stmts } => {
                    Some(declare(stmts, Some(params), session_context, client).await)
                }
                Statement::Fetch {
                    name,
                    direction,
                    into,
                    ..
                } => Some(fetch(name, direction, into, client).await),
                Statement::Close { cursor } => Some(Ok(close(cursor, client))),
                _ => None,
            }
        })
    }
}

/// Answers queries against `pg_catalog.pg_cursors` from the connection-local
/// cursor store.
///
/// The relation does not exist in the catalog. psycopg2 checks it before it
/// issues `DECLARE`; sqlparser does not have a virtual table hook for it, so
/// this hook recognises the simple probe and the common column projections and
/// answers them directly from the cursor state.
pub(crate) struct PgCursorsHook;

fn references_pg_cursors(statement: &Statement) -> bool {
    postgres_pgcompat_visitors::references_pg_cursors(statement)
}

#[derive(Clone, Copy)]
enum CursorColumn {
    Name,
    Statement,
    IsHoldable,
    IsBinary,
    IsScrollable,
    CreationTime,
}

impl CursorColumn {
    fn field(self) -> Field {
        match self {
            Self::Name => Field::new("name", DataType::Utf8, false),
            Self::Statement => Field::new("statement", DataType::Utf8, false),
            Self::IsHoldable => Field::new("is_holdable", DataType::Boolean, false),
            Self::IsBinary => Field::new("is_binary", DataType::Boolean, false),
            Self::IsScrollable => Field::new("is_scrollable", DataType::Boolean, false),
            Self::CreationTime => Field::new(
                "creation_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        }
    }

    fn array(self, infos: &[CursorInfo]) -> ArrayRef {
        match self {
            Self::Name => Arc::new(StringArray::from(
                infos
                    .iter()
                    .map(|info| info.name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Self::Statement => Arc::new(StringArray::from(
                infos
                    .iter()
                    .map(|info| info.statement.as_str())
                    .collect::<Vec<_>>(),
            )),
            Self::IsHoldable => Arc::new(BooleanArray::from(
                infos
                    .iter()
                    .map(|info| info.is_holdable)
                    .collect::<Vec<_>>(),
            )),
            Self::IsBinary => Arc::new(BooleanArray::from(
                infos.iter().map(|info| info.is_binary).collect::<Vec<_>>(),
            )),
            Self::IsScrollable => Arc::new(BooleanArray::from(
                infos
                    .iter()
                    .map(|info| info.is_scrollable)
                    .collect::<Vec<_>>(),
            )),
            Self::CreationTime => Arc::new(
                TimestampMicrosecondArray::from(
                    infos
                        .iter()
                        .map(|info| info.created_at.timestamp_micros())
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "name" => Some(Self::Name),
            "statement" => Some(Self::Statement),
            "is_holdable" => Some(Self::IsHoldable),
            "is_binary" => Some(Self::IsBinary),
            "is_scrollable" => Some(Self::IsScrollable),
            "creation_time" => Some(Self::CreationTime),
            _ => None,
        }
    }
}

enum CursorProjection {
    All,
    Columns(Vec<CursorColumn>),
    LiteralInt(i32),
    LiteralBool(bool),
}

fn cursor_projection(
    statement: &Statement,
) -> Option<(CursorProjection, Option<String>)> {
    let Statement::Query(query) = statement else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let filter = match &select.selection {
        None => None,
        Some(Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        }) => {
            let column = expr_column_name(left)?;
            if !column.eq_ignore_ascii_case("name") {
                return None;
            }
            let Expr::Value(value) = right.as_ref() else {
                return None;
            };
            let Value::SingleQuotedString(name) = &value.value else {
                return None;
            };
            Some(name.clone())
        }
        Some(_) => return None,
    };
    let projection = if select.projection.len() == 1 {
        match &select.projection[0] {
            SelectItem::Wildcard(_) => CursorProjection::All,
            SelectItem::UnnamedExpr(expr) => projection_expr(expr)?,
            SelectItem::ExprWithAlias { expr, .. } => projection_expr(expr)?,
            _ => return None,
        }
    } else {
        let columns = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) => projection_column(expr),
                SelectItem::ExprWithAlias { expr, .. } => projection_column(expr),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        CursorProjection::Columns(columns)
    };
    Some((projection, filter))
}

fn expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(&ident.value),
        Expr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.as_str()),
        _ => None,
    }
}

fn projection_column(expr: &Expr) -> Option<CursorColumn> {
    CursorColumn::from_name(expr_column_name(expr)?)
}

fn projection_expr(expr: &Expr) -> Option<CursorProjection> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Number(number, _) => {
                Some(CursorProjection::LiteralInt(number.parse().ok()?))
            }
            Value::Boolean(value) => Some(CursorProjection::LiteralBool(*value)),
            _ => None,
        },
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Some(CursorProjection::Columns(vec![projection_column(expr)?]))
        }
        _ => None,
    }
}

fn cursor_fields(projection: &CursorProjection) -> Vec<Field> {
    match projection {
        CursorProjection::All => vec![
            CursorColumn::Name.field(),
            CursorColumn::Statement.field(),
            CursorColumn::IsHoldable.field(),
            CursorColumn::IsBinary.field(),
            CursorColumn::IsScrollable.field(),
            CursorColumn::CreationTime.field(),
        ],
        CursorProjection::Columns(columns) => {
            columns.iter().map(|column| column.field()).collect()
        }
        CursorProjection::LiteralInt(_) => {
            vec![Field::new("?column?", DataType::Int32, false)]
        }
        CursorProjection::LiteralBool(_) => {
            vec![Field::new("?column?", DataType::Boolean, false)]
        }
    }
}

impl PgCursorsHook {
    fn respond(
        &self,
        statement: &Statement,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        let Some((projection, filter)) = cursor_projection(statement) else {
            return Err(PgWireError::ApiError(Box::new(
                datafusion::error::DataFusionError::NotImplemented(
                    "unsupported pg_cursors projection".to_string(),
                ),
            )));
        };
        let infos = store(client)
            .snapshot()
            .into_iter()
            .filter(|info| filter.as_deref().is_none_or(|name| info.name == name))
            .collect::<Vec<_>>();
        let (schema, arrays) = match projection {
            CursorProjection::All => {
                let columns = [
                    CursorColumn::Name,
                    CursorColumn::Statement,
                    CursorColumn::IsHoldable,
                    CursorColumn::IsBinary,
                    CursorColumn::IsScrollable,
                    CursorColumn::CreationTime,
                ];
                (
                    Schema::new(
                        columns
                            .iter()
                            .map(|column| column.field())
                            .collect::<Vec<_>>(),
                    ),
                    columns
                        .iter()
                        .map(|column| column.array(&infos))
                        .collect::<Vec<_>>(),
                )
            }
            CursorProjection::Columns(columns) => (
                Schema::new(
                    columns
                        .iter()
                        .map(|column| column.field())
                        .collect::<Vec<_>>(),
                ),
                columns
                    .iter()
                    .map(|column| column.array(&infos))
                    .collect::<Vec<_>>(),
            ),
            CursorProjection::LiteralInt(value) => (
                Schema::new(vec![Field::new("?column?", DataType::Int32, false)]),
                vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    value;
                    infos.len()
                ])) as ArrayRef],
            ),
            CursorProjection::LiteralBool(value) => (
                Schema::new(vec![Field::new("?column?", DataType::Boolean, false)]),
                vec![Arc::new(BooleanArray::from(vec![value; infos.len()])) as ArrayRef],
            ),
        };
        cursor_response(schema, arrays)
    }
}

fn cursor_response(schema: Schema, arrays: Vec<ArrayRef>) -> PgWireResult<Response> {
    let schema = Arc::new(schema);
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    let fields = Arc::new(arrow_schema_to_pg_fields(
        &schema,
        &Format::UnifiedText,
        None,
    )?);
    let rows = encode_recordbatch(Arc::clone(&fields), batch).collect::<Vec<_>>();
    let mut response = QueryResponse::new(fields, stream::iter(rows));
    response.set_command_tag("SELECT");
    Ok(Response::Query(response))
}

impl QueryHook for PgCursorsHook {
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        _session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            if !references_pg_cursors(statement) {
                return None;
            }
            Some(self.respond(statement, client))
        })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        statement: &'a Statement,
        _session_context: &'a SessionContext,
        _client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<LogicalPlan>>> {
        Box::pin(async move {
            if !references_pg_cursors(statement) {
                return None;
            }
            let (projection, _) = cursor_projection(statement)?;
            let schema =
                DFSchema::try_from(Schema::new(cursor_fields(&projection))).ok()?;
            Some(Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(schema),
            })))
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        _logical_plan: &'a LogicalPlan,
        _params: &'a ParamValues,
        _session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            if !references_pg_cursors(statement) {
                return None;
            }
            Some(self.respond(statement, client))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_limits_cover_forward_directions() {
        assert_eq!(fetch_limit(&FetchDirection::Next).expect("next"), Some(1));
        assert_eq!(
            fetch_limit(&FetchDirection::Forward { limit: None }).expect("forward"),
            Some(1)
        );
        assert_eq!(fetch_limit(&FetchDirection::All).expect("all"), None);
        assert_eq!(
            fetch_limit(&FetchDirection::ForwardAll).expect("forward all"),
            None
        );
    }

    #[test]
    fn backward_fetch_is_refused() {
        let error = fetch_limit(&FetchDirection::Prior).expect_err("backward");
        match error {
            PgWireError::UserError(info) => assert_eq!(info.code, CURSOR_ONLY_FORWARD),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn parse(sql: &str) -> Statement {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

        PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse pg_cursors query")
            .remove(0)
    }

    #[test]
    fn recognizes_psycopg2_cursor_probe() {
        let statement =
            parse("SELECT 1 FROM pg_catalog.pg_cursors WHERE name = 'probe_cur'");
        let (projection, filter) = cursor_projection(&statement).expect("cursor probe");
        assert!(matches!(projection, CursorProjection::LiteralInt(1)));
        assert_eq!(filter.as_deref(), Some("probe_cur"));
    }

    #[test]
    fn recognizes_cursor_columns_and_star() {
        let statement = parse("SELECT name, is_holdable FROM pg_catalog.pg_cursors");
        let (projection, filter) = cursor_projection(&statement).expect("cursor columns");
        assert!(matches!(projection, CursorProjection::Columns(_)));
        assert!(filter.is_none());

        let statement = parse("SELECT * FROM pg_catalog.pg_cursors");
        let (projection, _) = cursor_projection(&statement).expect("cursor star");
        assert!(matches!(projection, CursorProjection::All));
    }
}
