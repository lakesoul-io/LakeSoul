// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `COPY ... FROM STDIN` for the PostgreSQL front end.
//!
//! PostgreSQL loads bulk data through the copy sub-protocol: the server
//! answers the statement with `CopyInResponse`, the client streams `CopyData`
//! frames, and `CopyDone` ends the statement with a `COPY <rows>` tag. The
//! rows are written as one LakeSoul commit, so a `COPY` is atomic exactly like
//! a multi-row `INSERT`; inside a transaction block the write is staged and
//! published by `COMMIT`, like every other write.
//!
//! Two formats are implemented: `text` (the default) and `csv`. Binary
//! `COPY` is refused with `0A000`, as is `COPY ... FROM` a file or program:
//! this endpoint only speaks stdio. The export direction lives in
//! [`crate::copy_out`] and shares [`CopyOptions`] with this module.
//!
//! The statement is claimed by [`CopyStatementHook`] before DataFusion sees
//! it. The hook resolves the target table and column list, records the copy
//! state on the connection, and returns the `CopyInResponse`; [`CopyHandler`]
//! then decodes the incoming frames. The write itself is
//! [`crate::insert::execute_copy_write`], which plans the same `Dml` node an
//! `INSERT` over the decoded rows would.
//!
//! The whole copy is buffered in memory until `CopyDone`, because one
//! statement must become one commit. Large loads should be split by the
//! client into several `COPY` statements.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Ident,
    ObjectName, Statement, TableObject,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::copy::CopyHandler;
use datafusion_postgres::pgwire::api::results::{CopyResponse, Response, Tag};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use futures::{Sink, SinkExt, stream};
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use parking_lot::Mutex;

use crate::cancel::statement_timeout;
use crate::ddl::{feature_not_supported, normalize_column_ident};
use crate::insert::{check_write_target, request_context, target_reference, user_error};
use crate::limits::StatementOutcome;
use crate::server::{LakeSoulQueryRouter, connection_session, run_under_limit};

/// `22P02 invalid_text_representation` for a column value the target type
/// cannot parse.
const INVALID_TEXT: &str = "22P02";
/// `22P04 bad_copy_file_format` for a malformed COPY stream or row.
const BAD_COPY_FORMAT: &str = "22P04";
/// `22007 invalid_datetime_format` for a date/time value that does not parse.
const INVALID_DATETIME: &str = "22007";
/// `22003 numeric_value_out_of_range` for a numeric that does not fit.
const NUMERIC_RANGE: &str = "22003";
/// `42601 syntax_error` for options PostgreSQL itself rejects as invalid.
const SYNTAX_ERROR: &str = "42601";
/// `42701 duplicate_column` for a repeated target column.
const DUPLICATE_COLUMN: &str = "42701";
/// `42703 undefined_column` for a column the target table does not have.
const UNDEFINED_COLUMN: &str = "42703";
/// `42P01 undefined_table` for a relation the catalog does not hold.
const UNDEFINED_TABLE: &str = "42P01";
/// `22023 invalid_parameter_value` for an impossible delimiter, quote, etc.
const INVALID_PARAMETER: &str = "22023";
/// `57014 query_canceled`, PostgreSQL's SQLSTATE for a client `CopyFail`.
const QUERY_CANCELED: &str = "57014";

/// The two text encodings the copy sub-protocol supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CopyFormat {
    Text,
    Csv,
}

/// The options one `COPY` statement carries, already resolved against the
/// format defaults.
#[derive(Clone, Debug)]
pub(crate) struct CopyOptions {
    pub(crate) format: CopyFormat,
    /// One-byte field separator.
    pub(crate) delimiter: u8,
    /// Bytes that represent NULL in an unquoted field.
    pub(crate) null: Vec<u8>,
    /// CSV quote character; unused by text format.
    pub(crate) quote: u8,
    /// CSV escape character; unused by text format.
    pub(crate) escape: u8,
    /// Skip the first record (CSV only).
    pub(crate) header: bool,
}

/// A target column of the copy, in the order the client sends it.
#[derive(Clone, Debug)]
struct CopyColumn {
    name: String,
    data_type: DataType,
}

/// A `COPY ... FROM STDIN` statement, validated down to the target columns.
struct CopyRequest {
    table: ObjectName,
    /// Target columns in COPY order; empty means every table column.
    columns: Vec<Ident>,
    options: CopyOptions,
}

/// The connection-local state of one copy-in.
struct CopyInState {
    table: ObjectName,
    /// Resolved target column names (empty means the whole table).
    target_columns: Vec<String>,
    columns: Vec<CopyColumn>,
    schema: SchemaRef,
    decoder: RowDecoder,
    rows: Vec<Vec<ScalarValue>>,
    /// The CSV header record is skipped before the first data row.
    header_pending: bool,
}

impl CopyInState {
    fn new(
        table: ObjectName,
        target_columns: Vec<String>,
        columns: Vec<CopyColumn>,
        options: &CopyOptions,
    ) -> Self {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| Field::new(&column.name, column.data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));
        Self {
            table,
            target_columns,
            columns,
            schema,
            decoder: RowDecoder::new(options),
            rows: Vec::new(),
            header_pending: options.header,
        }
    }

    /// Decodes every complete row in `chunk`.
    fn push(&mut self, chunk: &[u8]) -> PgWireResult<()> {
        let rows = self.decoder.push(chunk)?;
        self.record(rows)
    }

    /// Flushes the trailing partial row (if any) and returns the write
    /// batches. An empty copy produces no batch, so it never commits.
    fn finish(&mut self) -> PgWireResult<Vec<RecordBatch>> {
        let rows = self.decoder.finish()?;
        self.record(rows)?;
        if self.rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut arrays = Vec::with_capacity(self.schema.fields().len());
        for (index, field) in self.schema.fields().iter().enumerate() {
            let values = self.rows.iter().map(|row| row[index].clone());
            let array: ArrayRef =
                ScalarValue::iter_to_array(values).map_err(|error| {
                    internal(format!(
                        "COPY column \"{}\" cannot be built: {error}",
                        field.name()
                    ))
                })?;
            arrays.push(array);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|error| internal(format!("COPY batch cannot be built: {error}")))?;
        Ok(vec![batch])
    }

    fn record(&mut self, rows: Vec<Vec<Option<Vec<u8>>>>) -> PgWireResult<()> {
        for raw in rows {
            if self.header_pending {
                self.header_pending = false;
                continue;
            }
            check_arity(&raw, &self.columns)?;
            let mut values = Vec::with_capacity(self.columns.len());
            for (column, field) in self.columns.iter().zip(raw.iter()) {
                values.push(decode_value(column, field.as_deref())?);
            }
            self.rows.push(values);
        }
        Ok(())
    }
}

/// The copy state of one connection.
#[derive(Default)]
pub(crate) struct CopyInCell(Mutex<Option<CopyInState>>);

/// Claims `COPY ... FROM STDIN` in both protocols.
pub(crate) struct CopyStatementHook;

#[async_trait]
impl QueryHook for CopyStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if !is_copy(statement) {
            return None;
        }
        if is_copy_to(statement) {
            return Some(
                crate::copy_out::copy_out(statement, session_context, client, None).await,
            );
        }
        Some(begin_copy(statement, session_context, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if !is_copy(statement) {
            return None;
        }
        // Validate the statement at Parse time, the way PostgreSQL does, and
        // hand the planner a plan without columns: a prepared COPY has no
        // result to describe. The copy state is created at Execute, because
        // only then does the server start the copy sub-protocol.
        let copy_to = is_copy_to(statement);
        Some(
            async {
                if copy_to {
                    crate::copy_out::validate(statement, session_context, client).await?;
                } else {
                    plan_copy(statement, session_context, client).await?;
                }
                Ok(no_data_plan())
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if !is_copy(statement) {
            return None;
        }
        if is_copy_to(statement) {
            return Some(
                crate::copy_out::copy_out(
                    statement,
                    session_context,
                    client,
                    Some(_params),
                )
                .await,
            );
        }
        Some(begin_copy(statement, session_context, client).await)
    }
}

fn is_copy(statement: &Statement) -> bool {
    matches!(statement, Statement::Copy { .. })
}

fn is_copy_to(statement: &Statement) -> bool {
    matches!(statement, Statement::Copy { to: true, .. })
}

fn no_data_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

/// Validate the copy and record its state on the connection.
async fn begin_copy<C>(
    statement: &Statement,
    session_context: &SessionContext,
    client: &C,
) -> PgWireResult<Response>
where
    C: ClientInfo + ?Sized,
{
    let request = parse_copy(statement)?;
    let state = plan_copy_state(request, session_context, client).await?;
    let columns = state.columns.len();
    let cell = client
        .session_extensions()
        .get_or_insert_with(CopyInCell::default);
    *cell.0.lock() = Some(state);
    Ok(Response::CopyIn(CopyResponse::new(
        0,
        columns,
        stream::empty(),
    )))
}

/// Validate a copy without starting it; this is what extended `Parse` runs.
async fn plan_copy<C>(
    statement: &Statement,
    session_context: &SessionContext,
    client: &C,
) -> PgWireResult<()>
where
    C: ClientInfo + ?Sized,
{
    let request = parse_copy(statement)?;
    plan_copy_state(request, session_context, client).await?;
    Ok(())
}

async fn plan_copy_state<C>(
    request: CopyRequest,
    session_context: &SessionContext,
    client: &C,
) -> PgWireResult<CopyInState>
where
    C: ClientInfo + ?Sized,
{
    let (status, database) = request_context(client)?;
    let (table_ref, table_name) =
        target_reference(&TableObject::TableName(request.table.clone()), &database)?;
    let provider = session_context
        .table_provider(table_ref)
        .await
        .map_err(|error| {
            user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table_name}\" does not exist: {error}"),
            )
        })?;
    if provider
        .as_ref()
        .downcast_ref::<LakeSoulTableProvider>()
        .is_some()
    {
        crate::acl::require_table_write(session_context, &database, &table_name).await?;
    }
    // A copy is a write of the same kinds `INSERT` supports; a block stages a
    // LakeSoul write and refuses a foreign/external one.
    check_write_target(
        provider.as_ref(),
        status,
        session_context,
        "COPY",
        &table_name,
    )?;
    let columns = resolve_columns(&provider.schema(), &request.columns, &table_name)?;
    let options = &request.options;
    let target_columns = if request.columns.is_empty() {
        Vec::new()
    } else {
        columns.iter().map(|column| column.name.clone()).collect()
    };
    Ok(CopyInState::new(
        request.table,
        target_columns,
        columns,
        options,
    ))
}

/// Resolves the COPY column list against the target schema.
fn resolve_columns(
    schema: &SchemaRef,
    requested: &[Ident],
    table: &str,
) -> PgWireResult<Vec<CopyColumn>> {
    let field = |index: usize| CopyColumn {
        name: schema.field(index).name().clone(),
        data_type: schema.field(index).data_type().clone(),
    };
    if requested.is_empty() {
        return Ok((0..schema.fields().len()).map(field).collect());
    }
    let table_schema = DFSchema::try_from(schema.clone()).map_err(|error| {
        internal(format!("cannot read the schema of \"{table}\": {error}"))
    })?;
    let mut seen = std::collections::BTreeSet::new();
    let mut columns: Vec<CopyColumn> = Vec::with_capacity(requested.len());
    for ident in requested {
        let name = normalize_column_ident(ident)?;
        let index = table_schema
            .index_of_column_by_name(None, &name)
            .ok_or_else(|| {
                user_error(
                    UNDEFINED_COLUMN,
                    format!("column \"{name}\" of relation \"{table}\" does not exist"),
                )
            })?;
        if !seen.insert(index) {
            return Err(user_error(
                DUPLICATE_COLUMN,
                format!(
                    "column \"{}\" specified more than once",
                    schema.field(index).name()
                ),
            ));
        }
        columns.push(field(index));
    }
    Ok(columns)
}

/// Parses and validates the statement's shape and options.
fn parse_copy(statement: &Statement) -> PgWireResult<CopyRequest> {
    let Statement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        values,
    } = statement
    else {
        return Err(internal(
            "COPY hook received a non-COPY statement".to_string(),
        ));
    };
    if *to {
        return Err(feature_not_supported(
            "COPY TO is not supported; only COPY ... FROM STDIN is implemented"
                .to_string(),
        ));
    }
    if !values.is_empty() {
        return Err(feature_not_supported(
            "COPY data must be sent through the copy sub-protocol, not in the \
             statement text"
                .to_string(),
        ));
    }
    match target {
        CopyTarget::Stdin => {}
        CopyTarget::Stdout => {
            return Err(feature_not_supported(
                "COPY TO STDOUT is not supported".to_string(),
            ));
        }
        CopyTarget::File { .. } | CopyTarget::Program { .. } => {
            return Err(feature_not_supported(
                "COPY from a file or program is not supported; use COPY ... \
                 FROM STDIN"
                    .to_string(),
            ));
        }
    }
    let (table, columns) = match source {
        CopySource::Table {
            table_name,
            columns,
        } => (table_name.clone(), columns.clone()),
        CopySource::Query(_) => {
            return Err(feature_not_supported(
                "COPY (query) is not supported; only COPY ... FROM STDIN of a \
                 table"
                    .to_string(),
            ));
        }
    };
    Ok(CopyRequest {
        table,
        columns,
        options: parse_options(options, legacy_options)?,
    })
}

/// Merges modern and pre-9.0 `WITH` options into one resolved option set.
pub(crate) fn parse_options(
    options: &[CopyOption],
    legacy: &[CopyLegacyOption],
) -> PgWireResult<CopyOptions> {
    let mut format = None;
    for option in options {
        if let CopyOption::Format(ident) = option {
            set_format(&mut format, ident)?;
        }
    }
    for option in legacy {
        match option {
            CopyLegacyOption::Binary => {
                return Err(feature_not_supported(
                    "COPY ... FROM STDIN BINARY is not supported".to_string(),
                ));
            }
            CopyLegacyOption::Csv(_) => set_format(&mut format, &Ident::new("csv"))?,
            _ => {}
        }
    }
    let format = format.unwrap_or(CopyFormat::Text);
    let mut resolved = CopyOptions {
        format,
        delimiter: match format {
            CopyFormat::Text => b'\t',
            CopyFormat::Csv => b',',
        },
        null: match format {
            CopyFormat::Text => b"\\N".to_vec(),
            CopyFormat::Csv => Vec::new(),
        },
        quote: b'"',
        escape: b'"',
        header: false,
    };
    for option in options {
        match option {
            CopyOption::Format(_) => {}
            CopyOption::Delimiter(delimiter) => {
                resolved.delimiter = option_byte("DELIMITER", *delimiter)?;
            }
            CopyOption::Null(null) => resolved.null = null.clone().into_bytes(),
            CopyOption::Header(header) => resolved.header = *header,
            CopyOption::Quote(quote) => {
                if resolved.format != CopyFormat::Csv {
                    return Err(user_error(
                        SYNTAX_ERROR,
                        "cannot specify QUOTE in non-CSV mode".to_string(),
                    ));
                }
                resolved.quote = option_byte("QUOTE", *quote)?;
            }
            CopyOption::Escape(escape) => {
                if resolved.format != CopyFormat::Csv {
                    return Err(user_error(
                        SYNTAX_ERROR,
                        "cannot specify ESCAPE in non-CSV mode".to_string(),
                    ));
                }
                resolved.escape = option_byte("ESCAPE", *escape)?;
            }
            CopyOption::Encoding(encoding) => check_encoding(encoding)?,
            CopyOption::Freeze(_) => {
                return Err(feature_not_supported(
                    "COPY ... FREEZE is not supported".to_string(),
                ));
            }
            CopyOption::ForceQuote(_)
            | CopyOption::ForceNotNull(_)
            | CopyOption::ForceNull(_) => {
                return Err(feature_not_supported(format!(
                    "COPY option {option} is not supported"
                )));
            }
        }
    }
    for option in legacy {
        match option {
            CopyLegacyOption::Binary => {}
            CopyLegacyOption::Delimiter(delimiter) => {
                resolved.delimiter = option_byte("DELIMITER", *delimiter)?;
            }
            CopyLegacyOption::Null(null) => resolved.null = null.clone().into_bytes(),
            CopyLegacyOption::Header => resolved.header = true,
            CopyLegacyOption::Csv(csv) => {
                for option in csv {
                    match option {
                        CopyLegacyCsvOption::Header => resolved.header = true,
                        CopyLegacyCsvOption::Quote(quote) => {
                            resolved.quote = option_byte("QUOTE", *quote)?;
                        }
                        CopyLegacyCsvOption::Escape(escape) => {
                            resolved.escape = option_byte("ESCAPE", *escape)?;
                        }
                        CopyLegacyCsvOption::ForceQuote(_)
                        | CopyLegacyCsvOption::ForceNotNull(_) => {
                            return Err(feature_not_supported(format!(
                                "COPY option {option} is not supported"
                            )));
                        }
                    }
                }
            }
            other => {
                return Err(feature_not_supported(format!(
                    "COPY option {other} is not supported"
                )));
            }
        }
    }
    validate_options(&resolved)?;
    Ok(resolved)
}

fn set_format(format: &mut Option<CopyFormat>, ident: &Ident) -> PgWireResult<()> {
    let detected = match ident.value.to_ascii_lowercase().as_str() {
        "text" => CopyFormat::Text,
        "csv" => CopyFormat::Csv,
        "binary" => {
            return Err(feature_not_supported(
                "COPY ... FROM STDIN with FORMAT binary is not supported".to_string(),
            ));
        }
        other => {
            return Err(user_error(
                SYNTAX_ERROR,
                format!("COPY format \"{other}\" not recognized"),
            ));
        }
    };
    if format.replace(detected).is_some() {
        return Err(user_error(
            SYNTAX_ERROR,
            "conflicting or redundant options".to_string(),
        ));
    }
    Ok(())
}

/// PostgreSQL delimiters and quote characters are one byte.
fn option_byte(option: &str, value: char) -> PgWireResult<u8> {
    let mut encoded = [0u8; 4];
    let bytes = value.encode_utf8(&mut encoded).as_bytes();
    let [byte] = bytes else {
        return Err(feature_not_supported(format!(
            "COPY {option} must be a single one-byte character"
        )));
    };
    Ok(*byte)
}

fn check_encoding(encoding: &str) -> PgWireResult<()> {
    let normalized = encoding.to_ascii_lowercase().replace('-', "");
    if normalized == "utf8" || normalized == "unicode" {
        return Ok(());
    }
    Err(feature_not_supported(format!(
        "COPY ENCODING '{encoding}' is not supported: the server encoding is \
         UTF8"
    )))
}

fn validate_options(options: &CopyOptions) -> PgWireResult<()> {
    let delimiter = options.delimiter;
    if delimiter == b'\n' || delimiter == b'\r' {
        return Err(user_error(
            INVALID_PARAMETER,
            "COPY delimiter cannot be newline or carriage return".to_string(),
        ));
    }
    match options.format {
        CopyFormat::Text => {
            if options.header {
                return Err(user_error(
                    SYNTAX_ERROR,
                    "cannot specify HEADER in non-CSV mode".to_string(),
                ));
            }
        }
        CopyFormat::Csv => {
            for (name, byte) in [("QUOTE", options.quote), ("ESCAPE", options.escape)] {
                if byte == b'\n' || byte == b'\r' {
                    return Err(user_error(
                        INVALID_PARAMETER,
                        format!("COPY {name} cannot be newline or carriage return"),
                    ));
                }
                if byte == delimiter {
                    return Err(user_error(
                        INVALID_PARAMETER,
                        format!("COPY {name} cannot be the delimiter"),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Streaming decoder producing complete rows of raw field bytes.
enum RowDecoder {
    Text(TextDecoder),
    Csv(CsvDecoder),
}

impl RowDecoder {
    fn new(options: &CopyOptions) -> Self {
        match options.format {
            CopyFormat::Text => RowDecoder::Text(TextDecoder {
                delimiter: options.delimiter,
                null: options.null.clone(),
                buffer: Vec::new(),
                ended: false,
            }),
            CopyFormat::Csv => RowDecoder::Csv(CsvDecoder::new(options)),
        }
    }

    fn push(&mut self, chunk: &[u8]) -> PgWireResult<Vec<Vec<Option<Vec<u8>>>>> {
        match self {
            RowDecoder::Text(decoder) => decoder.push(chunk),
            RowDecoder::Csv(decoder) => decoder.push(chunk),
        }
    }

    fn finish(&mut self) -> PgWireResult<Vec<Vec<Option<Vec<u8>>>>> {
        match self {
            RowDecoder::Text(decoder) => decoder.finish(),
            RowDecoder::Csv(decoder) => decoder.finish(),
        }
    }
}

/// `COPY ... FROM STDIN` TEXT format.
struct TextDecoder {
    delimiter: u8,
    null: Vec<u8>,
    buffer: Vec<u8>,
    ended: bool,
}

impl TextDecoder {
    fn push(&mut self, chunk: &[u8]) -> PgWireResult<Vec<Vec<Option<Vec<u8>>>>> {
        let mut rows = Vec::new();
        for &byte in chunk {
            if self.ended {
                return Err(user_error(
                    BAD_COPY_FORMAT,
                    "received COPY data after the end-of-data marker".to_string(),
                ));
            }
            if byte == b'\n' {
                let mut line = std::mem::take(&mut self.buffer);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line == b"\\." {
                    self.ended = true;
                    continue;
                }
                rows.push(self.split_line(&line));
            } else {
                self.buffer.push(byte);
            }
        }
        Ok(rows)
    }

    fn finish(&mut self) -> PgWireResult<Vec<Vec<Option<Vec<u8>>>>> {
        if self.ended || self.buffer.is_empty() {
            return Ok(Vec::new());
        }
        let mut line = std::mem::take(&mut self.buffer);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line == b"\\." {
            return Ok(Vec::new());
        }
        Ok(vec![self.split_line(&line)])
    }

    /// Split one line on unescaped delimiters; a backslash protects the byte
    /// that follows it, so `\,` is data for a comma delimiter.
    fn split_line(&self, line: &[u8]) -> Vec<Option<Vec<u8>>> {
        let mut fields = Vec::new();
        let mut start = 0;
        let mut index = 0;
        while index < line.len() {
            match line[index] {
                b'\\' => index += 2,
                byte if byte == self.delimiter => {
                    fields.push(self.field(&line[start..index]));
                    start = index + 1;
                    index += 1;
                }
                _ => index += 1,
            }
        }
        fields.push(self.field(&line[start..]));
        fields
    }

    fn field(&self, raw: &[u8]) -> Option<Vec<u8>> {
        if raw == self.null.as_slice() {
            None
        } else {
            Some(unescape(raw))
        }
    }
}

/// Reverse PostgreSQL's COPY TEXT escaping.
fn unescape(raw: &[u8]) -> Vec<u8> {
    if !raw.contains(&b'\\') {
        return raw.to_vec();
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        let byte = raw[index];
        index += 1;
        if byte != b'\\' {
            out.push(byte);
            continue;
        }
        let Some(&escaped) = raw.get(index) else {
            out.push(b'\\');
            break;
        };
        index += 1;
        match escaped {
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'\\' => out.push(b'\\'),
            b'x' => {
                let mut value = 0u8;
                let mut digits = 0;
                while digits < 2 {
                    let Some(&next) = raw.get(index) else {
                        break;
                    };
                    let Some(nibble) = hex_value(next) else {
                        break;
                    };
                    value = value.wrapping_mul(16).wrapping_add(nibble);
                    index += 1;
                    digits += 1;
                }
                if digits == 0 {
                    out.push(b'x');
                } else {
                    out.push(value);
                }
            }
            b'0'..=b'7' => {
                let mut value = u16::from(escaped - b'0');
                let mut digits = 1;
                while digits < 3 {
                    let Some(&next) = raw.get(index) else {
                        break;
                    };
                    if !(b'0'..=b'7').contains(&next) {
                        break;
                    }
                    value = value * 8 + u16::from(next - b'0');
                    index += 1;
                    digits += 1;
                }
                out.push((value & 0xff) as u8);
            }
            other => out.push(other),
        }
    }
    out
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CsvState {
    /// Start of a field: a quote here opens a quoted field.
    FieldStart,
    Unquoted,
    Quoted,
    /// A quote inside a quoted field: another quote is a literal, anything
    /// else closes the field.
    QuoteInQuoted,
    /// The byte after the CSV escape character.
    Escaped,
    /// A carriage return was seen; the next byte must be a newline.
    AfterCarriageReturn,
}

/// `COPY ... FROM STDIN` CSV format.
struct CsvDecoder {
    delimiter: u8,
    quote: u8,
    escape: u8,
    null: Vec<u8>,
    state: CsvState,
    field: Vec<u8>,
    /// Whether the current field was quoted; a quoted empty field is `''`,
    /// never NULL.
    quoted: bool,
    fields: Vec<Option<Vec<u8>>>,
    started: bool,
}

impl CsvDecoder {
    fn new(options: &CopyOptions) -> Self {
        Self {
            delimiter: options.delimiter,
            quote: options.quote,
            escape: options.escape,
            null: options.null.clone(),
            state: CsvState::FieldStart,
            field: Vec::new(),
            quoted: false,
            fields: Vec::new(),
            started: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> PgWireResult<Vec<Vec<Option<Vec<u8>>>>> {
        let mut rows = Vec::new();
        for &byte in chunk {
            self.byte(byte, &mut rows)?;
        }
        Ok(rows)
    }

    fn byte(
        &mut self,
        byte: u8,
        rows: &mut Vec<Vec<Option<Vec<u8>>>>,
    ) -> PgWireResult<()> {
        match self.state {
            CsvState::FieldStart => {
                self.started = true;
                if byte == self.quote {
                    self.quoted = true;
                    self.state = CsvState::Quoted;
                } else if byte == self.delimiter {
                    self.end_field();
                } else if byte == b'\n' {
                    self.end_row(rows);
                } else if byte == b'\r' {
                    self.state = CsvState::AfterCarriageReturn;
                } else {
                    self.field.push(byte);
                    self.state = CsvState::Unquoted;
                }
            }
            CsvState::Unquoted => {
                if byte == self.delimiter {
                    self.end_field();
                } else if byte == b'\n' {
                    self.end_row(rows);
                } else if byte == b'\r' {
                    self.state = CsvState::AfterCarriageReturn;
                } else {
                    self.field.push(byte);
                }
            }
            CsvState::Quoted => {
                if byte == self.quote {
                    self.state = CsvState::QuoteInQuoted;
                } else if self.escape != self.quote && byte == self.escape {
                    self.state = CsvState::Escaped;
                } else {
                    self.field.push(byte);
                }
            }
            CsvState::QuoteInQuoted => {
                if byte == self.quote {
                    self.field.push(self.quote);
                    self.state = CsvState::Quoted;
                } else if byte == self.delimiter {
                    self.end_field();
                } else if byte == b'\n' {
                    self.end_row(rows);
                } else if byte == b'\r' {
                    self.state = CsvState::AfterCarriageReturn;
                } else {
                    return Err(user_error(
                        BAD_COPY_FORMAT,
                        "unquoted data after a quoted CSV field".to_string(),
                    ));
                }
            }
            CsvState::Escaped => {
                self.field.push(byte);
                self.state = CsvState::Quoted;
            }
            CsvState::AfterCarriageReturn => {
                if byte == b'\n' {
                    self.end_row(rows);
                } else {
                    return Err(user_error(
                        BAD_COPY_FORMAT,
                        "carriage return without a following newline in a CSV \
                         record"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn end_field(&mut self) {
        let value = std::mem::take(&mut self.field);
        let value = if !self.quoted && value == self.null {
            None
        } else {
            Some(value)
        };
        self.fields.push(value);
        self.quoted = false;
        self.state = CsvState::FieldStart;
    }

    fn end_row(&mut self, rows: &mut Vec<Vec<Option<Vec<u8>>>>) {
        self.end_field();
        rows.push(std::mem::take(&mut self.fields));
        self.started = false;
        self.state = CsvState::FieldStart;
    }

    fn finish(&mut self) -> PgWireResult<Vec<Vec<Option<Vec<u8>>>>> {
        let mut rows = Vec::new();
        match self.state {
            CsvState::Quoted | CsvState::Escaped => {
                return Err(user_error(
                    BAD_COPY_FORMAT,
                    "unterminated quoted CSV field".to_string(),
                ));
            }
            CsvState::AfterCarriageReturn => {
                self.end_row(&mut rows);
            }
            CsvState::FieldStart | CsvState::Unquoted | CsvState::QuoteInQuoted => {
                if self.started || !self.field.is_empty() || !self.fields.is_empty() {
                    self.end_row(&mut rows);
                }
            }
        }
        Ok(rows)
    }
}

fn check_arity(fields: &[Option<Vec<u8>>], columns: &[CopyColumn]) -> PgWireResult<()> {
    if fields.len() < columns.len() {
        let missing = &columns[fields.len()].name;
        return Err(user_error(
            BAD_COPY_FORMAT,
            format!("missing data for column \"{missing}\""),
        ));
    }
    if fields.len() > columns.len() {
        return Err(user_error(
            BAD_COPY_FORMAT,
            "extra data after last expected column".to_string(),
        ));
    }
    Ok(())
}

/// Decode one field into the target column's `ScalarValue`.
fn decode_value(column: &CopyColumn, value: Option<&[u8]>) -> PgWireResult<ScalarValue> {
    let Some(bytes) = value else {
        return ScalarValue::try_from(&column.data_type).map_err(|error| {
            feature_not_supported(format!(
                "COPY into column \"{}\" of type {} is not supported: {error}",
                column.name, column.data_type
            ))
        });
    };
    let scalar = match &column.data_type {
        DataType::Boolean => {
            let value = parse_bool(bytes)
                .ok_or_else(|| invalid_input("boolean", &column.name, bytes))?;
            ScalarValue::Boolean(Some(value))
        }
        DataType::Int16 => {
            ScalarValue::Int16(Some(parse_number(bytes, &column.name, "smallint")?))
        }
        DataType::Int32 => {
            ScalarValue::Int32(Some(parse_number(bytes, &column.name, "integer")?))
        }
        DataType::Int64 => {
            ScalarValue::Int64(Some(parse_number(bytes, &column.name, "bigint")?))
        }
        DataType::UInt8 => {
            ScalarValue::UInt8(Some(parse_number(bytes, &column.name, "smallint")?))
        }
        DataType::UInt16 => {
            ScalarValue::UInt16(Some(parse_number(bytes, &column.name, "integer")?))
        }
        DataType::UInt32 => {
            ScalarValue::UInt32(Some(parse_number(bytes, &column.name, "bigint")?))
        }
        DataType::UInt64 => {
            ScalarValue::UInt64(Some(parse_number(bytes, &column.name, "bigint")?))
        }
        DataType::Float32 => {
            ScalarValue::Float32(Some(parse_float(bytes, &column.name, "real")?))
        }
        DataType::Float64 => ScalarValue::Float64(Some(parse_float(
            bytes,
            &column.name,
            "double precision",
        )?)),
        DataType::Utf8 => ScalarValue::Utf8(Some(parse_utf8(bytes, &column.name)?)),
        DataType::LargeUtf8 => {
            ScalarValue::LargeUtf8(Some(parse_utf8(bytes, &column.name)?))
        }
        DataType::Utf8View => {
            ScalarValue::Utf8View(Some(parse_utf8(bytes, &column.name)?))
        }
        DataType::Binary => ScalarValue::Binary(Some(parse_bytea(bytes, &column.name)?)),
        DataType::LargeBinary => {
            ScalarValue::LargeBinary(Some(parse_bytea(bytes, &column.name)?))
        }
        DataType::BinaryView => {
            ScalarValue::BinaryView(Some(parse_bytea(bytes, &column.name)?))
        }
        DataType::Date32 => ScalarValue::Date32(Some(parse_date(bytes, &column.name)?)),
        DataType::Date64 => {
            let days = i64::from(parse_date(bytes, &column.name)?);
            let millis = days.checked_mul(86_400_000).ok_or_else(|| {
                user_error(
                    NUMERIC_RANGE,
                    format!("date for column \"{}\" is out of range", column.name),
                )
            })?;
            ScalarValue::Date64(Some(millis))
        }
        DataType::Timestamp(unit, time_zone) => {
            let micros = parse_timestamp(bytes, &column.name)?;
            let out_of_range = || {
                user_error(
                    NUMERIC_RANGE,
                    format!("timestamp for column \"{}\" is out of range", column.name),
                )
            };
            match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(
                    Some(micros / 1_000_000),
                    time_zone.clone(),
                ),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(
                    Some(micros / 1_000),
                    time_zone.clone(),
                ),
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(micros), time_zone.clone())
                }
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(
                    Some(micros.checked_mul(1_000).ok_or_else(out_of_range)?),
                    time_zone.clone(),
                ),
            }
        }
        DataType::Decimal128(precision, scale) => {
            let value = parse_decimal(bytes, *precision, *scale, &column.name)?;
            ScalarValue::Decimal128(Some(value), *precision, *scale)
        }
        other => {
            return Err(feature_not_supported(format!(
                "COPY into column \"{}\" of type {other} is not supported",
                column.name
            )));
        }
    };
    if scalar.data_type() == column.data_type {
        Ok(scalar)
    } else {
        scalar.cast_to(&column.data_type).map_err(|error| {
            internal(format!(
                "COPY value for column \"{}\" cannot be converted to {}: {error}",
                column.name, column.data_type
            ))
        })
    }
}

fn parse_utf8(bytes: &[u8], column: &str) -> PgWireResult<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        user_error(
            "22021",
            format!("invalid byte sequence for encoding UTF8 in column \"{column}\""),
        )
    })
}

fn parse_bool(bytes: &[u8]) -> Option<bool> {
    let value = std::str::from_utf8(bytes).ok()?.to_ascii_lowercase();
    match value.as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

fn parse_number<T>(bytes: &[u8], column: &str, ty: &str) -> PgWireResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let text = trimmed(bytes);
    text.parse::<T>()
        .map_err(|_| invalid_input(ty, column, bytes))
}

fn parse_float<T>(bytes: &[u8], column: &str, ty: &str) -> PgWireResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let text = trimmed(bytes);
    let normalized = match text {
        "Infinity" | "infinity" => "inf",
        "-Infinity" | "-infinity" => "-inf",
        "NaN" | "nan" => "NaN",
        other => other,
    };
    normalized
        .parse::<T>()
        .map_err(|_| invalid_input(ty, column, bytes))
}

fn trimmed(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .unwrap_or_default()
}

fn parse_date(bytes: &[u8], column: &str) -> PgWireResult<i32> {
    use chrono::NaiveDate;
    let text = std::str::from_utf8(bytes).unwrap_or_default().trim();
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(|_| {
        user_error(
            INVALID_DATETIME,
            format!(
                "invalid input syntax for type date in column \"{column}\": \"{text}\""
            ),
        )
    })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("the Unix epoch is a date");
    i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(|_| {
        user_error(
            NUMERIC_RANGE,
            format!("date for column \"{column}\" is out of range"),
        )
    })
}

/// Parse a timestamp into microseconds since the Unix epoch. An explicit
/// numeric offset is applied; without one the value is read as UTC, which is
/// the time zone a LakeSoul `timestamptz` column stores.
fn parse_timestamp(bytes: &[u8], column: &str) -> PgWireResult<i64> {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    let text = std::str::from_utf8(bytes).unwrap_or_default().trim();
    let (main, offset) = split_offset(text);
    let main = main.trim_end().replace('T', " ");
    let (date, time) = main.split_once(' ').ok_or_else(|| {
        user_error(
            INVALID_DATETIME,
            format!(
                "invalid input syntax for type timestamp in column \
                 \"{column}\": \"{text}\""
            ),
        )
    })?;
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| {
        user_error(
            INVALID_DATETIME,
            format!(
                "invalid input syntax for type timestamp in column \
                 \"{column}\": \"{text}\""
            ),
        )
    })?;
    let time = NaiveTime::parse_from_str(time, "%H:%M:%S%.f").map_err(|_| {
        user_error(
            INVALID_DATETIME,
            format!(
                "invalid input syntax for type timestamp in column \
                 \"{column}\": \"{text}\""
            ),
        )
    })?;
    let value = NaiveDateTime::new(date, time);
    let micros = value.and_utc().timestamp_micros();
    Ok(micros.saturating_sub(offset))
}

/// Splits `... +05:30` / `...+05` / `...Z` into the local timestamp and the
/// offset in microseconds.
fn split_offset(text: &str) -> (&str, i64) {
    if let Some(stripped) = text.strip_suffix('Z').or_else(|| text.strip_suffix('z')) {
        return (stripped, 0);
    }
    // The date's own '-' signs come before index 10, so the first sign after
    // that starts the time-zone offset (`00:00:00+05:30` or `... +05`).
    let bytes = text.as_bytes();
    let Some(index) =
        (10..bytes.len()).find(|index| bytes[*index] == b'+' || bytes[*index] == b'-')
    else {
        return (text, 0);
    };
    let (main, zone) = text.split_at(index);
    let sign = if zone.starts_with('-') { -1 } else { 1 };
    let digits = &zone[1..];
    let (hours, minutes) = if let Some((hours, minutes)) = digits.split_once(':') {
        (hours, minutes)
    } else if digits.len() == 4 {
        digits.split_at(2)
    } else {
        (digits, "0")
    };
    let (Ok(hours), Ok(minutes)) = (hours.parse::<i64>(), minutes.parse::<i64>()) else {
        return (text, 0);
    };
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return (text, 0);
    }
    (
        main.trim_end(),
        sign * (hours * 3_600_000_000 + minutes * 60_000_000),
    )
}

/// Parse a PostgreSQL numeric into a decimal128 mantissa at the column scale.
fn parse_decimal(
    bytes: &[u8],
    precision: u8,
    scale: i8,
    column: &str,
) -> PgWireResult<i128> {
    let text = std::str::from_utf8(bytes).unwrap_or_default().trim();
    let invalid = || invalid_input("numeric", column, bytes);
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'-') => (-1i128, &text[1..]),
        Some(b'+') => (1i128, &text[1..]),
        _ => (1i128, text),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(index) => {
            let exponent = rest[index + 1..].parse::<i32>().map_err(|_| invalid())?;
            (&rest[..index], exponent)
        }
        None => (rest, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty() && fraction.is_empty() {
        return Err(invalid());
    }
    if !integer.chars().all(|c| c.is_ascii_digit())
        || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        return Err(invalid());
    }
    let digits = format!("{integer}{fraction}");
    if digits.is_empty() {
        return Err(invalid());
    }
    // value = digits * 10^(point - digits.len()), where `point` is the number
    // of digits before the decimal point after applying the exponent.
    let point = i64::try_from(integer.len()).expect("integer length fits i64")
        + i64::from(exponent);
    let shift =
        point - i64::try_from(digits.len()).expect("digits fit i64") + i64::from(scale);
    let mut coefficient = digits.parse::<i128>().map_err(|_| {
        user_error(
            NUMERIC_RANGE,
            format!("numeric value for column \"{column}\" is out of range"),
        )
    })?;
    if shift >= 0 {
        let shift = u32::try_from(shift).map_err(|_| {
            user_error(
                NUMERIC_RANGE,
                format!("numeric value for column \"{column}\" is out of range"),
            )
        })?;
        coefficient = coefficient
            .checked_mul(10i128.checked_pow(shift).ok_or_else(|| {
                user_error(
                    NUMERIC_RANGE,
                    format!("numeric value for column \"{column}\" is out of range"),
                )
            })?)
            .ok_or_else(|| {
                user_error(
                    NUMERIC_RANGE,
                    format!("numeric value for column \"{column}\" is out of range"),
                )
            })?;
    } else {
        let dropped = u32::try_from(-shift).map_err(|_| {
            user_error(
                NUMERIC_RANGE,
                format!("numeric value for column \"{column}\" is out of range"),
            )
        })?;
        let divisor = 10i128.checked_pow(dropped).ok_or_else(|| {
            user_error(
                NUMERIC_RANGE,
                format!("numeric value for column \"{column}\" is out of range"),
            )
        })?;
        let remainder = coefficient % divisor;
        coefficient /= divisor;
        // PostgreSQL rounds to the column scale, half away from zero.
        if remainder >= (divisor + 1) / 2 {
            coefficient += 1;
        }
    }
    let coefficient = sign * coefficient;
    let limit = 10i128
        .checked_pow(u32::from(precision))
        .ok_or_else(|| internal("numeric precision is out of range".to_string()))?;
    if coefficient.abs() >= limit {
        return Err(user_error(
            NUMERIC_RANGE,
            format!(
                "numeric field overflow for column \"{column}\": the value does \
                 not fit numeric({precision},{scale})"
            ),
        ));
    }
    Ok(coefficient)
}

/// Parse PostgreSQL bytea input: hex (`\x...`) or the legacy escape format.
fn parse_bytea(bytes: &[u8], column: &str) -> PgWireResult<Vec<u8>> {
    let invalid = || invalid_input("bytea", column, bytes);
    if let Some(hex) = bytes.strip_prefix(b"\\x") {
        if hex.len() % 2 != 0 {
            return Err(invalid());
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        for pair in hex.as_chunks::<2>().0 {
            let (Some(high), Some(low)) = (hex_value(pair[0]), hex_value(pair[1])) else {
                return Err(invalid());
            };
            out.push(high * 16 + low);
        }
        return Ok(out);
    }
    if !bytes.contains(&b'\\') {
        return Ok(bytes.to_vec());
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            out.push(bytes[index]);
            index += 1;
            continue;
        }
        if bytes.get(index + 1) == Some(&b'\\') {
            out.push(b'\\');
            index += 2;
            continue;
        }
        let Some(digits) = bytes.get(index + 1..index + 4) else {
            return Err(invalid());
        };
        if !digits.iter().all(|byte| (b'0'..=b'7').contains(byte)) {
            return Err(invalid());
        }
        let value = (digits[0] - b'0') as u16 * 64
            + (digits[1] - b'0') as u16 * 8
            + (digits[2] - b'0') as u16;
        out.push((value & 0xff) as u8);
        index += 4;
    }
    Ok(out)
}

fn invalid_input(ty: &str, column: &str, value: &[u8]) -> PgWireError {
    user_error(
        INVALID_TEXT,
        format!(
            "invalid input syntax for type {ty} in column \"{column}\": \"{}\"",
            String::from_utf8_lossy(value)
        ),
    )
}

fn internal(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        message,
    )))
}

fn protocol_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "08P01".to_string(),
        message,
    )))
}

/// The connection's copy state, or a protocol error when no copy is open.
fn copy_cell<C>(client: &C) -> PgWireResult<Arc<CopyInCell>>
where
    C: ClientInfo + ?Sized,
{
    client
        .session_extensions()
        .get::<CopyInCell>()
        .ok_or_else(|| {
            protocol_error(
                "COPY data received without an active COPY ... FROM STDIN".to_string(),
            )
        })
}

fn take_state<C>(client: &C) -> PgWireResult<CopyInState>
where
    C: ClientInfo + ?Sized,
{
    copy_cell(client)?.0.lock().take().ok_or_else(|| {
        protocol_error("COPY finished without an active COPY ... FROM STDIN".to_string())
    })
}

/// Feed one `CopyData` frame into the open copy.
pub(crate) fn push_copy_data<C>(client: &C, data: &[u8]) -> PgWireResult<()>
where
    C: ClientInfo + ?Sized,
{
    let cell = copy_cell(client)?;
    let mut slot = cell.0.lock();
    let Some(state) = slot.as_mut() else {
        return Err(protocol_error(
            "COPY data received without an active COPY ... FROM STDIN".to_string(),
        ));
    };
    match state.push(data) {
        Ok(()) => Ok(()),
        Err(error) => {
            // The stream is unusable now; the error response ends the copy.
            *slot = None;
            Err(error)
        }
    }
}

/// Run the buffered copy as one write and report `COPY <rows>`.
async fn finish_copy<C>(client: &mut C) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: std::fmt::Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut state = take_state(client)?;
    let batches = state.finish()?;
    let connection = connection_session(client)?;
    // See the simple-query path: a block the client never learned about must
    // not receive this write.
    crate::transaction::reconcile(&connection, client.transaction_status()).await;
    let status = client.transaction_status();
    let database = connection.session.identity.database.clone();
    let execution = connection
        .cancellation
        .begin_statement(statement_timeout(client), None)?;
    let _statement = connection
        .session
        .label_statement(execution.query_id(), &execution.span());
    let written = run_under_limit(
        &execution,
        crate::insert::execute_copy_write(
            &state.table,
            &state.target_columns,
            batches,
            &connection.session.context,
            status,
            &database,
        ),
    )
    .await;
    match written {
        Ok(rows) => {
            execution.end_after_send(StatementOutcome::Completed);
            connection.cancellation.finish_execution(&execution);
            client
                .send(PgWireBackendMessage::CommandComplete(
                    Tag::new("COPY").with_rows(rows).into(),
                ))
                .await?;
            Ok(())
        }
        Err(error) => {
            execution.end_after_send(StatementOutcome::Failed);
            connection.cancellation.finish_execution(&execution);
            Err(error)
        }
    }
}

/// Discard the open copy after a client `CopyFail`.
fn fail_copy<C>(client: &C, message: &str) -> PgWireError
where
    C: ClientInfo + ?Sized,
{
    if let Some(cell) = client.session_extensions().get::<CopyInCell>() {
        *cell.0.lock() = None;
    }
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        QUERY_CANCELED.to_string(),
        format!("COPY from stdin failed: {message}"),
    )))
}

#[async_trait]
impl CopyHandler for LakeSoulQueryRouter {
    async fn on_copy_data<C>(
        &self,
        client: &mut C,
        copy_data: CopyData,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        push_copy_data(client, &copy_data.data)
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        finish_copy(client).await
    }

    async fn on_copy_fail<C>(&self, client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        fail_copy(client, &fail.message)
    }
}

/// Refuse a `COPY` that shares a simple-query batch with other statements.
///
/// The copy sub-protocol takes over the connection between the statement and
/// `CopyDone`; the remaining statements of the batch would never run.
///
/// The check tokenizes the text instead of re-parsing the statements the
/// splitter produced: sqlparser reads everything after `COPY ... FROM STDIN;`
/// as inline copy data, so the splitter sees one statement and would drop the
/// statements that follow. Top-level semicolons delimit real statements, and
/// a statement that begins with the `COPY` keyword is the one that matters.
pub(crate) fn refuse_copy_in_batch(sql: &str) -> PgWireResult<()> {
    let dialect = PostgreSqlDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        // Unbalanced quotes and other lexical errors are reported by the
        // parser, with its own message.
        return Ok(());
    };
    let mut statements: Vec<Vec<&Token>> = Vec::new();
    let mut current: Vec<&Token> = Vec::new();
    for token in &tokens {
        match token {
            Token::Whitespace(_) | Token::EOF => {}
            Token::SemiColon => {
                if !current.is_empty() {
                    statements.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(token),
        }
    }
    if !current.is_empty() {
        statements.push(current);
    }
    if statements.len() < 2 {
        return Ok(());
    }
    let starts_with_copy = statements.iter().any(|statement| {
        matches!(
            statement.first(),
            Some(Token::Word(word)) if word.keyword == Keyword::COPY
        )
    });
    if starts_with_copy {
        return Err(feature_not_supported(
            "COPY must be the only statement in a query".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_copy_sharing_a_batch_is_refused() {
        for sql in [
            "COPY t FROM STDIN; SELECT 1",
            "SELECT 1; COPY t FROM STDIN",
            "COPY t FROM STDIN; COPY u FROM STDIN",
        ] {
            let error = refuse_copy_in_batch(sql).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("COPY must be the only statement"),
                "{error}"
            );
        }
        for sql in [
            "COPY t FROM STDIN",
            "COPY t FROM STDIN;",
            "SELECT 1; SELECT 2",
            "SELECT ';' AS semi",
        ] {
            refuse_copy_in_batch(sql).unwrap();
        }
    }

    fn text_options(delimiter: u8) -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Text,
            delimiter,
            null: b"\\N".to_vec(),
            quote: b'"',
            escape: b'"',
            header: false,
        }
    }

    fn csv_options() -> CopyOptions {
        CopyOptions {
            format: CopyFormat::Csv,
            delimiter: b',',
            null: Vec::new(),
            quote: b'"',
            escape: b'"',
            header: false,
        }
    }

    fn decode_all(options: &CopyOptions, chunks: &[&[u8]]) -> Vec<Vec<Option<Vec<u8>>>> {
        let mut decoder = RowDecoder::new(options);
        let mut rows = Vec::new();
        for chunk in chunks {
            rows.extend(decoder.push(chunk).unwrap());
        }
        rows.extend(decoder.finish().unwrap());
        rows
    }

    #[test]
    fn text_splits_on_unescaped_delimiters() {
        let rows = decode_all(&text_options(b','), &[b"a\\,b,2\nx\\,y,3\n\\N,4\n"]);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0].as_deref(), Some(b"a,b".as_slice()));
        assert_eq!(rows[0][1].as_deref(), Some(b"2".as_slice()));
        assert_eq!(rows[1][0].as_deref(), Some(b"x,y".as_slice()));
        assert_eq!(rows[2][0], None);
    }

    #[test]
    fn text_accepts_frames_that_split_a_row() {
        let rows = decode_all(&text_options(b'\t'), &[b"a\t", b"b\n1\t2\n"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some(b"b".as_slice()));
        assert_eq!(rows[1][0].as_deref(), Some(b"1".as_slice()));
    }

    #[test]
    fn text_accepts_a_final_row_without_a_newline() {
        let rows = decode_all(&text_options(b'\t'), &[b"a\tb"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].as_deref(), Some(b"b".as_slice()));
    }

    #[test]
    fn text_ignores_the_end_of_data_marker() {
        let rows = decode_all(&text_options(b'\t'), &[b"1\t2\n\\.\n"]);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn text_unescapes_the_usual_sequences() {
        let rows = decode_all(&text_options(b'\t'), &[b"a\\tb\\n\\x41\\101\n"]);
        assert_eq!(rows[0][0].as_deref(), Some(b"a\tb\nAA".as_slice()));
    }

    #[test]
    fn csv_handles_quotes_newlines_and_empty_values() {
        let rows = decode_all(&csv_options(), &[b"1,\"a,b\",\"line\nnext\"\n2,\"\",\n"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some(b"a,b".as_slice()));
        assert_eq!(rows[0][2].as_deref(), Some(b"line\nnext".as_slice()));
        assert_eq!(rows[1][1].as_deref(), Some(b"".as_slice()));
        assert_eq!(rows[1][2], None);
    }

    #[test]
    fn csv_handles_doubled_quotes_and_crlf() {
        let rows = decode_all(&csv_options(), &[b"1,\"a\"\"b\"\r\n2,c\r\n"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].as_deref(), Some(b"a\"b".as_slice()));
    }

    #[test]
    fn csv_supports_a_custom_null_string() {
        let mut options = csv_options();
        options.null = b"NULL".to_vec();
        let rows = decode_all(&options, &[b"1,NULL\n2,\"NULL\"\n"]);
        assert_eq!(rows[0][1], None);
        assert_eq!(rows[1][1].as_deref(), Some(b"NULL".as_slice()));
    }

    #[test]
    fn csv_rejects_an_unterminated_quote() {
        let mut decoder = RowDecoder::new(&csv_options());
        decoder.push(b"1,\"open").unwrap();
        let error = decoder.finish().unwrap_err();
        assert!(error.to_string().contains("unterminated"));
    }

    #[test]
    fn numbers_and_booleans_decode() {
        let column = |data_type| CopyColumn {
            name: "c".to_string(),
            data_type,
        };
        assert_eq!(
            decode_value(&column(DataType::Int32), Some(b"42")).unwrap(),
            ScalarValue::Int32(Some(42))
        );
        assert_eq!(
            decode_value(&column(DataType::Boolean), Some(b"t")).unwrap(),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            decode_value(&column(DataType::Utf8), None).unwrap(),
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            decode_value(&column(DataType::Binary), Some(b"\\xdeadbeef")).unwrap(),
            ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef]))
        );
    }

    #[test]
    fn timestamps_apply_their_offset() {
        let column = CopyColumn {
            name: "ts".to_string(),
            data_type: DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        };
        let value = decode_value(&column, Some(b"1970-01-01 05:30:00+05:30")).unwrap();
        assert_eq!(
            value,
            ScalarValue::TimestampMicrosecond(Some(0), Some(Arc::from("UTC")))
        );
    }

    #[test]
    fn decimals_round_to_the_column_scale() {
        let column = CopyColumn {
            name: "n".to_string(),
            data_type: DataType::Decimal128(10, 2),
        };
        assert_eq!(
            decode_value(&column, Some(b"1.239")).unwrap(),
            ScalarValue::Decimal128(Some(124), 10, 2)
        );
        assert_eq!(
            decode_value(&column, Some(b"-0.005")).unwrap(),
            ScalarValue::Decimal128(Some(-1), 10, 2)
        );
    }

    #[test]
    fn a_wrong_field_count_is_bad_copy_format() {
        let columns = vec![
            CopyColumn {
                name: "a".to_string(),
                data_type: DataType::Int32,
            },
            CopyColumn {
                name: "b".to_string(),
                data_type: DataType::Int32,
            },
        ];
        let fewer = check_arity(&[None], &columns).unwrap_err();
        assert!(
            fewer.to_string().contains("missing data for column \"b\""),
            "{fewer}"
        );
        let more = check_arity(&[None, None, None], &columns).unwrap_err();
        assert!(
            more.to_string()
                .contains("extra data after last expected column"),
            "{more}"
        );
    }
}
