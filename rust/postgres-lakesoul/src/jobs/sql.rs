// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The SQL control surface of detached queries.
//!
//! `lakesoul_submit`, `lakesoul_cancel_query` and `lakesoul_wait_query` are
//! claimed as statement hooks rather than registered as ordinary functions.
//! They have side effects, and a scalar UDF would run once per input row: a
//! `SELECT lakesoul_submit(...) FROM t` would submit N jobs. The hook accepts
//! only the single-row, no-FROM shape and rejects every other use.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query,
    SelectItem, SetExpr, Statement, Value,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use uuid::Uuid;

use crate::jobs::{CancelOutcome, JobError, JobService, JobSettings, SubmitRequest};
use crate::server::connection_session;
use crate::session_settings::show_response;

const SUBMIT: &str = "lakesoul_submit";
const CANCEL: &str = "lakesoul_cancel_query";
const WAIT: &str = "lakesoul_wait_query";

/// The three SQL functions the hook answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobFunction {
    Submit,
    Cancel,
    Wait,
}

impl JobFunction {
    fn name(self) -> &'static str {
        match self {
            Self::Submit => SUBMIT,
            Self::Cancel => CANCEL,
            Self::Wait => WAIT,
        }
    }

    fn response_column(self) -> &'static str {
        match self {
            Self::Submit => "query_id",
            Self::Cancel => "cancelled",
            Self::Wait => "state",
        }
    }
}

/// What a statement looks like from the hook's point of view.
enum CallMatch<'a> {
    /// The exact supported shape: a single function call with no FROM.
    Call(&'a Function),
    /// The name appears, but in a shape the hook does not answer.
    Mentioned,
    NotMentioned,
}

/// Answers the detached-query control statements.
pub struct JobStatementHook {
    jobs: Arc<JobService>,
}

impl JobStatementHook {
    pub fn new(jobs: Arc<JobService>) -> Self {
        Self { jobs }
    }

    async fn answer(
        &self,
        statement: &Statement,
        client: &mut dyn HookClient,
        params: Option<&ParamValues>,
    ) -> PgWireResult<Option<Response>> {
        for function in [JobFunction::Submit, JobFunction::Cancel, JobFunction::Wait] {
            match call_match(statement, function.name()) {
                CallMatch::Call(call) => {
                    return self.run(function, call, client, params).await.map(Some);
                }
                CallMatch::Mentioned => {
                    return Err(shape_error(function.name()));
                }
                CallMatch::NotMentioned => {}
            }
        }
        Ok(None)
    }

    async fn run(
        &self,
        function: JobFunction,
        call: &Function,
        client: &mut dyn HookClient,
        params: Option<&ParamValues>,
    ) -> PgWireResult<Response> {
        let state = connection_session(client)?;
        let user = state.session.identity.user.clone();
        let database = state.session.identity.database.clone();
        let args = FunctionArgs::parse(call, params)?;
        match function {
            JobFunction::Submit => {
                // Submitting is a control action, not a transactional write:
                // the job row is written through the metadata pool and cannot
                // be rolled back with the caller's transaction. It is allowed
                // in a transaction block so drivers that begin one implicitly
                // (psycopg, SQLAlchemy) can call it without changing their
                // autocommit mode.
                let sql = args.required_string("sql", 0)?;
                let job_key = args.optional_string("job_key", 1)?;
                let timeout_ms = args.optional_i64("timeout_ms", 2)?;
                let retry_on_restart =
                    args.optional_bool("retry_on_restart", 3)?.unwrap_or(true);
                let settings = JobSettings::from_session(
                    &state.session.settings.read().clone(),
                    crate::ivm::ivm_enabled(&state.session.context),
                    client.metadata().get("application_name").cloned(),
                );
                let query_id = self
                    .jobs
                    .submit(SubmitRequest {
                        sql,
                        job_key,
                        timeout_ms,
                        retry_on_restart,
                        user,
                        database,
                        settings,
                    })
                    .await
                    .map_err(job_error)?;
                show_response(function.response_column(), &query_id.to_string())
            }
            JobFunction::Cancel => {
                let raw = args.required_string("query_id", 0)?;
                let query_id = parse_query_id(&raw)?;
                let outcome = self
                    .jobs
                    .cancel(query_id, &user, &database)
                    .await
                    .map_err(job_error)?;
                let cancelled =
                    matches!(outcome, CancelOutcome::Queued | CancelOutcome::Running);
                show_response(
                    function.response_column(),
                    if cancelled { "t" } else { "f" },
                )
            }
            JobFunction::Wait => {
                let raw = args.required_string("query_id", 0)?;
                let query_id = parse_query_id(&raw)?;
                let timeout_ms = args.optional_i64("timeout_ms", 1)?.unwrap_or(0);
                let state = self
                    .jobs
                    .wait(query_id, &user, &database, timeout_ms)
                    .await
                    .map_err(job_error)?;
                show_response(function.response_column(), state.as_str())
            }
        }
    }
}

#[async_trait]
impl QueryHook for JobStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.answer(statement, client, None).await.transpose()
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        for function in [JobFunction::Submit, JobFunction::Cancel, JobFunction::Wait] {
            match call_match(statement, function.name()) {
                // Let the stub function plan the call: its signature is what
                // infers the parameter types, so a bound `$2` is decoded as an
                // integer before Execute reaches the hook.
                CallMatch::Call(_) => return None,
                CallMatch::Mentioned => return Some(Err(shape_error(function.name()))),
                CallMatch::NotMentioned => {}
            }
        }
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.answer(statement, client, Some(params))
            .await
            .transpose()
    }
}

/// Extracts the one function call a statement is, or says why the hook will
/// not answer it.
fn call_match<'a>(statement: &'a Statement, name: &str) -> CallMatch<'a> {
    let Some(query) = query_of(statement) else {
        return if mentions(statement, name) {
            CallMatch::Mentioned
        } else {
            CallMatch::NotMentioned
        };
    };
    let Some(function) = top_level_function(query) else {
        return if mentions(statement, name) {
            CallMatch::Mentioned
        } else {
            CallMatch::NotMentioned
        };
    };
    if function_name(function).eq_ignore_ascii_case(name) {
        CallMatch::Call(function)
    } else if mentions(statement, name) {
        CallMatch::Mentioned
    } else {
        CallMatch::NotMentioned
    }
}

fn query_of(statement: &Statement) -> Option<&Query> {
    match statement {
        Statement::Query(query) => Some(query),
        _ => None,
    }
}

/// The function a `SELECT <function>(...)` with no FROM projects, and nothing
/// else. Aliases are allowed (`SELECT lakesoul_submit(...) AS id`).
fn top_level_function(query: &Query) -> Option<&Function> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = &*query.body else {
        return None;
    };
    if !select.from.is_empty()
        || select.selection.is_some()
        || select.having.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(items, _) if items.is_empty())
    {
        return None;
    }
    if select.projection.len() != 1 {
        return None;
    }
    let expr = match &select.projection[0] {
        SelectItem::UnnamedExpr(expr) => expr,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return None,
    };
    match expr {
        Expr::Function(function) => Some(function),
        Expr::Nested(expr) => match expr.as_ref() {
            Expr::Function(function) => Some(function),
            _ => None,
        },
        _ => None,
    }
}

fn function_name(function: &Function) -> String {
    function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .unwrap_or_default()
}

fn mentions(statement: &Statement, name: &str) -> bool {
    let Some(query) = query_of(statement) else {
        return false;
    };
    let SetExpr::Select(select) = &*query.body else {
        return false;
    };
    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) => expression_mentions(expr, name),
        SelectItem::ExprWithAlias { expr, .. } => expression_mentions(expr, name),
        _ => false,
    })
}

fn expression_mentions(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Function(function) => function_name(function).eq_ignore_ascii_case(name),
        Expr::Nested(expr) => expression_mentions(expr, name),
        Expr::Cast { expr, .. } => expression_mentions(expr, name),
        _ => false,
    }
}

fn shape_error(name: &str) -> PgWireError {
    user_error(
        "0A000",
        format!(
            "{name} must be called as the only expression of a single SELECT \
             with no FROM clause"
        ),
    )
}

fn parse_query_id(raw: &str) -> PgWireResult<Uuid> {
    Uuid::parse_str(raw.trim()).map_err(|_| {
        user_error(
            "22P02",
            format!("invalid input syntax for query id: \"{raw}\""),
        )
    })
}

fn job_error(error: JobError) -> PgWireError {
    user_error(&error.sqlstate, error.message)
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message,
    )))
}

/// The arguments of one call, by position or by name.
struct FunctionArgs<'a> {
    positional: Vec<Expr>,
    named: Vec<(String, Expr)>,
    params: Option<&'a ParamValues>,
}

impl FunctionArgs<'_> {
    fn parse<'a>(
        function: &Function,
        params: Option<&'a ParamValues>,
    ) -> PgWireResult<FunctionArgs<'a>> {
        let FunctionArguments::List(list) = &function.args else {
            return Err(user_error(
                "0A000",
                format!("{} needs explicit arguments", function_name(function)),
            ));
        };
        let mut positional = Vec::new();
        let mut named = Vec::new();
        for arg in &list.args {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    positional.push(expr.clone());
                }
                FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => {
                    named.push((name.value.to_ascii_lowercase(), expr.clone()));
                }
                FunctionArg::ExprNamed {
                    name,
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => {
                    let key = match name {
                        Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
                        other => other.to_string().to_ascii_lowercase(),
                    };
                    named.push((key, expr.clone()));
                }
                other => {
                    return Err(user_error(
                        "0A000",
                        format!("unsupported argument {other}"),
                    ));
                }
            }
        }
        Ok(FunctionArgs {
            positional,
            named,
            params,
        })
    }

    fn expr(&self, name: &str, position: usize) -> Option<&Expr> {
        self.named
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, expr)| expr)
            .or_else(|| self.positional.get(position))
    }

    fn required_string(&self, name: &str, position: usize) -> PgWireResult<String> {
        match self.expr(name, position) {
            Some(expr) => literal_string(expr, self.params).flatten().ok_or_else(|| {
                user_error(
                    "0A000",
                    format!("{name} must be a string literal (got {expr})"),
                )
            }),
            None => Err(user_error("0A000", format!("{name} is required"))),
        }
    }

    fn optional_string(
        &self,
        name: &str,
        position: usize,
    ) -> PgWireResult<Option<String>> {
        match self.expr(name, position) {
            None => Ok(None),
            Some(expr) => literal_string(expr, self.params).ok_or_else(|| {
                user_error(
                    "0A000",
                    format!("{name} must be a string literal (got {expr})"),
                )
            }),
        }
    }

    fn optional_bool(&self, name: &str, position: usize) -> PgWireResult<Option<bool>> {
        match self.expr(name, position) {
            None => Ok(None),
            Some(expr) => literal_bool(expr, self.params).ok_or_else(|| {
                user_error(
                    "0A000",
                    format!("{name} must be a boolean literal (got {expr})"),
                )
            }),
        }
    }

    fn optional_i64(&self, name: &str, position: usize) -> PgWireResult<Option<i64>> {
        match self.expr(name, position) {
            None => Ok(None),
            Some(expr) => literal_i64(expr, self.params).ok_or_else(|| {
                user_error(
                    "0A000",
                    format!("{name} must be an integer literal (got {expr})"),
                )
            }),
        }
    }
}

/// `Some(None)` is SQL NULL, `Some(Some(_))` a value, `None` a non-literal.
fn literal_string(expr: &Expr, params: Option<&ParamValues>) -> Option<Option<String>> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Null => Some(None),
            Value::Placeholder(id) => scalar_string(parameter(params, id)?),
            other => other.clone().into_string().map(Some),
        },
        Expr::Nested(expr) => literal_string(expr, params),
        Expr::Cast { expr, .. } => literal_string(expr, params),
        _ => None,
    }
}

fn literal_i64(expr: &Expr, params: Option<&ParamValues>) -> Option<Option<i64>> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Null => Some(None),
            Value::Number(raw, _) => raw.parse::<i64>().ok().map(Some),
            Value::Placeholder(id) => scalar_i64(parameter(params, id)?),
            _ => None,
        },
        Expr::Nested(expr) => literal_i64(expr, params),
        Expr::Cast { expr, .. } => literal_i64(expr, params),
        _ => None,
    }
}

fn literal_bool(expr: &Expr, params: Option<&ParamValues>) -> Option<Option<bool>> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Null => Some(None),
            Value::Boolean(value) => Some(Some(*value)),
            Value::Placeholder(id) => scalar_bool(parameter(params, id)?),
            other => match other.clone().into_string()?.to_ascii_lowercase().as_str() {
                "on" | "true" | "1" => Some(Some(true)),
                "off" | "false" | "0" => Some(Some(false)),
                _ => None,
            },
        },
        Expr::Nested(expr) => literal_bool(expr, params),
        Expr::Cast { expr, .. } => literal_bool(expr, params),
        _ => None,
    }
}

fn scalar_bool(value: ScalarValue) -> Option<Option<bool>> {
    match value {
        ScalarValue::Null => Some(None),
        ScalarValue::Boolean(Some(value)) => Some(Some(value)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => {
            match value.trim().to_ascii_lowercase().as_str() {
                "on" | "true" | "1" => Some(Some(true)),
                "off" | "false" | "0" => Some(Some(false)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn parameter(params: Option<&ParamValues>, id: &str) -> Option<ScalarValue> {
    params
        .and_then(|params| params.get_placeholders_with_values(id).ok())
        .map(|value| value.value)
}

fn scalar_string(value: ScalarValue) -> Option<Option<String>> {
    match value {
        ScalarValue::Null => Some(None),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(Some(value)),
        _ => None,
    }
}

fn scalar_i64(value: ScalarValue) -> Option<Option<i64>> {
    match value {
        ScalarValue::Null => Some(None),
        ScalarValue::Int64(Some(value)) => Some(Some(value)),
        ScalarValue::Int32(Some(value)) => Some(Some(value as i64)),
        ScalarValue::Int16(Some(value)) => Some(Some(value as i64)),
        ScalarValue::Int8(Some(value)) => Some(Some(value as i64)),
        ScalarValue::UInt64(Some(value)) => Some(Some(value as i64)),
        ScalarValue::UInt32(Some(value)) => Some(Some(value as i64)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => {
            value.trim().parse::<i64>().ok().map(Some)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

    fn statement(sql: &str) -> Statement {
        PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn submit_shape_is_recognized() {
        let statement = statement(
            "select lakesoul_submit($job$insert into t select 1$job$, \
             'orders', 60000) as id",
        );
        let CallMatch::Call(call) = call_match(&statement, SUBMIT) else {
            panic!("not recognized");
        };
        let args = FunctionArgs::parse(call, None).expect("args");
        assert_eq!(
            args.required_string("sql", 0).expect("sql"),
            "insert into t select 1"
        );
        assert_eq!(
            args.optional_string("job_key", 1).expect("key"),
            Some("orders".to_string())
        );
        assert_eq!(
            args.optional_i64("timeout_ms", 2).expect("timeout"),
            Some(60000)
        );
    }

    #[test]
    fn submit_with_a_from_clause_is_rejected() {
        let statement =
            statement("select lakesoul_submit('insert into t select 1') from t");
        assert!(matches!(
            call_match(&statement, SUBMIT),
            CallMatch::Mentioned
        ));
    }

    #[test]
    fn submit_with_a_second_projection_is_rejected() {
        let statement = statement("select lakesoul_submit('insert into t select 1'), 1");
        assert!(matches!(
            call_match(&statement, SUBMIT),
            CallMatch::Mentioned
        ));
    }

    #[test]
    fn a_plain_select_is_not_claimed() {
        let statement = statement("select 1");
        assert!(matches!(
            call_match(&statement, SUBMIT),
            CallMatch::NotMentioned
        ));
    }

    #[test]
    fn a_string_literal_is_not_a_function_call() {
        let statement = statement("select 'lakesoul_submit'");
        assert!(matches!(
            call_match(&statement, SUBMIT),
            CallMatch::NotMentioned
        ));
    }

    #[test]
    fn named_arguments_are_read() {
        let statement = statement(
            "select lakesoul_cancel_query(query_id => '7c9e8f3a-0000-0000-0000-000000000000')",
        );
        let CallMatch::Call(call) = call_match(&statement, CANCEL) else {
            panic!("not recognized");
        };
        let args = FunctionArgs::parse(call, None).expect("args");
        assert_eq!(
            args.required_string("query_id", 0).expect("query id"),
            "7c9e8f3a-0000-0000-0000-000000000000"
        );
    }
}

#[cfg(test)]
mod parameter_tests {
    use super::*;
    use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

    fn statement(sql: &str) -> Statement {
        PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn wait_reads_bound_parameters() {
        let statement = statement("select lakesoul_wait_query($1, $2)");
        let CallMatch::Call(call) = call_match(&statement, WAIT) else {
            panic!("not recognized");
        };
        let params = ParamValues::from(vec![
            ScalarValue::Utf8(Some("7c9e8f3a-0000-0000-0000-000000000000".to_string())),
            ScalarValue::Int32(Some(5000)),
        ]);
        let args = FunctionArgs::parse(call, Some(&params)).expect("args");
        assert_eq!(
            args.required_string("query_id", 0).expect("query id"),
            "7c9e8f3a-0000-0000-0000-000000000000"
        );
        assert_eq!(
            args.optional_i64("timeout_ms", 1).expect("timeout"),
            Some(5000)
        );
    }
}

/// Keeps the session identity extension out of reach of SQL `SET`.
///
/// `lakesoul_query_status` and `lakesoul_query_result` check ownership from
/// `PgIdentityConfig`, which lives in the DataFusion session config. Without
/// this guard `SET lakesoul_pg_identity.user = 'someone'` would be parsed as a
/// generic extension option and let a session read another user's jobs.
pub(crate) struct JobIdentityGuard;

fn changes_identity(statement: &Statement) -> bool {
    fn named(name: &datafusion::sql::sqlparser::ast::ObjectName) -> bool {
        name.0
            .first()
            .and_then(|part| part.as_ident())
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case("lakesoul_pg_identity"))
    }
    match statement {
        Statement::Set(set) => match set {
            datafusion::sql::sqlparser::ast::Set::SingleAssignment {
                variable, ..
            } => named(variable),
            datafusion::sql::sqlparser::ast::Set::ParenthesizedAssignments {
                variables,
                ..
            } => variables.iter().any(named),
            datafusion::sql::sqlparser::ast::Set::MultipleAssignments { assignments } => {
                assignments.iter().any(|assignment| named(&assignment.name))
            }
            _ => false,
        },
        Statement::Reset(reset) => match &reset.reset {
            datafusion::sql::sqlparser::ast::Reset::ConfigurationParameter(name) => {
                named(name)
            }
            // `RESET ALL` restores the captured defaults, including the
            // identity; it is not a spoofing path.
            datafusion::sql::sqlparser::ast::Reset::ALL => false,
        },
        _ => false,
    }
}

fn identity_error() -> PgWireError {
    user_error(
        "42501",
        "permission denied: the query job identity is read-only".to_string(),
    )
}

#[async_trait]
impl QueryHook for JobIdentityGuard {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        changes_identity(statement).then(|| Err(identity_error()))
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        changes_identity(statement).then(|| Err(identity_error()))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        changes_identity(statement).then(|| Err(identity_error()))
    }
}

/// Registers the scalar stubs whose signatures infer bound parameter types.
///
/// The hook answers these calls at Execute; the stubs are never evaluated. A
/// `SELECT lakesoul_submit($1, $2, $3)` must nevertheless be planned once so
/// the extended protocol knows `$3` is an integer and not text.
pub(crate) fn register_control_stubs(context: &SessionContext) {
    use datafusion::logical_expr::ScalarUDF;
    context.register_udf(ScalarUDF::from(SubmitStub));
    context.register_udf(ScalarUDF::from(CancelStub));
    context.register_udf(ScalarUDF::from(WaitStub));
}

macro_rules! control_stub {
    ($ty:ident, $name:literal, $signature:expr, $column:literal) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $ty;

        impl datafusion::logical_expr::ScalarUDFImpl for $ty {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &datafusion::logical_expr::Signature {
                static SIGNATURE: std::sync::LazyLock<
                    datafusion::logical_expr::Signature,
                > = std::sync::LazyLock::new(|| $signature);
                &SIGNATURE
            }

            fn return_type(
                &self,
                _arg_types: &[datafusion::arrow::datatypes::DataType],
            ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
                Ok(datafusion::arrow::datatypes::DataType::Utf8)
            }

            fn schema_name(
                &self,
                _args: &[datafusion::prelude::Expr],
            ) -> datafusion::error::Result<String> {
                Ok($column.to_string())
            }

            fn invoke_with_args(
                &self,
                _args: datafusion::logical_expr::ScalarFunctionArgs,
            ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
                Err(datafusion::error::DataFusionError::Plan(format!(
                    "{} is only available as a top-level control statement",
                    $name
                )))
            }
        }
    };
}

control_stub!(
    SubmitStub,
    "lakesoul_submit",
    datafusion::logical_expr::Signature::one_of(
        vec![
            datafusion::logical_expr::TypeSignature::Exact(vec![
                datafusion::arrow::datatypes::DataType::Utf8,
            ]),
            datafusion::logical_expr::TypeSignature::Exact(vec![
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Utf8,
            ]),
            datafusion::logical_expr::TypeSignature::Exact(vec![
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Int64,
            ]),
            datafusion::logical_expr::TypeSignature::Exact(vec![
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Int64,
                datafusion::arrow::datatypes::DataType::Boolean,
            ]),
        ],
        datafusion::logical_expr::Volatility::Volatile,
    ),
    "query_id"
);

control_stub!(
    CancelStub,
    "lakesoul_cancel_query",
    datafusion::logical_expr::Signature::exact(
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::logical_expr::Volatility::Volatile,
    ),
    "cancelled"
);

control_stub!(
    WaitStub,
    "lakesoul_wait_query",
    datafusion::logical_expr::Signature::one_of(
        vec![
            datafusion::logical_expr::TypeSignature::Exact(vec![
                datafusion::arrow::datatypes::DataType::Utf8,
            ]),
            datafusion::logical_expr::TypeSignature::Exact(vec![
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Int64,
            ]),
        ],
        datafusion::logical_expr::Volatility::Volatile,
    ),
    "state"
);

#[cfg(test)]
mod identity_guard_tests {
    use super::*;
    use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

    fn statement(sql: &str) -> Statement {
        PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn identity_settings_are_rejected() {
        assert!(changes_identity(&statement(
            "set lakesoul_pg_identity.user = 'other'"
        )));
        assert!(changes_identity(&statement(
            "reset lakesoul_pg_identity.user"
        )));
    }

    #[test]
    fn ordinary_resets_are_allowed() {
        assert!(!changes_identity(&statement("reset all")));
        assert!(!changes_identity(&statement("reset statement_timeout")));
    }
}
