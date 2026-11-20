// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shims over the upstream `pg_catalog` functions.
//!
//! Real PostgreSQL declares `pg_relation_is_publishable(regclass)`, and psql
//! sends the relation oid as a *quoted literal*:
//!
//! ```sql
//! WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16495')
//! ```
//!
//! The upstream catalog registers the function for `Int32` only, so that call
//! fails with `type_coercion` and `\d <table>` cannot work from psql. LakeSoul
//! has no publications, so the shim accepts both the integer oid and the
//! quoted form, answering `true` for every row like the upstream
//! implementation does.
//!
//! The shim is registered *after* the upstream `pg_catalog` setup: the
//! function registry is keyed by name, so this registration replaces the
//! oid-only signature instead of coexisting with it.
//!
//! `version()` is also answered here. PostgreSQL clients use it for feature
//! discovery: SQLAlchemy raises during dialect initialization unless the
//! string contains `PostgreSQL <major>.<minor>`. The DataFusion built-in
//! identifies itself as `Apache DataFusion ...`, which is accurate but makes
//! the PostgreSQL dialect unusable. The compatibility string names both
//! engines and deliberately keeps a conservative PostgreSQL baseline; see the
//! compatibility page for the version policy.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::LazyLock;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, StringBuilder, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{TableReference, ToDFSchema};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, LimitClause, ObjectName, Query,
    SetExpr, Statement, TableFactor, Value, ValueWithSpan, Visit, VisitMut, Visitor,
    VisitorMut,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::arrow_pg::encode_dataframe;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::Type;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{
    FieldFormat, FieldInfo, QueryResponse, Response,
};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;

use crate::pg_settings::PgParameters;

/// The PostgreSQL-compatible identity reported by `version()`.
///
/// Keep the PostgreSQL baseline at 14.0: clients use it to gate SQL
/// features, and LakeSoul does not implement the full PostgreSQL surface.
pub fn compatibility_version() -> &'static str {
    static VERSION: LazyLock<String> = LazyLock::new(|| {
        format!(
            "PostgreSQL 14.0 (LakeSoul {} / DataFusion {})",
            env!("CARGO_PKG_VERSION"),
            datafusion::DATAFUSION_VERSION
        )
    });
    VERSION.as_str()
}

/// Registers the shims into a connection-local session.
///
/// `parameters` is the connection's session-parameter map; the
/// `current_setting`/`set_config` functions share it with `SHOW` and
/// `pg_settings`, so every reader sees the same value.
pub fn register_pg_catalog_shims(
    context: &SessionContext,
    parameters: &Arc<PgParameters>,
    user: &str,
) {
    context.register_udf(ScalarUDF::from(PgRelationIsPublishable));
    context.register_udf(ScalarUDF::from(PgVersion));
    context.register_udf(ScalarUDF::from(PgIsInRecovery));
    context.register_udf(ScalarUDF::from(PgGetSerialSequence));
    context.register_udf(ScalarUDF::from(PgVisibility("pg_type_is_visible")));
    context.register_udf(ScalarUDF::from(PgVisibility("pg_function_is_visible")));
    context.register_udf(ScalarUDF::from(ToRegtype));
    // Replaces the upstream UDF, which hard-codes "postgres". `current_user`
    // is rewritten to this function by the compatibility parser.
    context.register_udf(ScalarUDF::from(PgSessionUser::new(user)));
    context.register_udf(ScalarUDF::from(PgCurrentSetting::new(Arc::clone(
        parameters,
    ))));
    context.register_udf(ScalarUDF::from(PgSetConfig::new(Arc::clone(parameters))));
    // Replaces DataFusion's strftime-based `to_char`: PostgreSQL patterns
    // (`YYYY-MM-DD`) are not chrono patterns, and the old function silently
    // returned the pattern text for them. `register_udf` alone leaves the
    // built-in resolvable, so drop it first.
    context.deregister_udf("to_char");
    context.register_udf(ScalarUDF::from(PgToChar));
}

/// `session_user` (and, through the compatibility parser's rewrite,
/// `current_user`) for this connection.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgSessionUser(String);

impl PgSessionUser {
    fn new(user: &str) -> Self {
        Self(user.to_string())
    }
}

impl ScalarUDFImpl for PgSessionUser {
    fn name(&self) -> &str {
        "session_user"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![], Volatility::Stable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef =
            Arc::new(StringArray::from(vec![self.0.as_str(); args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

/// PostgreSQL `current_setting(name[, missing_ok])`.
#[derive(Debug)]
struct PgCurrentSetting {
    parameters: Arc<PgParameters>,
}

impl PgCurrentSetting {
    fn new(parameters: Arc<PgParameters>) -> Self {
        Self { parameters }
    }
}

impl PartialEq for PgCurrentSetting {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.parameters, &other.parameters)
    }
}

impl Eq for PgCurrentSetting {}

impl Hash for PgCurrentSetting {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.parameters).hash(state);
    }
}

impl ScalarUDFImpl for PgCurrentSetting {
    fn name(&self) -> &str {
        "current_setting"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Boolean]),
                ],
                Volatility::Stable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let names = args.args[0].clone().into_array(args.number_rows)?;
        let names = names
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "current_setting requires a text parameter name".to_string(),
                )
            })?;
        let missing_ok = if args.args.len() == 2 {
            let flags = args.args[1].clone().into_array(args.number_rows)?;
            Some(
                flags
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "current_setting's missing_ok argument must be boolean"
                                .to_string(),
                        )
                    })?
                    .clone(),
            )
        } else {
            None
        };
        let mut values = StringBuilder::new();
        for index in 0..args.number_rows {
            if names.is_null(index) {
                values.append_null();
                continue;
            }
            match self.parameters.get(names.value(index)) {
                Some(value) => values.append_value(value),
                None => {
                    let allowed = missing_ok
                        .as_ref()
                        .is_some_and(|flags| !flags.is_null(index) && flags.value(index));
                    if allowed {
                        values.append_null();
                    } else {
                        // DataFusion reports this as XX000. Unknown parameters
                        // are rare in the BI paths that use current_setting;
                        // the missing_ok form (used by probes) is exact.
                        values.append_null();
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(values.finish())))
    }
}

/// PostgreSQL `set_config(name, value, is_local)`.
#[derive(Debug)]
struct PgSetConfig {
    parameters: Arc<PgParameters>,
}

impl PgSetConfig {
    fn new(parameters: Arc<PgParameters>) -> Self {
        Self { parameters }
    }
}

impl PartialEq for PgSetConfig {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.parameters, &other.parameters)
    }
}

impl Eq for PgSetConfig {}

impl Hash for PgSetConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.parameters).hash(state);
    }
}

impl ScalarUDFImpl for PgSetConfig {
    fn name(&self) -> &str {
        "set_config"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
                Volatility::Volatile,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let names = args.args[0].clone().into_array(args.number_rows)?;
        let names = names
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "set_config requires a text parameter name".to_string(),
                )
            })?;
        let values = args.args[1].clone().into_array(args.number_rows)?;
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("set_config requires a text value".to_string())
            })?;
        let mut result = StringBuilder::new();
        for index in 0..args.number_rows {
            if names.is_null(index) || values.is_null(index) {
                result.append_null();
                continue;
            }
            let name = names.value(index);
            let value = values.value(index);
            match self.parameters.set(name, value) {
                Some(_) => result.append_value(value),
                None => result.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

/// PostgreSQL `to_char(timestamp|date, pattern)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgToChar;

impl ScalarUDFImpl for PgToChar {
    fn name(&self) -> &str {
        "to_char"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let values = args.args[0].clone().into_array(args.number_rows)?;
        let formats = args.args[1].clone().into_array(args.number_rows)?;
        let formats =
            formats
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "to_char requires a text format pattern".to_string(),
                    )
                })?;
        let mut text = StringBuilder::new();
        for index in 0..args.number_rows {
            if values.is_null(index) || formats.is_null(index) {
                text.append_null();
                continue;
            }
            let (timestamp, has_time_zone) = temporal_value(&values, index)?;
            text.append_value(pg_format_timestamp(
                timestamp,
                has_time_zone,
                formats.value(index),
            ));
        }
        Ok(ColumnarValue::Array(Arc::new(text.finish())))
    }
}

fn temporal_value(values: &ArrayRef, index: usize) -> DFResult<(NaiveDateTime, bool)> {
    let out_of_range =
        || DataFusionError::Execution("timestamp out of range for to_char".to_string());
    let (micros, has_time_zone) = match values.data_type() {
        DataType::Timestamp(unit, time_zone) => {
            let micros = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => values
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .map(|array| array.value(index) * 1_000_000),
                datafusion::arrow::datatypes::TimeUnit::Millisecond => values
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .map(|array| array.value(index) * 1_000),
                datafusion::arrow::datatypes::TimeUnit::Microsecond => values
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .map(|array| array.value(index)),
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => values
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .map(|array| array.value(index) / 1_000),
            }
            .ok_or_else(out_of_range)?;
            (micros, time_zone.is_some())
        }
        DataType::Date32 => {
            let days = values
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(out_of_range)?
                .value(index);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("the Unix epoch is a date")
                .checked_add_signed(chrono::Duration::days(i64::from(days)))
                .ok_or_else(out_of_range)?;
            (
                date.and_hms_opt(0, 0, 0)
                    .ok_or_else(out_of_range)?
                    .and_utc()
                    .timestamp_micros(),
                false,
            )
        }
        DataType::Date64 => {
            let millis = values
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(out_of_range)?
                .value(index);
            (millis * 1_000, false)
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "to_char currently supports date and timestamp values, not {other}"
            )));
        }
    };
    let timestamp = DateTime::from_timestamp_micros(micros)
        .ok_or_else(out_of_range)?
        .naive_utc();
    Ok((timestamp, has_time_zone))
}

/// Format one timestamp with PostgreSQL's `to_char` patterns.
///
/// Only the patterns reports actually use are implemented; an unrecognised
/// letter is copied through, which is what PostgreSQL does for literal text.
fn pg_format_timestamp(
    timestamp: NaiveDateTime,
    has_time_zone: bool,
    pattern: &str,
) -> String {
    let mut out = String::new();
    let mut rest = pattern;
    while !rest.is_empty() {
        let fill = rest.starts_with("FM");
        if fill {
            rest = &rest[2..];
            if rest.is_empty() {
                break;
            }
        }
        macro_rules! take {
            ($len:expr, $value:expr) => {{
                out.push_str($value.as_ref());
                rest = &rest[$len..];
                continue;
            }};
        }
        if rest.starts_with("YYYY") {
            take!(4, format!("{:04}", timestamp.year()));
        }
        if rest.starts_with("YYY") {
            take!(3, format!("{:03}", timestamp.year().rem_euclid(1000)));
        }
        if rest.starts_with("YY") {
            take!(2, format!("{:02}", timestamp.year().rem_euclid(100)));
        }
        if rest.starts_with("MONTH") {
            take!(
                5,
                format!("{:<9}", month_name(timestamp.month(), false, true))
            );
        }
        if rest.starts_with("Month") {
            take!(
                5,
                format!("{:<9}", month_name(timestamp.month(), false, false))
            );
        }
        if rest.starts_with("MON") {
            take!(3, month_name(timestamp.month(), true, true));
        }
        if rest.starts_with("Mon") {
            take!(3, month_name(timestamp.month(), true, false));
        }
        if rest.starts_with("MM") {
            let value = if fill {
                timestamp.month().to_string()
            } else {
                format!("{:02}", timestamp.month())
            };
            take!(2, value);
        }
        if rest.starts_with("DDD") {
            take!(3, format!("{:03}", timestamp.ordinal()));
        }
        if rest.starts_with("DD") {
            let value = if fill {
                timestamp.day().to_string()
            } else {
                format!("{:02}", timestamp.day())
            };
            take!(2, value);
        }
        if rest.starts_with("HH24") {
            let value = if fill {
                timestamp.hour().to_string()
            } else {
                format!("{:02}", timestamp.hour())
            };
            take!(4, value);
        }
        if rest.starts_with("HH12") || rest.starts_with("HH") {
            let len = if rest.starts_with("HH12") { 4 } else { 2 };
            let hour = timestamp.hour() % 12;
            let hour = if hour == 0 { 12 } else { hour };
            let value = if fill {
                hour.to_string()
            } else {
                format!("{hour:02}")
            };
            out.push_str(&value);
            rest = &rest[len..];
            continue;
        }
        if rest.starts_with("MI") {
            let value = if fill {
                timestamp.minute().to_string()
            } else {
                format!("{:02}", timestamp.minute())
            };
            take!(2, value);
        }
        if rest.starts_with("SS") {
            let value = if fill {
                timestamp.second().to_string()
            } else {
                format!("{:02}", timestamp.second())
            };
            take!(2, value);
        }
        if rest.starts_with("MS") {
            take!(
                2,
                format!("{:03}", timestamp.and_utc().timestamp_subsec_millis())
            );
        }
        if rest.starts_with("US") {
            take!(
                2,
                format!("{:06}", timestamp.and_utc().timestamp_subsec_micros())
            );
        }
        if rest.starts_with("AM") || rest.starts_with("PM") {
            let upper = rest.starts_with("PM");
            let value = if timestamp.hour() < 12 {
                if upper { "AM" } else { "am" }
            } else if upper {
                "PM"
            } else {
                "pm"
            };
            take!(2, value);
        }
        if rest.starts_with("TZ") {
            take!(
                2,
                if has_time_zone {
                    "UTC".to_string()
                } else {
                    String::new()
                }
            );
        }
        if rest.starts_with("OF") {
            take!(
                2,
                if has_time_zone {
                    "+00".to_string()
                } else {
                    String::new()
                }
            );
        }
        if rest.starts_with("Q") {
            take!(1, (((timestamp.month() - 1) / 3) + 1).to_string());
        }
        if rest.starts_with("IW") {
            take!(2, format!("{:02}", timestamp.iso_week().week()));
        }
        if rest.starts_with("WW") {
            take!(2, format!("{:02}", timestamp.ordinal0() / 7 + 1));
        }
        if rest.starts_with("W") {
            take!(1, ((timestamp.day0() / 7) + 1).to_string());
        }
        if rest.starts_with("ID") {
            take!(
                2,
                format!("{:02}", timestamp.weekday().number_from_monday())
            );
        }
        if rest.starts_with("DAY") {
            take!(
                3,
                format!("{:<9}", day_name(timestamp.weekday(), false, true))
            );
        }
        if rest.starts_with("Day") {
            take!(
                3,
                format!("{:<9}", day_name(timestamp.weekday(), false, false))
            );
        }
        if rest.starts_with("DY") {
            take!(2, day_name(timestamp.weekday(), true, true));
        }
        if rest.starts_with("Dy") {
            take!(2, day_name(timestamp.weekday(), true, false));
        }
        if rest.starts_with("CC") {
            take!(2, (((timestamp.year() - 1) / 100) + 1).to_string());
        }
        if rest.starts_with("D") {
            take!(
                1,
                (timestamp.weekday().num_days_from_sunday() + 1).to_string()
            );
        }
        let ch = rest.chars().next().expect("rest is not empty");
        out.push(ch);
        rest = &rest[ch.len_utf8()..];
    }
    out
}

fn month_name(month: u32, abbreviated: bool, upper: bool) -> String {
    const MONTHS: [&str; 12] = [
        "JANUARY",
        "FEBRUARY",
        "MARCH",
        "APRIL",
        "MAY",
        "JUNE",
        "JULY",
        "AUGUST",
        "SEPTEMBER",
        "OCTOBER",
        "NOVEMBER",
        "DECEMBER",
    ];
    let name = MONTHS[(month - 1) as usize];
    let name = if abbreviated { &name[..3] } else { name };
    if upper {
        name.to_string()
    } else {
        let mut chars = name.chars();
        match chars.next() {
            Some(first) => first.to_string() + &chars.as_str().to_ascii_lowercase(),
            None => String::new(),
        }
    }
}

fn day_name(day: chrono::Weekday, abbreviated: bool, upper: bool) -> String {
    const DAYS: [&str; 7] = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ];
    let name = DAYS[day.num_days_from_monday() as usize];
    let name = if abbreviated { &name[..3] } else { name };
    if upper {
        name.to_string()
    } else {
        let mut chars = name.chars();
        match chars.next() {
            Some(first) => first.to_string() + &chars.as_str().to_ascii_lowercase(),
            None => String::new(),
        }
    }
}

/// SQLAlchemy's PostgreSQL reflection asks for the sequence behind a serial
/// column. LakeSoul has no sequences or identity columns, so the honest
/// answer is NULL. Without this function the whole reflection query fails
/// before it can report the table's regular columns.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgGetSerialSequence;

impl ScalarUDFImpl for PgGetSerialSequence {
    fn name(&self) -> &str {
        "pg_get_serial_sequence"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable)
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgVisibility(&'static str);

impl ScalarUDFImpl for PgVisibility {
    fn name(&self) -> &str {
        self.0
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Int32], Volatility::Stable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

/// Rewrites PostgreSQL `bytea` string literals into `decode(hex, 'hex')`.
///
/// psycopg2 sends byte parameters as client-side literals:
///
/// ```sql
/// INSERT INTO t (payload) VALUES ('\x0001ff'::bytea)
/// ```
///
/// DataFusion's `Utf8 -> Binary` cast copies the literal's UTF-8 bytes, so
/// the server would store the eight characters `\x0001ff` instead of the
/// three bytes `00 01 ff`. PostgreSQL's `byteain` accepts both the hex form
/// and the escape form (`\\`, `\ooo`); decode both here so the planner only
/// sees a normal `Binary` expression.
pub(crate) fn rewrite_bytea_literals(sql: &str) -> PgWireResult<Option<String>> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("bytea")
        || (!lower.contains("::bytea") && !lower.contains("as bytea"))
    {
        return Ok(None);
    }
    let statements = match PostgresCompatibilityParser::new().parse(sql) {
        Ok(statements) => statements,
        // Leave the syntax error to the normal planning path.
        Err(_) => return Ok(None),
    };
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(statements.len());
    for mut statement in statements {
        let mut visitor = ByteaLiteralVisitor { changed: false };
        if let ControlFlow::Break(error) = VisitMut::visit(&mut statement, &mut visitor) {
            return Err(error);
        }
        changed |= visitor.changed;
        rewritten.push(statement.to_string());
    }
    if changed {
        Ok(Some(rewritten.join("; ")))
    } else {
        Ok(None)
    }
}

struct ByteaLiteralVisitor {
    changed: bool,
}

impl VisitorMut for ByteaLiteralVisitor {
    type Break = PgWireError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Cast {
            expr: inner,
            data_type: SqlDataType::Bytea,
            ..
        } = expr
        else {
            return ControlFlow::Continue(());
        };
        let Some((text, escaped)) = string_literal(inner) else {
            return ControlFlow::Continue(());
        };
        let text = text.to_string();
        match decode_bytea_literal(&text, escaped) {
            Ok(bytes) => {
                *expr = bytea_decode_expr(&bytes);
                self.changed = true;
                ControlFlow::Continue(())
            }
            Err(()) => ControlFlow::Break(invalid_bytea_literal(&text)),
        }
    }
}

fn string_literal(expr: &Expr) -> Option<(&str, bool)> {
    match expr {
        Expr::Nested(inner) => string_literal(inner),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) => Some((text, false)),
            Value::EscapedStringLiteral(text) => Some((text, true)),
            _ => None,
        },
        _ => None,
    }
}

/// PostgreSQL `byteain` for a literal's text. `escaped` distinguishes an
/// `E'...'` literal, whose backslash sequences are first interpreted by the
/// string literal parser, from a standard single-quoted literal.
fn decode_bytea_literal(text: &str, escaped: bool) -> Result<Vec<u8>, ()> {
    let text = if escaped {
        unescape_sql_literal(text)
    } else {
        text.to_string()
    };
    if let Some(hex) = text.strip_prefix("\\x") {
        if !hex.len().is_multiple_of(2) {
            return Err(());
        }
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        let encoded = hex.as_bytes();
        for pair in encoded.chunks(2) {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            bytes.push(high << 4 | low);
        }
        return Ok(bytes);
    }

    let mut bytes = Vec::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            let mut buffer = [0; 4];
            bytes.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        match chars.next() {
            Some('\\') => bytes.push(b'\\'),
            Some(first) if first.is_digit(8) => {
                let mut value = first.to_digit(8).ok_or(())?;
                for _ in 0..2 {
                    let digit = chars.next().and_then(|ch| ch.to_digit(8)).ok_or(())?;
                    value = value * 8 + digit;
                }
                bytes.push(u8::try_from(value).map_err(|_| ())?);
            }
            _ => return Err(()),
        }
    }
    Ok(bytes)
}

fn hex_digit(byte: u8) -> Result<u8, ()> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(()),
    }
}

fn unescape_sql_literal(text: &str) -> String {
    let mut unescaped = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            unescaped.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => unescaped.push('\\'),
            Some('\'') => unescaped.push('\''),
            Some('n') => unescaped.push('\n'),
            Some('r') => unescaped.push('\r'),
            Some('t') => unescaped.push('\t'),
            Some('b') => unescaped.push('\u{8}'),
            Some('f') => unescaped.push('\u{c}'),
            Some(other) => {
                unescaped.push('\\');
                unescaped.push(other);
            }
            None => unescaped.push('\\'),
        }
    }
    unescaped
}

fn bytea_decode_expr(bytes: &[u8]) -> Expr {
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Expr::Function(Function {
        name: ObjectName::from(Ident::new("decode")),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(hex).with_empty_span(),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString("hex".to_string()).with_empty_span(),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn invalid_bytea_literal(text: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "22P02".to_string(),
        format!("invalid input syntax for type bytea: \"{text}\""),
    )))
}

/// Rewrites pgjdbc's primary-key metadata query.
///
/// pgjdbc calls `information_schema._pg_expandarray(i.indkey)` to turn the
/// index key vector into one row per key column. That set-returning function
/// is not implemented here, so the query never reaches the catalog scan.
/// LakeSoul's catalog keeps the primary-key columns in the table provider, so
/// the hook answers pgjdbc's outer six-column projection directly from those
/// columns. `pg_index` is not populated for LakeSoul user tables.
pub struct JdbcPrimaryKeysHook;

fn quoted_value_after(sql: &str, marker: &str) -> Option<String> {
    let start = sql.find(marker)? + marker.len();
    let start = start + sql[start..].find('\'')? + 1;
    let end = start + sql[start..].find('\'')?;
    Some(sql[start..end].replace('\'', "''"))
}

struct PrimaryKeyRequest {
    table: String,
    schema: String,
}

fn jdbc_primary_keys_request(statement: &Statement) -> Option<PrimaryKeyRequest> {
    if !matches!(statement, Statement::Query(_)) {
        return None;
    }
    let sql = statement.to_string().to_ascii_lowercase();
    if !sql.contains("_pg_expandarray")
        || !sql.contains("i.indisprimary")
        || !sql.contains("pg_catalog.pg_index")
    {
        return None;
    }
    let table = quoted_value_after(&sql, "ct.relname = ")?;
    let schema =
        quoted_value_after(&sql, "n.nspname = ").unwrap_or_else(|| "public".to_string());
    Some(PrimaryKeyRequest { table, schema })
}

async fn jdbc_primary_keys_sql(
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<DFResult<String>> {
    let request = jdbc_primary_keys_request(statement)?;
    let table_ref = TableReference::from(format!("{}.{}", request.schema, request.table));
    let result = async move {
        let provider = session_context.table_provider(table_ref).await?;
        let provider_any: &dyn std::any::Any = provider.as_ref();
        let Some(provider) = provider_any.downcast_ref::<LakeSoulTableProvider>() else {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "relation {} is not a LakeSoul table",
                request.table
            )));
        };
        let primary_keys = provider.primary_key_columns();
        if primary_keys.is_empty() {
            return Ok(
                "SELECT NULL::text AS table_cat, NULL::text AS table_schem, \
                      NULL::text AS table_name, NULL::text AS column_name, \
                      NULL::int AS key_seq, NULL::text AS pk_name WHERE false"
                    .to_string(),
            );
        }
        let schema = request.schema.replace('\'', "''");
        let table = request.table.replace('\'', "''");
        let pk_name = format!("{table}_pkey");
        let selects = primary_keys
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let column = column.replace('\'', "''");
                format!(
                    "SELECT current_database() AS table_cat, '{schema}' AS table_schem, \
                     '{table}' AS table_name, '{column}' AS column_name, \
                     CAST({} AS INT) AS key_seq, '{pk_name}' AS pk_name",
                    index + 1
                )
            })
            .collect::<Vec<_>>();
        Ok(selects.join(" UNION ALL "))
    }
    .await;
    Some(result)
}

#[async_trait]
impl QueryHook for JdbcPrimaryKeysHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let sql = jdbc_primary_keys_sql(statement, session_context).await?;
        Some(
            async move {
                let dataframe = session_context
                    .sql(&sql.map_err(|error| PgWireError::ApiError(Box::new(error)))?)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let response =
                    encode_dataframe(dataframe, &Format::UnifiedText, Some(options))
                        .await?;
                Ok(Response::Query(response))
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let sql = jdbc_primary_keys_sql(statement, session_context).await?;
        Some(
            async move {
                let sql = sql.map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                session_context
                    .sql(&sql)
                    .await
                    .map(|dataframe| dataframe.logical_plan().clone())
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Answers SQLAlchemy's reflection constraint and index queries.
///
/// SQLAlchemy 2.0 builds two catalog queries that turn `pg_index.indkey` and
/// `indclass` into rows with `unnest()` and `generate_subscripts()`. The
/// catalog stores those columns as text (PostgreSQL's `int2vector`), which
/// DataFusion's `unnest()` cannot consume, and `pg_index`/`pg_constraint`
/// have no rows for LakeSoul user tables. The table provider still knows the
/// primary-key columns, so this hook answers the two outer projections with
/// PostgreSQL's shape. LakeSoul has no secondary indexes, foreign keys,
/// unique constraints or check constraints, so those results are empty.
pub struct SqlAlchemyCatalogHook;

#[derive(Debug, PartialEq, Eq)]
enum SqlAlchemyCatalogQuery {
    Constraint { contype: char, oids: Vec<u32> },
    Index,
}

fn sqlalchemy_catalog_query(statement: &Statement) -> Option<SqlAlchemyCatalogQuery> {
    if !matches!(statement, Statement::Query(_)) {
        return None;
    }
    let sql = statement.to_string().to_ascii_lowercase();
    if sql.contains("unnest(") && sql.contains("pg_index.indclass") {
        return Some(SqlAlchemyCatalogQuery::Index);
    }
    if sql.contains("unnest(")
        && sql.contains("pg_index.indkey")
        && sql.contains("generate_subscripts")
        && sql.contains("pg_constraint")
    {
        let contype = quoted_char_after(&sql, "contype = ")?;
        let oids = oids_after(&sql, "conrelid in (")
            .or_else(|| oids_after(&sql, "conrelid = "))?;
        return Some(SqlAlchemyCatalogQuery::Constraint { contype, oids });
    }
    None
}

fn quoted_char_after(sql: &str, marker: &str) -> Option<char> {
    let start = sql.find(marker)? + marker.len();
    let rest = &sql[start..];
    let quote = rest.find('\'')? + 1;
    rest[quote..].chars().next()
}

fn oids_after(sql: &str, marker: &str) -> Option<Vec<u32>> {
    let start = sql.find(marker)? + marker.len();
    let rest = &sql[start..];
    let list = if marker.ends_with('(') {
        &rest[..rest.find(')')?]
    } else {
        let end = rest
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(rest.len());
        &rest[..end]
    };
    let oids = list
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.parse::<u32>().ok())
        .collect::<Option<Vec<_>>>()?;
    (!oids.is_empty()).then_some(oids)
}

fn sqlalchemy_constraint_sql(
    contype: char,
    tables: &[(u32, String, Vec<String>)],
) -> String {
    if contype != 'p' {
        return sqlalchemy_empty_constraint_sql();
    }
    let selects = tables
        .iter()
        .filter(|(_, _, primary_keys)| !primary_keys.is_empty())
        .map(|(oid, table, primary_keys)| {
            let values = primary_keys
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    format!("('{}', {})", escape_sql_string(column), index + 1)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT CAST({oid} AS INT) AS conrelid, \
                 array_agg(v.column_name ORDER BY v.ord) AS cols, \
                 '{}' AS conname, CAST(NULL AS TEXT) AS description, \
                 CAST({} AS INT) AS indnkeyatts, false AS indnullsnotdistinct \
                 FROM (VALUES {values}) AS v(column_name, ord)",
                escape_sql_string(&format!("{table}_pkey")),
                primary_keys.len()
            )
        })
        .collect::<Vec<_>>();
    if selects.is_empty() {
        sqlalchemy_empty_constraint_sql()
    } else {
        selects.join(" UNION ALL ")
    }
}

fn sqlalchemy_empty_constraint_sql() -> String {
    "SELECT CAST(NULL AS INT) AS conrelid, CAST(NULL AS TEXT) AS cols, \
     CAST(NULL AS TEXT) AS conname, CAST(NULL AS TEXT) AS description, \
     CAST(NULL AS INT) AS indnkeyatts, CAST(NULL AS BOOLEAN) AS indnullsnotdistinct \
     WHERE false"
        .to_string()
}

fn sqlalchemy_index_sql() -> String {
    "SELECT CAST(NULL AS INT) AS indrelid, CAST(NULL AS TEXT) AS relname, \
     CAST(NULL AS BOOLEAN) AS indisunique, CAST(NULL AS BOOLEAN) AS has_constraint, \
     CAST(NULL AS INT) AS indoption, CAST(NULL AS TEXT) AS reloptions, \
     CAST(NULL AS TEXT) AS amname, CAST(NULL AS TEXT) AS filter_definition, \
     CAST(NULL AS INT) AS indnkeyatts, \
     CAST(NULL AS BOOLEAN) AS indnullsnotdistinct, \
     CAST(NULL AS TEXT) AS elements, CAST(NULL AS TEXT) AS elements_is_expr, \
     CAST(NULL AS TEXT) AS elements_opclass, \
     CAST(NULL AS TEXT) AS elements_opdefault WHERE false"
        .to_string()
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

async fn sqlalchemy_constraint_tables(
    oids: &[u32],
    session_context: &SessionContext,
) -> DFResult<Vec<(u32, String, Vec<String>)>> {
    if oids.is_empty() {
        return Ok(Vec::new());
    }
    let oid_list = oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let batches = session_context
        .sql(&format!(
            "SELECT CAST(oid AS BIGINT) AS oid, relname \
             FROM pg_catalog.pg_class WHERE oid IN ({oid_list})"
        ))
        .await?
        .collect()
        .await?;
    let mut tables = Vec::new();
    for batch in batches {
        let oid_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("pg_class.oid is not BIGINT".to_string())
            })?;
        let name_column = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("pg_class.relname is not TEXT".to_string())
            })?;
        for row in 0..batch.num_rows() {
            let oid = oid_column.value(row);
            if oid <= 0 || oid > i64::from(u32::MAX) {
                continue;
            }
            let oid = oid as u32;
            let table = name_column.value(row).to_string();
            let table_ref = TableReference::from(format!("public.{table}"));
            let Ok(provider) = session_context.table_provider(table_ref).await else {
                continue;
            };
            let provider_any: &dyn std::any::Any = provider.as_ref();
            let Some(provider) = provider_any.downcast_ref::<LakeSoulTableProvider>()
            else {
                continue;
            };
            tables.push((oid, table, provider.primary_key_columns().to_vec()));
        }
    }
    Ok(tables)
}

async fn sqlalchemy_catalog_sql(
    query: &SqlAlchemyCatalogQuery,
    session_context: &SessionContext,
) -> DFResult<String> {
    match query {
        SqlAlchemyCatalogQuery::Constraint { contype, oids } => {
            if *contype != 'p' {
                return Ok(sqlalchemy_empty_constraint_sql());
            }
            let tables = sqlalchemy_constraint_tables(oids, session_context).await?;
            Ok(sqlalchemy_constraint_sql(*contype, &tables))
        }
        SqlAlchemyCatalogQuery::Index => Ok(sqlalchemy_index_sql()),
    }
}

#[async_trait]
impl QueryHook for SqlAlchemyCatalogHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let query = sqlalchemy_catalog_query(statement)?;
        Some(
            async move {
                let sql = sqlalchemy_catalog_sql(&query, session_context)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let dataframe = session_context
                    .sql(&sql)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let response =
                    encode_dataframe(dataframe, &Format::UnifiedText, Some(options))
                        .await?;
                Ok(Response::Query(response))
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let query = sqlalchemy_catalog_query(statement)?;
        Some(
            async move {
                let sql = sqlalchemy_catalog_sql(&query, session_context)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                session_context
                    .sql(&sql)
                    .await
                    .map(|dataframe| dataframe.logical_plan().clone())
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Rewrites catalog-describe queries whose projections contain correlated
/// scalar subqueries.
///
/// psql `\d` and SQLAlchemy reflection both ask `pg_attribute` for columns,
/// defaults, collations and identity options. The pinned DataFusion planner
/// refuses the correlated scalar subqueries those clients put in the
/// projection ("must be aggregated to return at most one row"). LakeSoul has
/// no column defaults or identity sequences, so the correlated subqueries
/// answer NULL; replacing them with `NULL` preserves the client-visible
/// column shape while keeping the rest of the catalog query intact.
pub struct CatalogProjectionHook;

fn is_catalog_projection_query(statement: &Statement) -> bool {
    if !matches!(statement, Statement::Query(_)) {
        return false;
    }
    let lower = statement.to_string().to_ascii_lowercase();
    lower.contains("pg_attribute")
        && (lower.contains("format_type") || lower.contains("json_build_object"))
}

fn rewrite_catalog_projection(statement: &Statement) -> Statement {
    let mut statement = statement.clone();
    let mut visitor = CatalogProjectionVisitor;
    let _ = VisitMut::visit(&mut statement, &mut visitor);
    statement
}

struct CatalogProjectionVisitor;

impl CatalogProjectionVisitor {
    fn has_correlation(&self, query: &Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            let table_aliases: HashSet<String> = select
                .from
                .iter()
                .flat_map(|twj| {
                    let mut aliases = HashSet::new();
                    Self::collect_table_aliases_from_table_factor(
                        &twj.relation,
                        &mut aliases,
                    );
                    for join in &twj.joins {
                        Self::collect_table_aliases_from_table_factor(
                            &join.relation,
                            &mut aliases,
                        );
                    }
                    aliases
                })
                .collect();

            let mut has_correlation = false;
            let mut visitor =
                CorrelationCheckVisitor(&mut has_correlation, &table_aliases);
            let _ = Visit::visit(query, &mut visitor);
            has_correlation
        } else {
            false
        }
    }

    fn has_limit(&self, query: &Query) -> bool {
        query.limit_clause.is_some() || query.fetch.is_some()
    }

    fn collect_table_aliases_from_table_factor(
        table_factor: &TableFactor,
        aliases: &mut HashSet<String>,
    ) {
        if let TableFactor::Table {
            alias: Some(alias), ..
        } = table_factor
        {
            aliases.insert(alias.name.value.clone());
        }
    }
}

struct CorrelationCheckVisitor<'a>(&'a mut bool, &'a HashSet<String>);

impl Visitor for CorrelationCheckVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            }) => {
                *self.0 = true;
            }
            Expr::CompoundIdentifier(idents)
                if !idents.is_empty() && !self.1.contains(&idents[0].value) =>
            {
                *self.0 = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for CatalogProjectionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Subquery(subquery) = expr {
            if self.has_correlation(subquery) {
                *expr = Expr::Value(Value::Null.with_empty_span());
            } else if !self.has_limit(subquery) {
                subquery.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number("1".to_string(), false).with_empty_span(),
                    )),
                    offset: None,
                    limit_by: vec![],
                });
            }
        }
        ControlFlow::Continue(())
    }
}

#[async_trait]
impl QueryHook for CatalogProjectionHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if !is_catalog_projection_query(statement) {
            return None;
        }
        let sql = rewrite_catalog_projection(statement).to_string();
        Some(
            async move {
                let dataframe = session_context
                    .sql(&sql)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let response =
                    encode_dataframe(dataframe, &Format::UnifiedText, Some(options))
                        .await?;
                Ok(Response::Query(response))
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if !is_catalog_projection_query(statement) {
            return None;
        }
        let rewritten = rewrite_catalog_projection(statement);
        Some(
            async move {
                session_context
                    .state()
                    .statement_to_plan(DfStatement::Statement(Box::new(rewritten)))
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Answers psql's publication probe with an empty result.
///
/// psql 18 asks for a table's publication membership with
/// `SELECT pubname, NULL, NULL FROM pg_publication ... UNION ALL ...` when it
/// talks to a pre-15 server. DataFusion refuses the two unaliased `NULL`
/// projections ("Projections require unique expression names"), which aborted
/// `\d` before it could describe the table. LakeSoul has no publications, so
/// the honest answer is the same three-column empty result.
pub(crate) struct PgPublicationHook;

fn is_publication_probe(statement: &Statement) -> bool {
    let Statement::Query(_) = statement else {
        return false;
    };
    let sql = statement.to_string().to_ascii_lowercase();
    sql.contains("pg_publication") && sql.contains("union all")
}

fn publication_response() -> PgWireResult<Response> {
    let fields = vec![
        FieldInfo::new(
            "pubname".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "pubinsert".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "pubupdate".to_string(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
    ];
    let rows = futures::stream::empty();
    Ok(Response::Query(QueryResponse::new(
        Arc::new(fields),
        Box::pin(rows),
    )))
}

fn publication_plan() -> PgWireResult<LogicalPlan> {
    let schema = Schema::new(vec![
        Field::new("pubname", DataType::Utf8, true),
        Field::new("pubinsert", DataType::Utf8, true),
        Field::new("pubupdate", DataType::Utf8, true),
    ]);
    let schema = schema
        .to_dfschema()
        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    Ok(LogicalPlan::EmptyRelation(
        datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(schema),
        },
    ))
}

#[async_trait]
impl QueryHook for PgPublicationHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        is_publication_probe(statement).then(publication_response)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        is_publication_probe(statement).then(publication_plan)
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        is_publication_probe(statement).then(publication_response)
    }
}

/// `pg_is_in_recovery()`: the endpoint is never a standby.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgIsInRecovery;

impl ScalarUDFImpl for PgIsInRecovery {
    fn name(&self) -> &str {
        "pg_is_in_recovery"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![], Volatility::Stable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![false; args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgVersion;

impl ScalarUDFImpl for PgVersion {
    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![], Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            compatibility_version();
            args.number_rows
        ]));
        Ok(ColumnarValue::Array(array))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgRelationIsPublishable;

impl ScalarUDFImpl for PgRelationIsPublishable {
    fn name(&self) -> &str {
        "pg_relation_is_publishable"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Utf8]),
                ],
                Volatility::Stable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_postgresql_compatible() {
        let version = compatibility_version();
        assert!(version.starts_with("PostgreSQL 14.0 "), "{version}");
        assert!(version.contains("LakeSoul"), "{version}");
    }

    #[test]
    fn version_udf_has_no_arguments() {
        assert_eq!(PgVersion.name(), "version");
        assert_eq!(
            PgVersion.return_type(&[]).expect("version return type"),
            DataType::Utf8
        );
    }

    #[test]
    fn rewrites_psycopg2_bytea_literal() {
        let sql = r"INSERT INTO t (id, payload) VALUES (1, '\x0001ff'::bytea)";
        let rewritten = rewrite_bytea_literals(sql)
            .expect("rewrite")
            .expect("bytea cast is rewritten");
        assert!(rewritten.contains("decode('0001ff', 'hex')"), "{rewritten}");
        assert!(!rewritten.contains("::bytea"), "{rewritten}");
    }

    #[test]
    fn rewrites_bytea_escape_literal() {
        let sql = r"SELECT '\000\001\377'::bytea";
        let rewritten = rewrite_bytea_literals(sql)
            .expect("rewrite")
            .expect("bytea cast is rewritten");
        assert!(rewritten.contains("decode('0001ff', 'hex')"), "{rewritten}");
    }

    #[test]
    fn rewrites_cast_bytea_literal() {
        let sql = r"SELECT CAST('\x00ff' AS bytea)";
        let rewritten = rewrite_bytea_literals(sql)
            .expect("rewrite")
            .expect("bytea cast is rewritten");
        assert!(rewritten.contains("decode('00ff', 'hex')"), "{rewritten}");
    }

    #[test]
    fn leaves_non_literal_bytea_alone() {
        for sql in [
            "CREATE TABLE t (payload bytea)",
            "SELECT payload FROM t",
            "SELECT payload::bytea FROM t",
        ] {
            assert!(
                rewrite_bytea_literals(sql).expect("rewrite").is_none(),
                "{sql}"
            );
        }
    }

    #[test]
    fn invalid_bytea_literal_is_22p02() {
        let error = rewrite_bytea_literals(r"SELECT '\x0'::bytea")
            .expect_err("invalid hex must fail");
        match error {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "22P02");
                assert!(
                    info.message.contains("invalid input syntax for type bytea"),
                    "{}",
                    info.message
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn detects_sqlalchemy_reflection_query() {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

        let sql = "SELECT pg_catalog.pg_attribute.attname AS name, \
            (SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a') \
             FROM pg_catalog.pg_sequence WHERE pg_catalog.pg_attribute.attidentity != '') AS identity_options \
            FROM pg_catalog.pg_class LEFT JOIN pg_catalog.pg_attribute \
            ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid";
        let statement = PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse SQLAlchemy reflection query")
            .remove(0);
        assert!(is_catalog_projection_query(&statement));
        let rewritten = rewrite_catalog_projection(&statement).to_string();
        assert!(!rewritten.contains("json_build_object"), "{rewritten}");
    }

    #[test]
    fn rewrites_psql_describe_columns_query() {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

        let sql = "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
            (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d \
             WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
            a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
             WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
            a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a \
            WHERE a.attrelid = '16455' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";
        let statement = PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse psql query")
            .remove(0);
        assert!(is_catalog_projection_query(&statement));
        let rewritten = rewrite_catalog_projection(&statement).to_string();
        assert!(
            !rewritten.to_ascii_lowercase().contains("pg_attrdef"),
            "{rewritten}"
        );
        assert!(
            rewritten.to_ascii_lowercase().contains("null"),
            "{rewritten}"
        );
    }

    fn parse_statement(sql: &str) -> Statement {
        PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse SQLAlchemy metadata query")
            .remove(0)
    }

    #[test]
    fn detects_sqlalchemy_constraint_query() {
        let statement = parse_statement(
            "SELECT attr.conrelid, array_agg(attr.attname ORDER BY attr.ord) AS cols \
             FROM (SELECT con.conrelid AS conrelid, con.ord AS ord, \
                          pg_catalog.pg_attribute.attname AS attname \
                   FROM pg_catalog.pg_attribute JOIN ( \
                       SELECT pg_catalog.pg_constraint.conrelid AS conrelid, \
                              unnest(pg_catalog.pg_index.indkey) AS attnum, \
                              generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord \
                       FROM pg_catalog.pg_constraint \
                       JOIN pg_catalog.pg_index \
                         ON pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid \
                       WHERE pg_catalog.pg_constraint.contype = 'p' \
                         AND pg_catalog.pg_constraint.conrelid IN (16464)) AS con \
                     ON pg_catalog.pg_attribute.attnum = con.attnum) AS attr \
             GROUP BY attr.conrelid, attr.conname \
             ORDER BY attr.conrelid, attr.conname",
        );
        assert_eq!(
            sqlalchemy_catalog_query(&statement),
            Some(SqlAlchemyCatalogQuery::Constraint {
                contype: 'p',
                oids: vec![16464],
            })
        );
    }

    #[test]
    fn detects_sqlalchemy_index_query() {
        let statement = parse_statement(
            "SELECT pg_catalog.pg_index.indrelid, \
                    unnest(pg_catalog.pg_index.indclass) AS att_opclass \
             FROM pg_catalog.pg_index \
             WHERE NOT pg_catalog.pg_index.indisprimary \
               AND pg_catalog.pg_index.indrelid IN (16464)",
        );
        assert_eq!(
            sqlalchemy_catalog_query(&statement),
            Some(SqlAlchemyCatalogQuery::Index)
        );
    }

    #[test]
    fn builds_sqlalchemy_primary_key_rows() {
        let sql = sqlalchemy_constraint_sql(
            'p',
            &[(
                16464,
                "refl_probe".to_string(),
                vec!["tenant".to_string(), "id".to_string()],
            )],
        );
        assert!(
            sql.contains("array_agg(v.column_name ORDER BY v.ord) AS cols"),
            "{sql}"
        );
        assert!(sql.contains("('tenant', 1), ('id', 2)"), "{sql}");
        assert!(sql.contains("'refl_probe_pkey'"), "{sql}");
        assert!(sql.contains("CAST(2 AS INT) AS indnkeyatts"), "{sql}");
    }

    #[test]
    fn sqlalchemy_empty_results_keep_the_expected_columns() {
        let constraint = sqlalchemy_empty_constraint_sql();
        for column in [
            "conrelid",
            "cols",
            "conname",
            "description",
            "indnkeyatts",
            "indnullsnotdistinct",
        ] {
            assert!(constraint.contains(&format!("AS {column}")), "{constraint}");
        }
        let index = sqlalchemy_index_sql();
        for column in [
            "indrelid",
            "relname",
            "indisunique",
            "has_constraint",
            "indoption",
            "reloptions",
            "amname",
            "filter_definition",
            "indnkeyatts",
            "indnullsnotdistinct",
            "elements",
            "elements_is_expr",
            "elements_opclass",
            "elements_opdefault",
        ] {
            assert!(index.contains(&format!("AS {column}")), "{index}");
        }
    }
}

/// PostgreSQL's `to_regtype(text)`: resolve a type name to its OID.
///
/// Drivers use it to discover extension types before registering their
/// adapters - `pgvector`'s `psycopg` integration issues
/// `... WHERE t.oid = to_regtype('vector')` from `TypeInfo.fetch`.  DataFusion
/// has no `regtype` function for calls like that, so this shim resolves the
/// names the pgvector clients ask for (`vector`, `bit`, ...) plus the common
/// built-in types, returning `NULL` for unknown names exactly like PostgreSQL
/// does for `to_regtype`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ToRegtype;

/// Type-name to OID map for [`ToRegtype`].
///
/// Values are PostgreSQL's built-in type OIDs; `vector` is the OID
/// `datafusion-postgres` advertises in `pg_catalog.pg_type`
/// (`PG_VECTOR_TYPE_OID`).  Unknown names return `NULL` instead of searching
/// `pg_type`: this shim never runs SQL, and the drivers only ask for the types
/// they know how to decode.
static REGTYPE_OIDS: LazyLock<HashMap<&'static str, i32>> = LazyLock::new(|| {
    let mut oids: HashMap<&'static str, i32> = HashMap::from([
        ("bool", 16),
        ("boolean", 16),
        ("bytea", 17),
        ("char", 18),
        ("name", 19),
        ("int8", 20),
        ("bigint", 20),
        ("int2", 21),
        ("smallint", 21),
        ("int4", 23),
        ("int", 23),
        ("integer", 23),
        ("text", 25),
        ("oid", 26),
        ("json", 114),
        ("float4", 700),
        ("real", 700),
        ("float8", 701),
        ("double precision", 701),
        ("varchar", 1043),
        ("character varying", 1043),
        ("date", 1082),
        ("time", 1083),
        ("timestamp", 1114),
        ("timestamp without time zone", 1114),
        ("timestamptz", 1184),
        ("timestamp with time zone", 1184),
        ("interval", 1186),
        ("timetz", 1266),
        ("bit", 1560),
        ("varbit", 1562),
        ("bit varying", 1562),
        ("numeric", 1700),
        ("decimal", 1700),
        ("uuid", 2950),
        ("jsonb", 3802),
        ("bpchar", 1042),
        ("character", 1042),
        ("inet", 869),
        ("money", 790),
        // Array type names clients use for reflection.
        ("bool[]", 1000),
        ("boolean[]", 1000),
        ("bytea[]", 1001),
        ("int2[]", 1005),
        ("smallint[]", 1005),
        ("int4[]", 1007),
        ("integer[]", 1007),
        ("int8[]", 1016),
        ("bigint[]", 1016),
        ("text[]", 1009),
        ("bpchar[]", 1014),
        ("varchar[]", 1015),
        ("character varying[]", 1015),
        ("float4[]", 1021),
        ("real[]", 1021),
        ("float8[]", 1022),
        ("double precision[]", 1022),
        ("numeric[]", 1231),
        ("decimal[]", 1231),
        ("date[]", 1182),
        ("time[]", 1183),
        ("timestamp[]", 1115),
        ("timestamptz[]", 1185),
        ("interval[]", 1187),
        ("json[]", 199),
        ("jsonb[]", 3807),
        ("uuid[]", 2951),
        ("money[]", 791),
    ]);
    oids.insert(
        "vector",
        datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID as i32,
    );
    oids
});

/// Normalize a type name the way PostgreSQL resolves unquoted identifiers:
/// lowercase and ignore whitespace; strip a `pg_catalog.` qualifier and
/// surrounding double quotes.
fn normalize_type_name(name: &str) -> String {
    let trimmed = name.trim().trim_matches('"').to_lowercase();
    let trimmed = trimmed.strip_prefix("pg_catalog.").unwrap_or(&trimmed);
    trimmed.trim().trim_matches('"').to_string()
}

impl ScalarUDFImpl for ToRegtype {
    fn name(&self) -> &str {
        "to_regtype"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Null]),
                ],
                Volatility::Stable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // PostgreSQL's `regtype` is an oid; `pg_type.oid` is Arrow Int32 in
        // the catalog, so a plain Int32 compares and displays correctly here.
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let argument = args
            .args
            .first()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "to_regtype expects one argument".to_string(),
                )
            })?
            .clone();
        let values: Vec<Option<i32>> = match &argument {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Utf8(Some(name))
                | ScalarValue::LargeUtf8(Some(name))
                | ScalarValue::Utf8View(Some(name)) => vec![resolve(name)],
                ScalarValue::Null
                | ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::Utf8View(None) => vec![None],
                other => {
                    return Err(datafusion::error::DataFusionError::Plan(format!(
                        "to_regtype expects text, got {other:?}"
                    )));
                }
            },
            ColumnarValue::Array(array) => {
                let array = array.clone();
                if array.data_type() == &DataType::Null {
                    vec![None; args.number_rows]
                } else {
                    let names = downcast_strings(array.as_ref())?;
                    (0..names.len())
                        .map(|index| names[index].as_deref().and_then(resolve))
                        .collect()
                }
            }
        };
        Ok(ColumnarValue::Array(Arc::new(Int32Array::from(values))))
    }
}

/// Resolve one type name to its OID via [`REGTYPE_OIDS`].
fn resolve(name: &str) -> Option<i32> {
    REGTYPE_OIDS
        .get(normalize_type_name(name).as_str())
        .copied()
}

/// Borrow the UTF-8 values of any Arrow string array `to_regtype` accepts.
fn downcast_strings(array: &dyn Array) -> DFResult<Vec<Option<String>>> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((0..values.len())
            .map(|index| {
                (!values.is_null(index)).then(|| values.value(index).to_string())
            })
            .collect());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((0..values.len())
            .map(|index| {
                (!values.is_null(index)).then(|| values.value(index).to_string())
            })
            .collect());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok((0..values.len())
            .map(|index| {
                (!values.is_null(index)).then(|| values.value(index).to_string())
            })
            .collect());
    }
    Err(datafusion::error::DataFusionError::Plan(format!(
        "to_regtype expects text, got {:?}",
        array.data_type()
    )))
}

#[cfg(test)]
mod to_char_tests {
    use super::*;

    fn timestamp() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, 5)
            .unwrap()
    }

    #[test]
    fn formats_postgres_patterns() {
        assert_eq!(
            pg_format_timestamp(timestamp(), false, "YYYY-MM-DD HH24:MI:SS"),
            "2024-01-02 03:04:05"
        );
        assert_eq!(
            pg_format_timestamp(timestamp(), false, "DD Mon YYYY"),
            "02 Jan 2024"
        );
        assert_eq!(
            pg_format_timestamp(timestamp(), false, "FMMM/FMDD/YYYY"),
            "1/2/2024"
        );
        assert_eq!(
            pg_format_timestamp(timestamp(), true, "YYYY TZ OF"),
            "2024 UTC +00"
        );
    }
}

#[cfg(test)]
mod to_regtype_tests {
    use super::*;

    #[test]
    fn resolves_pgvector_and_builtin_type_names() {
        assert_eq!(
            resolve("vector"),
            Some(datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID as i32)
        );
        assert_eq!(resolve("VECTOR"), resolve("vector"));
        assert_eq!(resolve(" pg_catalog.vector "), resolve("vector"));
        assert_eq!(resolve("bit"), Some(1560));
        assert_eq!(resolve("int4"), Some(23));
        assert_eq!(resolve("no_such_type"), None);
    }
}
