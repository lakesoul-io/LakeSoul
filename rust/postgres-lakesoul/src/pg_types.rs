// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL type metadata and DataFusion type planning.
//!
//! LakeSoul stores every value in an Arrow column. A few PostgreSQL types do
//! not have a distinct Arrow physical type (`uuid`, `varchar`, `timestamp(p)`,
//! arrays), so the semantic type is carried in field metadata. The wire layer
//! and `pg_catalog` read that metadata; the DDL and ALTER paths write it.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::sql::sqlparser::ast::{
    ArrayElemTypeDef, CharacterLength, DataType as SqlDataType, ExactNumberInfo,
    TimezoneInfo,
};
use datafusion_postgres::pgwire::error::PgWireResult;

use crate::ddl::{feature_not_supported, pg_json_kind, with_json_metadata};

/// Field metadata key carrying the semantic PostgreSQL type.
pub(crate) const PG_TYPE_KEY: &str = "pg.type";
/// Field metadata key carrying PostgreSQL's `atttypmod`.
pub(crate) const PG_TYPMOD_KEY: &str = "pg.typmod";
/// Marks a one-dimensional semantic array; the element field carries the type.
pub(crate) const PG_ARRAY_KEY: &str = "pg.array";

/// The semantic PostgreSQL type stored on a field, if any.
pub(crate) fn pg_type_name(field: &Field) -> Option<&str> {
    field.metadata().get(PG_TYPE_KEY).map(String::as_str)
}

/// The PostgreSQL `atttypmod` stored on a field, if any.
pub(crate) fn pg_typmod(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PG_TYPMOD_KEY)
        .and_then(|value| value.parse().ok())
}

/// The character length declared by a SQL character type, if any.
fn character_length(length: &Option<CharacterLength>) -> PgWireResult<Option<u64>> {
    match length {
        None => Ok(None),
        Some(CharacterLength::IntegerLength { length, unit }) => {
            if let Some(unit) = unit {
                use datafusion::sql::sqlparser::ast::CharLengthUnits;
                if !matches!(unit, CharLengthUnits::Characters) {
                    return Err(feature_not_supported(
                        "character length units other than CHARACTERS are not supported"
                            .to_string(),
                    ));
                }
            }
            Ok(Some(*length))
        }
        Some(CharacterLength::Max) => Err(feature_not_supported(
            "VARCHAR(MAX)/CHAR(MAX) is not supported".to_string(),
        )),
    }
}

/// Build a field for a SQL type, including the semantic metadata used by the
/// wire layer. `arrow_type` supplies the physical representation.
pub(crate) fn field_for_sql_type(
    name: &str,
    data_type: &SqlDataType,
    nullable: bool,
) -> PgWireResult<Field> {
    if let Some(kind) = pg_json_kind(data_type) {
        return Ok(with_json_metadata(
            Field::new(name, DataType::Utf8, nullable),
            kind,
        ));
    }

    match data_type {
        SqlDataType::Array(ArrayElemTypeDef::SquareBracket(inner, None)) => {
            let element = field_for_sql_type("item", inner, true)?;
            let mut field = Field::new(name, DataType::List(Arc::new(element)), nullable);
            field = with_metadata(field, PG_ARRAY_KEY, "1");
            return Ok(field);
        }
        SqlDataType::Array(ArrayElemTypeDef::SquareBracket(_, Some(_))) => {
            return Err(feature_not_supported(
                "fixed-size array columns are not supported; use a one-dimensional array"
                    .to_string(),
            ));
        }
        SqlDataType::Array(_) => {
            return Err(feature_not_supported(
                "only one-dimensional PostgreSQL array columns are supported".to_string(),
            ));
        }
        _ => {}
    }

    let arrow = crate::ddl::arrow_type(data_type)?;
    let mut field = Field::new(name, arrow, nullable);
    match data_type {
        SqlDataType::Uuid => {
            // UUID is stored as canonical text. Arrow's canonical UUID
            // extension requires FixedSizeBinary(16), but DataFusion's string
            // literal/cast path cannot produce that representation without a
            // custom cast; text keeps DDL/INSERT/COPY readable by every
            // engine while the marker still reports the UUID OID.
            field = with_metadata(field, PG_TYPE_KEY, "uuid");
        }
        SqlDataType::Time(precision, timezone) => {
            let typmod = precision.map(|value| value as i32).unwrap_or(-1);
            field = with_typmod(field, "time", typmod);
            if !matches!(timezone, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) {
                return Err(feature_not_supported(
                    "TIME WITH TIME ZONE is not supported".to_string(),
                ));
            }
        }
        SqlDataType::Varchar(length)
        | SqlDataType::CharacterVarying(length)
        | SqlDataType::CharVarying(length) => {
            let length = character_length(length)?;
            if length.is_some_and(|length| length > (i32::MAX - 4) as u64) {
                return Err(feature_not_supported(
                    "character length is too large".to_string(),
                ));
            }
            let typmod = length.map(|length| length as i32 + 4).unwrap_or(-1);
            field = with_typmod(field, "varchar", typmod);
        }
        SqlDataType::Char(length) | SqlDataType::Character(length) => {
            let length = character_length(length)?.unwrap_or(1);
            if length > (i32::MAX - 4) as u64 {
                return Err(feature_not_supported(
                    "character length is too large".to_string(),
                ));
            }
            field = with_typmod(field, "bpchar", length as i32 + 4);
        }
        SqlDataType::Numeric(info)
        | SqlDataType::Decimal(info)
        | SqlDataType::Dec(info) => {
            if let Some((precision, scale)) = numeric_info(info)
                && precision <= 76
                && (0..=precision as i64).contains(&scale)
            {
                field = with_typmod(
                    field,
                    "numeric",
                    numeric_typmod(precision as u8, scale as i8),
                );
            }
        }
        SqlDataType::Timestamp(precision, timezone) => {
            let typmod = precision.map(|value| value as i32).unwrap_or(-1);
            let kind = match timezone {
                TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => "timestamptz",
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => "timestamp",
            };
            field = with_typmod(field, kind, typmod);
        }
        SqlDataType::Custom(name, _) if custom_name(name).as_deref() == Some("money") => {
            field = with_metadata(field, PG_TYPE_KEY, "money");
        }
        _ => {}
    }
    Ok(field)
}

fn with_metadata(mut field: Field, key: &str, value: &str) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(key.to_string(), value.to_string());
    field = field.with_metadata(metadata);
    field
}

fn with_typmod(field: Field, kind: &str, typmod: i32) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(PG_TYPE_KEY.to_string(), kind.to_string());
    if typmod >= 0 {
        metadata.insert(PG_TYPMOD_KEY.to_string(), typmod.to_string());
    }
    field.with_metadata(metadata)
}

fn numeric_info(info: &ExactNumberInfo) -> Option<(u64, i64)> {
    match info {
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            Some((*precision, *scale))
        }
        ExactNumberInfo::Precision(precision) => Some((*precision, 0)),
        ExactNumberInfo::None => None,
    }
}

/// PostgreSQL's packed `numeric(p,s)` typmod.
fn numeric_typmod(precision: u8, scale: i8) -> i32 {
    (((precision as i32) << 16) | (scale as u16 as i32)) + 4
}

fn custom_name(name: &datafusion::sql::sqlparser::ast::ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.trim_matches('"').to_ascii_lowercase())
}

/// DataFusion `TypePlanner` for PostgreSQL-specific SQL types.
///
/// DataFusion holds one planner per session, so this one must also serve the
/// types the `pg_catalog` setup installed `PgOidTypePlanner` for (json,
/// pgvector and oid aliases): everything not owned here is delegated to it.
/// The session state does not expose the installed planner for reading, so
/// [`install`] constructs the same planner again.
pub(crate) struct PgTypePlanner {
    inner: Option<Arc<dyn datafusion::logical_expr::planner::TypePlanner>>,
}

impl std::fmt::Debug for PgTypePlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTypePlanner").finish()
    }
}

impl datafusion::logical_expr::planner::TypePlanner for PgTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> DFResult<Option<FieldRef>> {
        if !is_pg_specific(sql_type) {
            return match &self.inner {
                Some(inner) => inner.plan_type_field(sql_type),
                None => Ok(None),
            };
        }
        field_for_sql_type("", sql_type, true)
            .map(|field| Some(Arc::new(field)))
            .map_err(|error| DataFusionError::Plan(error.to_string()))
    }
}

fn is_pg_specific(data_type: &SqlDataType) -> bool {
    matches!(
        data_type,
        SqlDataType::Uuid
            | SqlDataType::Time(Some(_), _)
            | SqlDataType::Array(_)
            | SqlDataType::Varchar(_)
            | SqlDataType::Char(_)
            | SqlDataType::Character(_)
            | SqlDataType::CharacterVarying(_)
            | SqlDataType::CharVarying(_)
    ) || matches!(
        data_type,
        SqlDataType::Numeric(info)
            | SqlDataType::Decimal(info)
            | SqlDataType::Dec(info)
            if !matches!(info, ExactNumberInfo::None)
    ) || matches!(
        data_type,
        SqlDataType::Custom(name, args)
            if args.is_empty()
                && matches!(custom_name(name).as_deref(), Some("money" | "inet"))
    )
}

/// Install the PostgreSQL type planner on an existing session state.
pub(crate) fn install(context: &datafusion::prelude::SessionContext) -> DFResult<()> {
    use datafusion::execution::SessionStateBuilder;

    let existing = context.state_ref().read().clone();
    let inner: Arc<dyn datafusion::logical_expr::planner::TypePlanner> =
        Arc::new(
            datafusion_postgres::datafusion_pg_catalog::pg_catalog::oid_type_planner::PgOidTypePlanner,
        );
    let planner = Arc::new(PgTypePlanner { inner: Some(inner) });
    let new_state = SessionStateBuilder::new_from_existing(existing)
        .with_type_planner(planner)
        .build();
    *context.state_ref().write() = new_state;
    Ok(())
}

/// Convert a SQL type to the Arrow physical type. This is the single source of
/// truth used by DDL, ALTER and the type planner.
pub(crate) fn arrow_type(data_type: &SqlDataType) -> PgWireResult<DataType> {
    let ty = match data_type {
        SqlDataType::Uuid => DataType::Utf8,
        SqlDataType::Time(precision, timezone) => {
            if !matches!(timezone, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) {
                return Err(feature_not_supported(
                    "TIME WITH TIME ZONE is not supported".to_string(),
                ));
            }
            match precision.unwrap_or(6) {
                0 => DataType::Time32(TimeUnit::Second),
                3 => DataType::Time32(TimeUnit::Millisecond),
                6 => DataType::Time64(TimeUnit::Microsecond),
                value => {
                    return Err(feature_not_supported(format!(
                        "TIME({value}) is not supported: Arrow represents seconds, \
                         milliseconds or microseconds"
                    )));
                }
            }
        }
        SqlDataType::Interval { .. } => {
            // Arrow has an in-memory interval type, but neither Parquet nor
            // Vortex can persist Interval(MonthDayNano). Keep interval
            // expressions working; do not accept a column that cannot be read
            // back after a restart.
            return Err(feature_not_supported(
                "INTERVAL columns are not supported: the file formats cannot persist Arrow intervals"
                    .to_string(),
            ));
        }
        SqlDataType::Array(ArrayElemTypeDef::SquareBracket(inner, None)) => {
            DataType::List(Arc::new(Field::new("item", arrow_type(inner)?, true)))
        }
        SqlDataType::Array(_) => {
            return Err(feature_not_supported(
                "only one-dimensional PostgreSQL arrays are supported".to_string(),
            ));
        }
        SqlDataType::Custom(name, args) if args.is_empty() => {
            match custom_name(name).as_deref() {
                Some("money") => DataType::Decimal128(19, 2),
                Some("inet") => {
                    return Err(feature_not_supported(
                        "INET is not supported: Arrow has no inet type".to_string(),
                    ));
                }
                _ => return old_arrow_type(data_type),
            }
        }
        SqlDataType::Numeric(info)
        | SqlDataType::Decimal(info)
        | SqlDataType::Dec(info) => {
            let Some((precision, scale)) = numeric_info(info) else {
                return Err(feature_not_supported(
                    "NUMERIC requires an explicit precision and scale".to_string(),
                ));
            };
            if precision == 0 || precision > 76 {
                return Err(feature_not_supported(
                    "NUMERIC precision must be within 1..=76".to_string(),
                ));
            }
            if scale < 0 || scale as u64 > precision {
                return Err(feature_not_supported(
                    "NUMERIC scale must be within 0..=precision".to_string(),
                ));
            }
            if precision <= 38 {
                DataType::Decimal128(precision as u8, scale as i8)
            } else {
                DataType::Decimal256(precision as u8, scale as i8)
            }
        }
        SqlDataType::Timestamp(precision, timezone) => {
            let unit = match precision.unwrap_or(6) {
                0 => TimeUnit::Second,
                3 => TimeUnit::Millisecond,
                6 => TimeUnit::Microsecond,
                value => {
                    return Err(feature_not_supported(format!(
                        "TIMESTAMP({value}) is not supported: Arrow represents \
                         seconds, milliseconds or microseconds"
                    )));
                }
            };
            let tz = match timezone {
                TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => {
                    Some(Arc::<str>::from("UTC"))
                }
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
            };
            DataType::Timestamp(unit, tz)
        }
        _ => return old_arrow_type(data_type),
    };
    Ok(ty)
}

fn old_arrow_type(data_type: &SqlDataType) -> PgWireResult<DataType> {
    crate::ddl::arrow_type_legacy(data_type)
}
