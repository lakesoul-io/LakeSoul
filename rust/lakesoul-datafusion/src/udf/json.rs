// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL JSON functions and operators over the text representation.
//!
//! JSON columns are stored as UTF-8 text with the Arrow canonical `arrow.json`
//! extension. The functions below parse that text with `serde_json` and
//! implement the PostgreSQL operators whose results the PostgreSQL wire layer
//! can encode:
//!
//! * `->` / `#>` return the nested document as JSON text;
//! * `->>` / `#>>` return the value as SQL text;
//! * `?` tests whether a key or array element exists.
//!
//! `json_get_json`, `json_as_text`, `json_contains`, `json_typeof`,
//! `json_array_length`, `json_object_keys` and `json_valid` are also available
//! as functions. Results that represent JSON carry the canonical extension and
//! copy the input's `pg.json` marker, so a `jsonb` input yields a `jsonb`
//! result on the wire.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanBuilder, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DFSchema, Result as DFResult, exec_err, plan_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

const ARROW_JSON_EXTENSION_NAME: &str = "arrow.json";
const PG_JSON_KEY: &str = "pg.json";

/// Every JSON scalar function, in registration order.
pub fn all_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        json_get_json_udf(),
        json_as_text_udf(),
        json_contains_udf(),
        json_typeof_udf(),
        json_array_length_udf(),
        json_object_keys_udf(),
        json_valid_udf(),
    ]
}

/// Register the JSON scalar functions and the PG-style JSON operators.
pub fn register(context: &SessionContext) -> DFResult<()> {
    for udf in all_udfs() {
        context.register_udf((*udf).clone());
    }
    // `SessionContext::register_expr_planner` needs `&mut self`; the connection
    // context is shared, so append to the state the way the pgvector installer
    // does.
    let state = context.state();
    let mut planners = state.expr_planners().to_vec();
    planners.push(Arc::new(PgJsonExprPlanner));
    let state_ref = context.state_ref();
    let existing = state_ref.read().clone();
    let new_state = SessionStateBuilder::new_from_existing(existing)
        .with_expr_planners(planners)
        .build();
    *state_ref.write() = new_state;
    Ok(())
}

// ---------------------------------------------------------------------------
// Path handling
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum PathElem {
    Key(String),
    Index(i64),
}

/// One row's path, or `None` when any path argument is SQL NULL.
fn path_for_row(args: &[ColumnarValue], row: usize) -> DFResult<Option<Vec<PathElem>>> {
    let mut path = Vec::new();
    for arg in args {
        let scalar = match arg {
            ColumnarValue::Scalar(scalar) => scalar.clone(),
            ColumnarValue::Array(array) => {
                ScalarValue::try_from_array(array.as_ref(), row)?
            }
        };
        if !append_path_scalar(&mut path, scalar)? {
            return Ok(None);
        }
    }
    Ok(Some(path))
}

/// Returns `false` when the scalar is a SQL NULL path element.
fn append_path_scalar(path: &mut Vec<PathElem>, scalar: ScalarValue) -> DFResult<bool> {
    match scalar {
        ScalarValue::Null => Ok(false),
        ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None) => Ok(false),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => {
            path.push(PathElem::Key(value));
            Ok(true)
        }
        ScalarValue::Int8(Some(value)) => {
            path.push(PathElem::Index(value as i64));
            Ok(true)
        }
        ScalarValue::Int16(Some(value)) => {
            path.push(PathElem::Index(value as i64));
            Ok(true)
        }
        ScalarValue::Int32(Some(value)) => {
            path.push(PathElem::Index(value as i64));
            Ok(true)
        }
        ScalarValue::Int64(Some(value)) => {
            path.push(PathElem::Index(value));
            Ok(true)
        }
        ScalarValue::UInt8(Some(value)) => {
            path.push(PathElem::Index(value as i64));
            Ok(true)
        }
        ScalarValue::UInt16(Some(value)) => {
            path.push(PathElem::Index(value as i64));
            Ok(true)
        }
        ScalarValue::UInt32(Some(value)) => {
            path.push(PathElem::Index(value as i64));
            Ok(true)
        }
        ScalarValue::UInt64(Some(value)) => {
            let value = i64::try_from(value).map_err(|_| {
                datafusion::common::DataFusionError::Execution(
                    "JSON array index is out of range".to_string(),
                )
            })?;
            path.push(PathElem::Index(value));
            Ok(true)
        }
        ScalarValue::List(list) => {
            let values = list.value(0);
            for index in 0..values.len() {
                let scalar = ScalarValue::try_from_array(values.as_ref(), index)?;
                if !append_path_scalar(path, scalar)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        ScalarValue::LargeList(list) => {
            let values = list.value(0);
            for index in 0..values.len() {
                let scalar = ScalarValue::try_from_array(values.as_ref(), index)?;
                if !append_path_scalar(path, scalar)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        ScalarValue::FixedSizeList(list) => {
            let values = list.value(0);
            for index in 0..values.len() {
                let scalar = ScalarValue::try_from_array(values.as_ref(), index)?;
                if !append_path_scalar(path, scalar)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        other => exec_err!("unsupported JSON path element: {other:?}"),
    }
}

fn json_value_at<'a>(
    value: &'a serde_json::Value,
    path: &[PathElem],
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for elem in path {
        match (elem, current) {
            (PathElem::Key(key), serde_json::Value::Object(map)) => {
                current = map.get(key)?;
            }
            (PathElem::Index(index), serde_json::Value::Array(values)) => {
                let index = if *index < 0 {
                    values.len() as i64 + *index
                } else {
                    *index
                };
                if index < 0 {
                    return None;
                }
                current = values.get(index as usize)?;
            }
            // A text path element reaching an array is an index, as in
            // PostgreSQL's `#>` / `#>>`: `ARRAY['items', '0', 'name']`.
            (PathElem::Key(key), serde_json::Value::Array(values)) => {
                let index = key.parse::<i64>().ok()?;
                let index = if index < 0 {
                    values.len() as i64 + index
                } else {
                    index
                };
                if index < 0 {
                    return None;
                }
                current = values.get(index as usize)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// The JSON text used by `->`, `#>`, and the JSON-returning functions.
fn json_text(value: &serde_json::Value) -> DFResult<String> {
    serde_json::to_string(value).map_err(|error| {
        datafusion::common::DataFusionError::Execution(format!(
            "could not serialize JSON value: {error}"
        ))
    })
}

/// The SQL text used by `->>` and `#>>`: strings lose their quotes, JSON null
/// becomes SQL NULL, and every other value keeps its JSON text.
fn value_text(value: &serde_json::Value) -> DFResult<Option<String>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(text) => Ok(Some(text.clone())),
        other => json_text(other).map(Some),
    }
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

fn json_field_with_input_kind(input: Option<&FieldRef>) -> Field {
    let kind = input
        .and_then(|field| field.metadata().get(PG_JSON_KEY))
        .cloned()
        .unwrap_or_else(|| "json".to_string());
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "ARROW:extension:name".to_string(),
        ARROW_JSON_EXTENSION_NAME.to_string(),
    );
    metadata.insert(PG_JSON_KEY.to_string(), kind);
    Field::new("", DataType::Utf8, true).with_metadata(metadata)
}

/// A variadic JSON accessor returning UTF-8 text.
struct JsonAccessor {
    name: &'static str,
    as_text: bool,
    signature: Signature,
}

impl std::fmt::Debug for JsonAccessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonAccessor")
            .field("name", &self.name)
            .finish()
    }
}

impl PartialEq for JsonAccessor {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.as_text == other.as_text
    }
}

impl Eq for JsonAccessor {}

impl std::hash::Hash for JsonAccessor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.as_text.hash(state);
    }
}

impl ScalarUDFImpl for JsonAccessor {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        if self.as_text {
            return Ok(Arc::new(Field::new(self.name, DataType::Utf8, true)));
        }
        Ok(Arc::new(
            json_field_with_input_kind(args.arg_fields.first()).with_name(self.name),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.is_empty() {
            return plan_err!("{} requires a JSON document", self.name);
        }
        let json = args.args[0].clone().into_array(args.number_rows)?;
        let mut builder =
            StringBuilder::with_capacity(args.number_rows, args.number_rows * 16);
        for row in 0..args.number_rows {
            let Some(path) = path_for_row(&args.args[1..], row)? else {
                builder.append_null();
                continue;
            };
            let Some(document) = string_at(json.as_ref(), row)? else {
                builder.append_null();
                continue;
            };
            let value: serde_json::Value = match serde_json::from_str(&document) {
                Ok(value) => value,
                Err(error) => {
                    return exec_err!(
                        "invalid input syntax for type json in {}: {error}",
                        self.name
                    );
                }
            };
            let Some(value) = json_value_at(&value, &path) else {
                builder.append_null();
                continue;
            };
            if self.as_text {
                match value_text(value)? {
                    Some(text) => builder.append_value(text),
                    None => builder.append_null(),
                }
            } else {
                builder.append_value(json_text(value)?);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn string_at(array: &dyn Array, row: usize) -> DFResult<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .map(|array| array.value(row).to_string())),
        DataType::Utf8View => Ok(array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
            .map(|array| array.value(row).to_string())),
        DataType::LargeUtf8 => Ok(array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
            .map(|array| array.value(row).to_string())),
        other => exec_err!("JSON argument must be text, got {other}"),
    }
}

fn json_get_json_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonAccessor {
        name: "json_get_json",
        as_text: false,
        signature: Signature::variadic_any(Volatility::Immutable),
    }))
}

fn json_as_text_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonAccessor {
        name: "json_as_text",
        as_text: true,
        signature: Signature::variadic_any(Volatility::Immutable),
    }))
}

// -- json_contains ----------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonContains {
    signature: Signature,
}

impl JsonContains {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonContains {
    fn name(&self) -> &str {
        "json_contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() < 2 {
            return plan_err!("json_contains requires a JSON document and a key");
        }
        let json = args.args[0].clone().into_array(args.number_rows)?;
        let mut builder = BooleanBuilder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            let Some(path) = path_for_row(&args.args[1..], row)? else {
                builder.append_null();
                continue;
            };
            let Some(document) = string_at(json.as_ref(), row)? else {
                builder.append_null();
                continue;
            };
            let value: serde_json::Value = match serde_json::from_str(&document) {
                Ok(value) => value,
                Err(error) => {
                    return exec_err!("invalid input syntax for type json: {error}");
                }
            };
            builder.append_value(contains_path(&value, &path));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn contains_path(value: &serde_json::Value, path: &[PathElem]) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };
    let parent = match json_value_at(value, parents) {
        Some(parent) => parent,
        None => return false,
    };
    match (last, parent) {
        (PathElem::Key(key), serde_json::Value::Object(map)) => map.contains_key(key),
        (PathElem::Key(key), serde_json::Value::Array(values)) => {
            if let Ok(index) = key.parse::<i64>() {
                let index = if index < 0 {
                    values.len() as i64 + index
                } else {
                    index
                };
                return index >= 0 && (index as usize) < values.len();
            }
            values.iter().any(
                |value| matches!(value, serde_json::Value::String(text) if text == key),
            )
        }
        (PathElem::Index(index), serde_json::Value::Array(values)) => {
            let index = if *index < 0 {
                values.len() as i64 + *index
            } else {
                *index
            };
            index >= 0 && (index as usize) < values.len()
        }
        _ => false,
    }
}

fn json_contains_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonContains::new()))
}

// -- json_typeof ------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonTypeOf {
    signature: Signature,
}

impl JsonTypeOf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonTypeOf {
    fn name(&self) -> &str {
        "json_typeof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json = args.args[0].clone().into_array(args.number_rows)?;
        let mut builder =
            StringBuilder::with_capacity(args.number_rows, args.number_rows * 8);
        for row in 0..args.number_rows {
            let Some(path) = path_for_row(&args.args[1..], row)? else {
                builder.append_null();
                continue;
            };
            let Some(document) = string_at(json.as_ref(), row)? else {
                builder.append_null();
                continue;
            };
            let value: serde_json::Value = match serde_json::from_str(&document) {
                Ok(value) => value,
                Err(error) => {
                    return exec_err!("invalid input syntax for type json: {error}");
                }
            };
            match json_value_at(&value, &path) {
                Some(value) => builder.append_value(json_type_name(value)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn json_typeof_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonTypeOf::new()))
}

// -- json_array_length ------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayLength {
    signature: Signature,
}

impl JsonArrayLength {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonArrayLength {
    fn name(&self) -> &str {
        "json_array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json = args.args[0].clone().into_array(args.number_rows)?;
        let mut values = Vec::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            let Some(path) = path_for_row(&args.args[1..], row)? else {
                values.push(None);
                continue;
            };
            let Some(document) = string_at(json.as_ref(), row)? else {
                values.push(None);
                continue;
            };
            let value: serde_json::Value = match serde_json::from_str(&document) {
                Ok(value) => value,
                Err(error) => {
                    return exec_err!("invalid input syntax for type json: {error}");
                }
            };
            values.push(match json_value_at(&value, &path) {
                Some(serde_json::Value::Array(values)) => Some(values.len() as i64),
                _ => None,
            });
        }
        Ok(ColumnarValue::Array(Arc::new(
            datafusion::arrow::array::Int64Array::from(values),
        )))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn json_array_length_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonArrayLength::new()))
}

// -- json_object_keys -------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonObjectKeys {
    signature: Signature,
}

impl JsonObjectKeys {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonObjectKeys {
    fn name(&self) -> &str {
        "json_object_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json = args.args[0].clone().into_array(args.number_rows)?;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..args.number_rows {
            let Some(path) = path_for_row(&args.args[1..], row)? else {
                builder.append_null();
                continue;
            };
            let Some(document) = string_at(json.as_ref(), row)? else {
                builder.append_null();
                continue;
            };
            let value: serde_json::Value = match serde_json::from_str(&document) {
                Ok(value) => value,
                Err(error) => {
                    return exec_err!("invalid input syntax for type json: {error}");
                }
            };
            match json_value_at(&value, &path) {
                Some(serde_json::Value::Object(map)) => {
                    for key in map.keys() {
                        builder.values().append_value(key);
                    }
                    builder.append(true);
                }
                _ => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn json_object_keys_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonObjectKeys::new()))
}

// -- json_valid -------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonValid {
    signature: Signature,
}

impl JsonValid {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonValid {
    fn name(&self) -> &str {
        "json_valid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json = args.args[0].clone().into_array(args.number_rows)?;
        let mut builder = BooleanBuilder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            match string_at(json.as_ref(), row)? {
                Some(document) => builder.append_value(
                    serde_json::from_str::<serde_json::Value>(&document).is_ok(),
                ),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn aliases(&self) -> &[String] {
        &[]
    }
}

fn json_valid_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(JsonValid::new()))
}

/// The JSON UDF with `name`, used by the distributed physical-plan codec.
pub fn udf_by_name(name: &str) -> Option<Arc<ScalarUDF>> {
    Some(match name {
        "json_get_json" => json_get_json_udf(),
        "json_as_text" => json_as_text_udf(),
        "json_contains" => json_contains_udf(),
        "json_typeof" => json_typeof_udf(),
        "json_array_length" => json_array_length_udf(),
        "json_object_keys" => json_object_keys_udf(),
        "json_valid" => json_valid_udf(),
        _ => return None,
    })
}

/// Distributed physical-plan codec for the JSON scalar functions.
///
/// The functions carry no state beyond their name, so the payload is empty and
/// the decoder reconstructs the UDF from the name. The codec refuses every
/// non-JSON UDF so the composed codec can try the next entry.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonUdfCodec;

impl datafusion_proto::physical_plan::PhysicalExtensionCodec for JsonUdfCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        _ctx: &datafusion::execution::TaskContext,
        _proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        datafusion::common::not_impl_err!("JsonUdfCodec only encodes scalar functions")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        datafusion::common::not_impl_err!("JsonUdfCodec only encodes scalar functions")
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> DFResult<Arc<ScalarUDF>> {
        udf_by_name(name).ok_or_else(|| {
            datafusion::common::DataFusionError::NotImplemented(format!(
                "JsonUdfCodec does not know scalar function {name}"
            ))
        })
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> DFResult<()> {
        if udf_by_name(node.name()).is_some() {
            buf.extend_from_slice(node.name().as_bytes());
            Ok(())
        } else {
            datafusion::common::not_impl_err!(
                "JsonUdfCodec does not encode scalar function {}",
                node.name()
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Operator planner
// ---------------------------------------------------------------------------

/// Plans PostgreSQL's JSON path operators to text-returning UDFs.
#[derive(Debug, Default)]
struct PgJsonExprPlanner;

impl ExprPlanner for PgJsonExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> DFResult<PlannerResult<RawBinaryExpr>> {
        let udf = match expr.op {
            BinaryOperator::Arrow | BinaryOperator::HashArrow => json_get_json_udf(),
            BinaryOperator::LongArrow | BinaryOperator::HashLongArrow => {
                json_as_text_udf()
            }
            BinaryOperator::Question => json_contains_udf(),
            _ => return Ok(PlannerResult::Original(expr)),
        };
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction {
                func: udf,
                args: vec![expr.left, expr.right],
            },
        )))
    }
}
