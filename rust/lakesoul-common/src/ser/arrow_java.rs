// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name")]
enum ArrowJavaType {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "struct")]
    Struct,
    #[serde(rename = "list")]
    List,
    #[serde(rename = "largelist")]
    LargeList,
    #[serde(rename = "fixedsizelist")]
    FixedSizeList {
        #[serde(rename = "listSize")]
        list_size: i32,
    },
    #[serde(rename = "union")]
    Union,
    #[serde(rename = "map")]
    Map {
        #[serde(rename = "keysSorted")]
        keys_sorted: bool,
    },
    #[serde(rename = "int")]
    Int {
        #[serde(rename = "isSigned")]
        is_signed: bool,
        #[serde(rename = "bitWidth")]
        bit_width: i32,
    },
    #[serde(rename = "floatingpoint")]
    FloatingPoint { precision: String },
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "largeutf8")]
    LargeUtf8,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "largebinary")]
    LargeBinary,
    #[serde(rename = "fixedsizebinary")]
    FixedSizeBinary {
        #[serde(rename = "byteWidth", alias = "bitWidth")]
        byte_width: i32,
    },
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "decimal")]
    Decimal {
        precision: u8,
        scale: i8,
        #[serde(rename = "bitWidth", default = "default_decimal_bit_width")]
        bit_width: i32,
    },
    #[serde(rename = "date")]
    Date { unit: String },
    #[serde(rename = "time")]
    Time {
        #[serde(rename = "bitWidth")]
        bit_width: i32,
        unit: String,
    },
    #[serde(rename = "timestamp")]
    Timestamp {
        unit: String,
        timezone: Option<String>,
    },
    #[serde(rename = "interval")]
    Interval {
        #[serde(default = "default_interval_unit")]
        unit: String,
    },
    #[serde(rename = "duration")]
    Duration {
        #[serde(default = "default_duration_unit")]
        unit: String,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArrowJavaField {
    name: String,
    #[serde(rename = "type")]
    data_type: ArrowJavaType,
    nullable: bool,
    #[serde(default)]
    children: Vec<ArrowJavaField>,
    /// A map of key-value pairs containing additional field meta data.
    #[serde(default)]
    metadata: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArrowJavaSchema {
    fields: Vec<ArrowJavaField>,
    /// A map of key-value pairs containing additional meta data.
    metadata: Option<HashMap<String, String>>,
}

fn default_decimal_bit_width() -> i32 {
    128
}

fn default_interval_unit() -> String {
    "YEAR_MONTH".to_string()
}

fn default_duration_unit() -> String {
    "MICROSECOND".to_string()
}

fn time_unit_to_java(unit: &TimeUnit) -> String {
    match unit {
        TimeUnit::Second => "SECOND",
        TimeUnit::Microsecond => "MICROSECOND",
        TimeUnit::Millisecond => "MILLISECOND",
        TimeUnit::Nanosecond => "NANOSECOND",
    }
    .to_string()
}

fn time_unit_from_java(unit: &str) -> TimeUnit {
    match unit.to_ascii_uppercase().as_str() {
        "SECOND" => TimeUnit::Second,
        "MILLISECOND" => TimeUnit::Millisecond,
        "MICROSECOND" => TimeUnit::Microsecond,
        "NANOSECOND" => TimeUnit::Nanosecond,
        other => panic!("TimeUnit has an invalid value = {}", other),
    }
}

fn interval_unit_to_java(unit: &IntervalUnit) -> String {
    match unit {
        IntervalUnit::YearMonth => "YEAR_MONTH",
        IntervalUnit::DayTime => "DAY_TIME",
        IntervalUnit::MonthDayNano => "MONTH_DAY_NANO",
    }
    .to_string()
}

fn interval_unit_from_java(unit: &str) -> IntervalUnit {
    match unit.to_ascii_uppercase().as_str() {
        "YEAR_MONTH" => IntervalUnit::YearMonth,
        "DAY_TIME" => IntervalUnit::DayTime,
        "MONTH_DAY_NANO" => IntervalUnit::MonthDayNano,
        other => panic!("IntervalUnit has an invalid value = {}", other),
    }
}

impl From<&FieldRef> for ArrowJavaField {
    fn from(field: &FieldRef) -> Self {
        let name = field.name().clone();
        let (data_type, children) = match field.data_type() {
            DataType::Null => (ArrowJavaType::Null, vec![]),

            DataType::Struct(fields) => (
                ArrowJavaType::Struct,
                fields.iter().map(ArrowJavaField::from).collect::<Vec<_>>(),
            ),

            DataType::List(field) => {
                (ArrowJavaType::List, vec![ArrowJavaField::from(field)])
            }
            DataType::LargeList(field) => {
                (ArrowJavaType::LargeList, vec![ArrowJavaField::from(field)])
            }
            DataType::FixedSizeList(field, list_size) => (
                ArrowJavaType::FixedSizeList {
                    list_size: *list_size,
                },
                vec![ArrowJavaField::from(field)],
            ),

            DataType::Map(struct_field, key_sorted) => (
                ArrowJavaType::Map {
                    keys_sorted: *key_sorted,
                },
                vec![ArrowJavaField::from(struct_field)],
            ),

            DataType::Int8 => (
                ArrowJavaType::Int {
                    is_signed: true,
                    bit_width: 8,
                },
                vec![],
            ),
            DataType::Int16 => (
                ArrowJavaType::Int {
                    is_signed: true,
                    bit_width: 16,
                },
                vec![],
            ),
            DataType::Int32 => (
                ArrowJavaType::Int {
                    is_signed: true,
                    bit_width: 32,
                },
                vec![],
            ),
            DataType::Int64 => (
                ArrowJavaType::Int {
                    is_signed: true,
                    bit_width: 64,
                },
                vec![],
            ),
            DataType::UInt8 => (
                ArrowJavaType::Int {
                    is_signed: false,
                    bit_width: 8,
                },
                vec![],
            ),
            DataType::UInt16 => (
                ArrowJavaType::Int {
                    is_signed: false,
                    bit_width: 16,
                },
                vec![],
            ),
            DataType::UInt32 => (
                ArrowJavaType::Int {
                    is_signed: false,
                    bit_width: 32,
                },
                vec![],
            ),
            DataType::UInt64 => (
                ArrowJavaType::Int {
                    is_signed: false,
                    bit_width: 64,
                },
                vec![],
            ),

            DataType::Float16 => (
                ArrowJavaType::FloatingPoint {
                    precision: "HALF".to_string(),
                },
                vec![],
            ),
            DataType::Float32 => (
                ArrowJavaType::FloatingPoint {
                    precision: "SINGLE".to_string(),
                },
                vec![],
            ),
            DataType::Float64 => (
                ArrowJavaType::FloatingPoint {
                    precision: "DOUBLE".to_string(),
                },
                vec![],
            ),

            DataType::Utf8 => (ArrowJavaType::Utf8, vec![]),
            DataType::LargeUtf8 => (ArrowJavaType::LargeUtf8, vec![]),

            DataType::Binary => (ArrowJavaType::Binary, vec![]),
            DataType::LargeBinary => (ArrowJavaType::LargeBinary, vec![]),
            DataType::FixedSizeBinary(bit_width) => (
                ArrowJavaType::FixedSizeBinary {
                    byte_width: *bit_width,
                },
                vec![],
            ),

            DataType::Boolean => (ArrowJavaType::Bool, vec![]),

            DataType::Decimal32(precision, scale) => (
                ArrowJavaType::Decimal {
                    precision: *precision,
                    scale: *scale,
                    bit_width: 32,
                },
                vec![],
            ),
            DataType::Decimal64(precision, scale) => (
                ArrowJavaType::Decimal {
                    precision: *precision,
                    scale: *scale,
                    bit_width: 64,
                },
                vec![],
            ),

            DataType::Decimal128(precision, scale) => (
                ArrowJavaType::Decimal {
                    precision: *precision,
                    scale: *scale,
                    bit_width: 128,
                },
                vec![],
            ),
            DataType::Decimal256(precision, scale) => (
                ArrowJavaType::Decimal {
                    precision: *precision,
                    scale: *scale,
                    bit_width: 256,
                },
                vec![],
            ),

            DataType::Date32 => (
                ArrowJavaType::Date {
                    unit: "DAY".to_string(),
                },
                vec![],
            ),
            DataType::Date64 => (
                ArrowJavaType::Date {
                    unit: "MILLISECOND".to_string(),
                },
                vec![],
            ),

            DataType::Time32(unit) => (
                ArrowJavaType::Time {
                    bit_width: 32,
                    unit: time_unit_to_java(unit),
                },
                vec![],
            ),
            DataType::Time64(unit) => (
                ArrowJavaType::Time {
                    bit_width: 64,
                    unit: time_unit_to_java(unit),
                },
                vec![],
            ),
            DataType::Timestamp(unit, timezone) => (
                ArrowJavaType::Timestamp {
                    unit: time_unit_to_java(unit),
                    timezone: timezone.as_ref().map(|s| s.to_string()),
                },
                vec![],
            ),
            DataType::Duration(unit) => (
                ArrowJavaType::Duration {
                    unit: time_unit_to_java(unit),
                },
                vec![],
            ),
            DataType::Interval(unit) => (
                ArrowJavaType::Interval {
                    unit: interval_unit_to_java(unit),
                },
                vec![],
            ),

            DataType::Union(_, _) => todo!("Union type not supported"),
            DataType::Dictionary(_, _) => todo!("Dictionary type not supported"),
            DataType::RunEndEncoded(_, _) => todo!("RunEndEncoded type not supported"),
            DataType::BinaryView => todo!("BinaryView type not supported"),
            DataType::Utf8View => todo!("Utf8View type not supported"),
            DataType::ListView(_) => todo!("ListView type not supported"),
            DataType::LargeListView(_) => todo!("LargeListView type not supported"),
        };
        let nullable = field.is_nullable();
        ArrowJavaField {
            name,
            data_type,
            nullable,
            children,
            metadata: Some(field.metadata().clone()),
        }
    }
}

impl From<&ArrowJavaField> for Field {
    fn from(field: &ArrowJavaField) -> Field {
        let java_type = &field.data_type.clone();
        let data_type = match java_type {
            ArrowJavaType::Null => DataType::Null,
            ArrowJavaType::Struct => DataType::Struct(Fields::from(
                field
                    .children
                    .iter()
                    .map(|f| f.into())
                    .collect::<Vec<Field>>(),
            )),
            ArrowJavaType::List => {
                assert_eq!(field.children.len(), 1);
                DataType::List(Arc::new(field.children.first().unwrap().into()))
            }
            ArrowJavaType::LargeList => {
                assert_eq!(field.children.len(), 1);
                DataType::LargeList(Arc::new(field.children.first().unwrap().into()))
            }
            ArrowJavaType::FixedSizeList { list_size } => {
                assert_eq!(field.children.len(), 1);
                DataType::FixedSizeList(
                    Arc::new(field.children.first().unwrap().into()),
                    *list_size,
                )
            }
            ArrowJavaType::Union => todo!("Union type not supported"),
            ArrowJavaType::Map { keys_sorted } => {
                assert_eq!(field.children.len(), 1);
                DataType::Map(
                    Arc::new(field.children.first().unwrap().into()),
                    *keys_sorted,
                )
            }
            ArrowJavaType::Int {
                is_signed,
                bit_width,
            } => {
                if *is_signed {
                    match bit_width {
                        8 => DataType::Int8,
                        16 => DataType::Int16,
                        32 => DataType::Int32,
                        64 => DataType::Int64,
                        other => panic!("Int has an invalid bit_width = {}", other),
                    }
                } else {
                    match bit_width {
                        8 => DataType::UInt8,
                        16 => DataType::UInt16,
                        32 => DataType::UInt32,
                        64 => DataType::UInt64,
                        other => panic!("Int has an invalid bit_width = {}", other),
                    }
                }
            }
            ArrowJavaType::FloatingPoint { precision } => {
                match precision.to_ascii_uppercase().as_str() {
                    "HALF" => DataType::Float16,
                    "SINGLE" => DataType::Float32,
                    "DOUBLE" => DataType::Float64,
                    other => panic!("FloatingPoint has an invalid precision = {}", other),
                }
            }
            ArrowJavaType::Utf8 => DataType::Utf8,
            ArrowJavaType::LargeUtf8 => DataType::LargeUtf8,
            ArrowJavaType::Binary => DataType::Binary,
            ArrowJavaType::LargeBinary => DataType::LargeBinary,
            ArrowJavaType::FixedSizeBinary { byte_width } => {
                DataType::FixedSizeBinary(*byte_width)
            }
            ArrowJavaType::Bool => DataType::Boolean,
            ArrowJavaType::Decimal {
                precision,
                scale,
                bit_width,
            } if *bit_width <= 32 => DataType::Decimal32(*precision, *scale),
            ArrowJavaType::Decimal {
                precision,
                scale,
                bit_width,
            } if *bit_width <= 64 => DataType::Decimal64(*precision, *scale),
            ArrowJavaType::Decimal {
                precision,
                scale,
                bit_width,
            } if *bit_width <= 128 => DataType::Decimal128(*precision, *scale),
            ArrowJavaType::Decimal {
                precision,
                scale,
                bit_width: _,
            } => DataType::Decimal256(*precision, *scale),
            ArrowJavaType::Date { unit } if unit.eq_ignore_ascii_case("DAY") => {
                DataType::Date32
            }
            ArrowJavaType::Date { unit: _ } => DataType::Date64,
            ArrowJavaType::Time { bit_width, unit } => {
                let time_unit = time_unit_from_java(unit);
                match bit_width {
                    32 => DataType::Time32(time_unit),
                    64 => DataType::Time64(time_unit),
                    other => panic!("Time has an invalid bit_width = {}", other),
                }
            }
            ArrowJavaType::Timestamp { unit, timezone } => {
                let time_unit = time_unit_from_java(unit);
                let timezone: Option<Arc<str>> =
                    timezone.as_ref().map(|t| Arc::from(t.as_str()));
                DataType::Timestamp(time_unit, timezone)
            }
            ArrowJavaType::Interval { unit } => {
                DataType::Interval(interval_unit_from_java(unit))
            }
            ArrowJavaType::Duration { unit } => {
                DataType::Duration(time_unit_from_java(unit))
            }
        };
        Field::new(field.name.clone(), data_type, field.nullable)
            .with_metadata(field.metadata.clone().unwrap_or_default())
    }
}

impl From<SchemaRef> for ArrowJavaSchema {
    fn from(schema: SchemaRef) -> Self {
        Self {
            fields: schema
                .fields()
                .iter()
                .map(ArrowJavaField::from)
                .collect::<Vec<_>>(),
            metadata: Some(schema.metadata().clone()),
        }
    }
}

impl From<Schema> for ArrowJavaSchema {
    fn from(schema: Schema) -> Self {
        Self {
            fields: schema
                .fields()
                .iter()
                .map(ArrowJavaField::from)
                .collect::<Vec<_>>(),
            metadata: Some(schema.metadata().clone()),
        }
    }
}

impl From<ArrowJavaSchema> for SchemaRef {
    fn from(schema: ArrowJavaSchema) -> Self {
        SchemaRef::new(Schema::new_with_metadata(
            schema
                .fields
                .iter()
                .map(|f| f.into())
                .collect::<Vec<Field>>(),
            schema.metadata.unwrap_or_default(),
        ))
    }
}

pub fn schema_from_metadata_str(s: &str) -> SchemaRef {
    serde_json::from_str::<Schema>(s).map_or_else(
        |_| {
            let java_schema = serde_json::from_str::<ArrowJavaSchema>(s).unwrap();
            java_schema.into()
        },
        SchemaRef::new,
    )
}
fn normalize_field(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::LargeUtf8 => DataType::Utf8,
        DataType::LargeBinary => DataType::Binary,

        DataType::List(inner) => {
            DataType::List(Arc::new(normalize_field(inner.as_ref())))
        }

        DataType::LargeList(inner) => {
            DataType::List(Arc::new(normalize_field(inner.as_ref())))
        }

        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| Arc::new(normalize_field(f.as_ref())))
                .collect(),
        ),

        other => other.clone(),
    };

    Field::new(field.name(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone())
}

fn normalize_schema_for_spark(schema: &Schema) -> Schema {
    Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| normalize_field(f.as_ref()))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    )
}

// workaround
pub fn schema_to_metadata_str(schema: &Schema) -> String {
    let normalized = Arc::new(normalize_schema_for_spark(schema.as_ref()));
    serde_json::to_string(&ArrowJavaSchema::from(normalized)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_arrow_java_schema_preserves_metadata_and_java_types() {
        let schema_json = r#"
        {
          "fields": [
            {
              "name": "id",
              "nullable": false,
              "type": {"name": "int", "isSigned": true, "bitWidth": 32},
              "children": [],
              "metadata": {"spark_comment": "primary key"}
            },
            {
              "name": "values",
              "nullable": true,
              "type": {"name": "list"},
              "children": [
                {
                  "name": "element",
                  "nullable": true,
                  "type": {"name": "utf8"},
                  "children": [],
                  "metadata": {"child": "meta"}
                }
              ],
              "metadata": {}
            },
            {
              "name": "attrs",
              "nullable": true,
              "type": {"name": "map", "keysSorted": false},
              "children": [
                {
                  "name": "entries",
                  "nullable": false,
                  "type": {"name": "struct"},
                  "children": [
                    {
                      "name": "key",
                      "nullable": false,
                      "type": {"name": "utf8"},
                      "children": []
                    },
                    {
                      "name": "value",
                      "nullable": true,
                      "type": {"name": "int", "isSigned": true, "bitWidth": 64},
                      "children": []
                    }
                  ]
                }
              ]
            },
            {
              "name": "duration_col",
              "nullable": true,
              "type": {"name": "duration", "unit": "MICROSECOND"},
              "children": []
            },
            {
              "name": "interval_col",
              "nullable": true,
              "type": {"name": "interval", "unit": "YEAR_MONTH"},
              "children": []
            },
            {
              "name": "timestamp_ntz",
              "nullable": true,
              "type": {"name": "timestamp", "unit": "MICROSECOND", "timezone": null},
              "children": []
            },
            {
              "name": "fixed",
              "nullable": true,
              "type": {"name": "fixedsizebinary", "byteWidth": 16},
              "children": []
            }
          ],
          "metadata": {"schema_key": "schema_value"}
        }
        "#;

        let schema = schema_from_metadata_str(schema_json);

        assert_eq!(schema.metadata().get("schema_key").unwrap(), "schema_value");
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema
                .field_with_name("id")
                .unwrap()
                .metadata()
                .get("spark_comment")
                .unwrap(),
            "primary key"
        );
        assert!(matches!(
            schema.field_with_name("values").unwrap().data_type(),
            DataType::List(_)
        ));
        assert!(matches!(
            schema.field_with_name("attrs").unwrap().data_type(),
            DataType::Map(_, false)
        ));
        assert_eq!(
            schema.field_with_name("duration_col").unwrap().data_type(),
            &DataType::Duration(TimeUnit::Microsecond)
        );
        assert_eq!(
            schema.field_with_name("interval_col").unwrap().data_type(),
            &DataType::Interval(IntervalUnit::YearMonth)
        );
        assert_eq!(
            schema.field_with_name("timestamp_ntz").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field_with_name("fixed").unwrap().data_type(),
            &DataType::FixedSizeBinary(16)
        );
    }

    #[test]
    fn rust_schema_round_trips_through_arrow_java_json() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "duration_col",
                    DataType::Duration(TimeUnit::Microsecond),
                    true,
                )
                .with_metadata(HashMap::from([(
                    "spark_comment".to_string(),
                    "duration field".to_string(),
                )])),
                Field::new(
                    "interval_col",
                    DataType::Interval(IntervalUnit::YearMonth),
                    true,
                ),
                Field::new("decimal_col", DataType::Decimal64(12, 3), true),
                Field::new("fixed", DataType::FixedSizeBinary(8), true),
            ],
            HashMap::from([("schema_key".to_string(), "schema_value".to_string())]),
        );

        let json = schema_to_metadata_str(&schema);
        let parsed = schema_from_metadata_str(&json);

        assert_eq!(parsed.as_ref(), &schema);
    }
}
