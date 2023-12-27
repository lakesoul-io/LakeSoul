// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};

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
        #[serde(rename = "bitWidth")]
        bit_width: i32,
    },
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "decimal")]
    Decimal {
        precision: u8,
        scale: i8,
        #[serde(rename = "bitWidth")]
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
    Timestamp { unit: String, timezone: Option<String> },
    #[serde(rename = "interval")]
    Interval,
    #[serde(rename = "duration")]
    Duration,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ArrowJavaField {
    name: String,
    #[serde(rename = "type")]
    data_type: ArrowJavaType,
    nullable: bool,
    children: Vec<ArrowJavaField>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArrowJavaSchema {
    fields: Vec<ArrowJavaField>,
    /// A map of key-value pairs containing additional meta data.
    metadata: Option<HashMap<String, String>>,
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

            DataType::List(field) => (ArrowJavaType::List, vec![ArrowJavaField::from(field)]),
            DataType::LargeList(field) => (ArrowJavaType::LargeList, vec![ArrowJavaField::from(field)]),
            DataType::FixedSizeList(field, list_size) => (
                ArrowJavaType::FixedSizeList { list_size: *list_size },
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
            DataType::FixedSizeBinary(bit_width) => (ArrowJavaType::FixedSizeBinary { bit_width: *bit_width }, vec![]),

            DataType::Boolean => (ArrowJavaType::Bool, vec![]),

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
                    unit: match unit {
                        TimeUnit::Second => "SECOND".to_string(),
                        TimeUnit::Microsecond => "MICROSECOND".to_string(),
                        TimeUnit::Millisecond => "MILLISECOND".to_string(),
                        TimeUnit::Nanosecond => "NANOSECOND".to_string(),
                    },
                },
                vec![],
            ),
            DataType::Time64(unit) => (
                ArrowJavaType::Time {
                    bit_width: 64,
                    unit: match unit {
                        TimeUnit::Second => "SECOND".to_string(),
                        TimeUnit::Microsecond => "MICROSECOND".to_string(),
                        TimeUnit::Millisecond => "MILLISECOND".to_string(),
                        TimeUnit::Nanosecond => "NANOSECOND".to_string(),
                    },
                },
                vec![],
            ),
            DataType::Timestamp(unit, timezone) => (
                ArrowJavaType::Timestamp {
                    unit: match unit {
                        TimeUnit::Second => "SECOND".to_string(),
                        TimeUnit::Microsecond => "MICROSECOND".to_string(),
                        TimeUnit::Millisecond => "MILLISECOND".to_string(),
                        TimeUnit::Nanosecond => "NANOSECOND".to_string(),
                    },
                    timezone: timezone.as_ref().map(|s| s.to_string()),
                },
                vec![],
            ),

            DataType::Union(_, _) => todo!("Union type not supported"),
            DataType::Dictionary(_, _) => todo!("Dictionary type not supported"),
            DataType::Duration(_) => todo!("Duration type not supported"),
            DataType::Interval(_) => todo!("Interval type not supported"),
            DataType::RunEndEncoded(_, _) => todo!("RunEndEncoded type not supported"),
        };
        let nullable = field.is_nullable();
        ArrowJavaField {
            name,
            data_type,
            nullable,
            children,
        }
    }
}

impl From<&ArrowJavaField> for Field {
    fn from(field: &ArrowJavaField) -> Field {
        let java_type = &field.data_type.clone();
        let data_type = match java_type {
            ArrowJavaType::Null => DataType::Null,
            ArrowJavaType::Struct => DataType::Struct(Fields::from(
                field.children.iter().map(|f| f.into()).collect::<Vec<Field>>(),
            )),
            ArrowJavaType::List => {
                assert!(field.children.len() == 1);
                DataType::List(Arc::new(field.children.first().unwrap().into()))
            }
            ArrowJavaType::LargeList => {
                assert!(field.children.len() == 1);
                DataType::LargeList(Arc::new(field.children.first().unwrap().into()))
            }
            ArrowJavaType::FixedSizeList { list_size } => {
                assert!(field.children.len() == 1);
                DataType::FixedSizeList(Arc::new(field.children.first().unwrap().into()), *list_size)
            }
            ArrowJavaType::Union => todo!("Union type not supported"),
            ArrowJavaType::Map { keys_sorted } => {
                assert!(field.children.len() == 1);
                DataType::Map(Arc::new(field.children.first().unwrap().into()), *keys_sorted)
            }
            ArrowJavaType::Int { is_signed, bit_width } => {
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
            ArrowJavaType::FloatingPoint { precision } => match precision.as_str() {
                "HALF" => DataType::Float16,
                "SINGLE" => DataType::Float32,
                "DOUBLE" => DataType::Float64,
                other => panic!("FloatingPoint has an invalid precision = {}", other),
            },
            ArrowJavaType::Utf8 => DataType::Utf8,
            ArrowJavaType::LargeUtf8 => DataType::LargeUtf8,
            ArrowJavaType::Binary => DataType::Binary,
            ArrowJavaType::LargeBinary => DataType::LargeBinary,
            ArrowJavaType::FixedSizeBinary { bit_width } => DataType::FixedSizeBinary(*bit_width),
            ArrowJavaType::Bool => DataType::Boolean,
            ArrowJavaType::Decimal {
                precision,
                scale,
                bit_width,
            } if *bit_width > 128 => DataType::Decimal256(*precision, *scale),
            ArrowJavaType::Decimal {
                precision,
                scale,
                bit_width,
            } => DataType::Decimal128(*precision, *scale),
            ArrowJavaType::Date { unit } if unit == "DAY" => DataType::Date32,
            ArrowJavaType::Date { unit } => DataType::Date64,
            ArrowJavaType::Time { bit_width, unit } => {
                let time_unit = match unit.as_str() {
                    "SECOND" => TimeUnit::Second,
                    "MILLISECOND" => TimeUnit::Millisecond,
                    "MICROSECOND" => TimeUnit::Microsecond,
                    "NANOSECOND" => TimeUnit::Nanosecond,
                    other => panic!("TimeUnit has an invalid value = {}", other),
                };
                match bit_width {
                    32 => DataType::Time32(time_unit),
                    64 => DataType::Time64(time_unit),
                    other => panic!("Time has an invalid bit_width = {}", other),
                }
            }
            ArrowJavaType::Timestamp { unit, timezone } => {
                let time_unit = match unit.as_str() {
                    "SECOND" => TimeUnit::Second,
                    "MILLISECOND" => TimeUnit::Millisecond,
                    "MICROSECOND" => TimeUnit::Microsecond,
                    "NANOSECOND" => TimeUnit::Nanosecond,
                    other => panic!("TimeUnit has an invalid value = {}", other),
                };
                let timezone: Option<Arc<str>> = timezone.as_ref().map(|t| Arc::from(t.as_str()));
                DataType::Timestamp(time_unit, timezone)
            }
            ArrowJavaType::Interval => todo!("Interval type not supported"),
            ArrowJavaType::Duration => todo!("Duration type not supported"),
        };
        Field::new(field.name.clone(), data_type, field.nullable)
    }
}

impl From<SchemaRef> for ArrowJavaSchema {
    fn from(schema: SchemaRef) -> Self {
        Self {
            fields: schema.fields().iter().map(ArrowJavaField::from).collect::<Vec<_>>(),
            metadata: None,
        }
    }
}

impl From<ArrowJavaSchema> for SchemaRef {
    fn from(schema: ArrowJavaSchema) -> Self {
        SchemaRef::new(Schema::new(
            schema.fields.iter().map(|f| f.into()).collect::<Vec<Field>>(),
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
