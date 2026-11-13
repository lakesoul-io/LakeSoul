// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The schema-evolution rule every LakeSoul writer applies to a table's
//! current schema.
//!
//! LakeSoul keeps one current schema per table and never rewrites old files:
//! every reader infers each file's schema, matches columns **by name**, casts
//! them to the table type and fills the columns a file lacks with NULL. A
//! change is therefore safe only when every file written under the old schema
//! still reads correctly under the new one:
//!
//! * a **nullable** column appended at the end reads as NULL in old files;
//! * a column whose type is **widened** (`Int16 -> Int32 | Int64`,
//!   `Int32 -> Int64`, `Float32 -> Float64`, a decimal that keeps every
//!   integer and fraction digit) casts losslessly;
//! * `DROP NOT NULL` on a non-key column only relaxes a constraint.
//!
//! Everything else - dropping or renaming a column (old data would read as
//! NULL), narrowing or changing a type, `SET NOT NULL` (old rows cannot be
//! proved to satisfy it), or touching a primary-key, range-partition or CDC
//! column (the physical layout depends on them) - is refused.

use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema};

/// One column change to apply to a table's current schema.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnChange {
    /// Append a column. It must be nullable.
    Add(Field),
    /// Change a column's type to a wider one.
    Widen {
        /// The column to widen.
        name: String,
        /// Its new type.
        to: DataType,
    },
    /// Make a non-key column nullable.
    DropNotNull(String),
}

/// The columns a table's physical layout depends on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableLayout {
    /// Primary-key (hash-bucket) columns.
    pub primary_keys: Vec<String>,
    /// Range-partition columns.
    pub range_partitions: Vec<String>,
    /// The CDC change column, when the table is a CDC table.
    pub cdc_column: Option<String>,
}

impl TableLayout {
    /// Layout from `table_info.partitions` (`"<range,...>;<hash,...>"`) and the
    /// CDC change column, if any.
    pub fn from_partitions(partitions: &str, cdc_column: Option<String>) -> Self {
        let (range, hash) = partitions.split_once(';').unwrap_or((partitions, ""));
        let split = |list: &str| {
            list.split(',')
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        Self {
            primary_keys: split(hash),
            range_partitions: split(range),
            cdc_column,
        }
    }

    fn role(&self, name: &str) -> Option<ColumnRole> {
        if self.primary_keys.iter().any(|key| key == name) {
            Some(ColumnRole::PrimaryKey)
        } else if self.range_partitions.iter().any(|key| key == name) {
            Some(ColumnRole::RangePartition)
        } else if self.cdc_column.as_deref() == Some(name) {
            Some(ColumnRole::CdcChange)
        } else {
            None
        }
    }
}

/// Why a layout column cannot change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnRole {
    /// The column is part of the primary key, which also decides its hash
    /// bucket.
    PrimaryKey,
    /// The column's value names the partition directory.
    RangePartition,
    /// The column carries the CDC row kind every engine reads as text.
    CdcChange,
}

impl fmt::Display for ColumnRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PrimaryKey => "primary-key",
            Self::RangePartition => "range-partition",
            Self::CdcChange => "CDC change",
        })
    }
}

/// A refused change.
#[derive(Debug, Clone, PartialEq)]
pub enum EvolutionError {
    /// The added column already exists. `same_type` tells an
    /// `ADD COLUMN IF NOT EXISTS` whether it may be a no-op.
    DuplicateColumn { name: String, same_type: bool },
    /// The changed column does not exist.
    UndefinedColumn(String),
    /// The change touches a column the physical layout depends on.
    LayoutColumn { name: String, role: ColumnRole },
    /// The change is not one the rule allows.
    Unsupported(String),
}

impl fmt::Display for EvolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateColumn { name, .. } => {
                write!(f, "column \"{name}\" already exists")
            }
            Self::UndefinedColumn(name) => write!(f, "column \"{name}\" does not exist"),
            Self::LayoutColumn { name, role } => write!(
                f,
                "column \"{name}\" is a {role} column: the table's physical layout \
                 depends on it, so it cannot be changed"
            ),
            Self::Unsupported(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for EvolutionError {}

/// Apply `change` to `current` and return the new schema.
///
/// A change that leaves the schema as it is (widening to the same type,
/// `DROP NOT NULL` on a nullable column) returns `current` unchanged, the way
/// PostgreSQL treats it.
pub fn check_evolution(
    current: &Schema,
    change: &ColumnChange,
    layout: &TableLayout,
) -> Result<Schema, EvolutionError> {
    match change {
        ColumnChange::Add(field) => add_column(current, field),
        ColumnChange::Widen { name, to } => {
            let (index, field) = existing(current, name)?;
            if field.data_type() == to {
                return Ok(current.clone());
            }
            if let Some(role) = layout.role(name) {
                return Err(EvolutionError::LayoutColumn {
                    name: name.clone(),
                    role,
                });
            }
            if !can_widen(field.data_type(), to) {
                return Err(EvolutionError::Unsupported(format!(
                    "cannot change column \"{name}\" from {} to {to}: only lossless \
                     widenings are supported (smallint -> integer/bigint, integer -> \
                     bigint, real -> double precision, and numeric(p,s) -> \
                     numeric(p',s') keeping every integer and fraction digit), \
                     because existing files keep their old type and are cast on read",
                    field.data_type()
                )));
            }
            Ok(replace(
                current,
                index,
                field.as_ref().clone().with_data_type(to.clone()),
            ))
        }
        ColumnChange::DropNotNull(name) => {
            let (index, field) = existing(current, name)?;
            if field.is_nullable() {
                return Ok(current.clone());
            }
            if let Some(role) = layout.role(name) {
                return Err(EvolutionError::LayoutColumn {
                    name: name.clone(),
                    role,
                });
            }
            Ok(replace(
                current,
                index,
                field.as_ref().clone().with_nullable(true),
            ))
        }
    }
}

fn add_column(current: &Schema, field: &Field) -> Result<Schema, EvolutionError> {
    // Readers match columns by name, and Spark and Flink resolve them
    // case-insensitively: a column that differs only in case would be
    // ambiguous there.
    if let Some(existing) = current
        .fields()
        .iter()
        .find(|existing| existing.name().eq_ignore_ascii_case(field.name()))
    {
        return Err(EvolutionError::DuplicateColumn {
            name: existing.name().clone(),
            same_type: existing.name() == field.name()
                && existing.data_type() == field.data_type(),
        });
    }
    if !field.is_nullable() {
        return Err(EvolutionError::Unsupported(format!(
            "cannot add NOT NULL column \"{}\": rows in existing files read the new \
             column as NULL",
            field.name()
        )));
    }
    if is_nested(field.data_type()) {
        return Err(EvolutionError::Unsupported(format!(
            "cannot add column \"{}\" of nested type {}",
            field.name(),
            field.data_type()
        )));
    }
    let mut fields: Vec<FieldRef> = current.fields().iter().cloned().collect();
    fields.push(Arc::new(field.clone()));
    Ok(Schema::new_with_metadata(
        fields,
        current.metadata().clone(),
    ))
}

fn existing<'a>(
    current: &'a Schema,
    name: &str,
) -> Result<(usize, &'a FieldRef), EvolutionError> {
    current
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == name)
        .ok_or_else(|| EvolutionError::UndefinedColumn(name.to_string()))
}

fn replace(current: &Schema, index: usize, field: Field) -> Schema {
    let mut fields: Vec<FieldRef> = current.fields().iter().cloned().collect();
    fields[index] = Arc::new(field);
    Schema::new_with_metadata(fields, current.metadata().clone())
}

fn is_nested(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(..)
            | DataType::Struct(_)
            | DataType::Map(..)
            | DataType::Union(..)
    )
}

/// Whether every value of `from` is represented exactly by `to`.
pub fn can_widen(from: &DataType, to: &DataType) -> bool {
    if from == to {
        return true;
    }
    match (from, to) {
        (DataType::Int16, DataType::Int32 | DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Float32, DataType::Float64) => true,
        (
            DataType::Decimal128(from_precision, from_scale),
            DataType::Decimal128(to_precision, to_scale),
        ) => decimal_accepts(*to_precision, *to_scale, *from_precision, *from_scale),
        _ => false,
    }
}

/// Whether `decimal(target_precision, target_scale)` holds every value of
/// `decimal(source_precision, source_scale)`: no fraction digit and no integer
/// digit is lost.
pub fn decimal_accepts(
    target_precision: u8,
    target_scale: i8,
    source_precision: u8,
    source_scale: i8,
) -> bool {
    target_scale >= source_scale
        && i16::from(target_precision) - i16::from(target_scale)
            >= i16::from(source_precision) - i16::from(source_scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("part", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("price", DataType::Decimal128(10, 2), true),
            Field::new("rowKinds", DataType::Utf8, true),
        ])
    }

    fn layout() -> TableLayout {
        TableLayout::from_partitions("part;id", Some("rowKinds".to_string()))
    }

    #[test]
    fn layout_parses_range_and_hash_columns() {
        let layout = TableLayout::from_partitions("a,b;c", None);
        assert_eq!(layout.range_partitions, ["a", "b"]);
        assert_eq!(layout.primary_keys, ["c"]);
        let layout = TableLayout::from_partitions(";", None);
        assert!(layout.range_partitions.is_empty() && layout.primary_keys.is_empty());
        let layout = TableLayout::from_partitions("", None);
        assert!(layout.range_partitions.is_empty() && layout.primary_keys.is_empty());
    }

    #[test]
    fn add_appends_a_nullable_column() {
        let added = check_evolution(
            &schema(),
            &ColumnChange::Add(Field::new("note", DataType::Utf8, true)),
            &layout(),
        )
        .unwrap();
        assert_eq!(added.fields().len(), 6);
        assert_eq!(added.field(5).name(), "note");
        assert!(added.field(5).is_nullable());
    }

    #[test]
    fn add_refuses_not_null_nested_and_duplicates() {
        let error = check_evolution(
            &schema(),
            &ColumnChange::Add(Field::new("note", DataType::Utf8, false)),
            &layout(),
        )
        .unwrap_err();
        assert!(matches!(error, EvolutionError::Unsupported(_)), "{error}");

        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let error = check_evolution(
            &schema(),
            &ColumnChange::Add(Field::new("tags", list, true)),
            &layout(),
        )
        .unwrap_err();
        assert!(matches!(error, EvolutionError::Unsupported(_)), "{error}");

        let error = check_evolution(
            &schema(),
            &ColumnChange::Add(Field::new("qty", DataType::Int32, true)),
            &layout(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            EvolutionError::DuplicateColumn {
                name: "qty".to_string(),
                same_type: true
            }
        );
        let error = check_evolution(
            &schema(),
            &ColumnChange::Add(Field::new("qty", DataType::Int64, true)),
            &layout(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            EvolutionError::DuplicateColumn {
                name: "qty".to_string(),
                same_type: false
            }
        );
        // A case-only difference is ambiguous to case-insensitive engines.
        let error = check_evolution(
            &schema(),
            &ColumnChange::Add(Field::new("rowkinds", DataType::Utf8, true)),
            &layout(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            EvolutionError::DuplicateColumn {
                name: "rowKinds".to_string(),
                same_type: false
            }
        );
    }

    #[test]
    fn widen_follows_the_lossless_list() {
        let widened = check_evolution(
            &schema(),
            &ColumnChange::Widen {
                name: "qty".to_string(),
                to: DataType::Int64,
            },
            &layout(),
        )
        .unwrap();
        assert_eq!(widened.field(2).data_type(), &DataType::Int64);
        assert!(!widened.field(2).is_nullable(), "nullability is kept");

        let widened = check_evolution(
            &schema(),
            &ColumnChange::Widen {
                name: "price".to_string(),
                to: DataType::Decimal128(12, 3),
            },
            &layout(),
        )
        .unwrap();
        assert_eq!(widened.field(3).data_type(), &DataType::Decimal128(12, 3));

        for to in [
            DataType::Int16,
            DataType::Utf8,
            DataType::Float64,
            DataType::Decimal128(38, 0),
        ] {
            let error = check_evolution(
                &schema(),
                &ColumnChange::Widen {
                    name: "qty".to_string(),
                    to: to.clone(),
                },
                &layout(),
            )
            .unwrap_err();
            assert!(
                matches!(error, EvolutionError::Unsupported(_)),
                "{to}: {error}"
            );
        }
        // Losing a fraction digit or an integer digit is not a widening.
        for to in [DataType::Decimal128(11, 1), DataType::Decimal128(10, 3)] {
            assert!(
                check_evolution(
                    &schema(),
                    &ColumnChange::Widen {
                        name: "price".to_string(),
                        to: to.clone(),
                    },
                    &layout(),
                )
                .is_err(),
                "{to}"
            );
        }
    }

    #[test]
    fn same_type_and_nullable_changes_are_no_ops() {
        let current = schema();
        assert_eq!(
            check_evolution(
                &current,
                &ColumnChange::Widen {
                    name: "id".to_string(),
                    to: DataType::Int64,
                },
                &layout(),
            )
            .unwrap(),
            current
        );
        assert_eq!(
            check_evolution(
                &current,
                &ColumnChange::DropNotNull("price".to_string()),
                &layout(),
            )
            .unwrap(),
            current
        );
    }

    #[test]
    fn layout_columns_cannot_change() {
        for (name, role) in [
            ("id", ColumnRole::PrimaryKey),
            ("part", ColumnRole::RangePartition),
        ] {
            let error = check_evolution(
                &schema(),
                &ColumnChange::DropNotNull(name.to_string()),
                &layout(),
            )
            .unwrap_err();
            assert_eq!(
                error,
                EvolutionError::LayoutColumn {
                    name: name.to_string(),
                    role
                }
            );
        }
        let error = check_evolution(
            &schema(),
            &ColumnChange::Widen {
                name: "id".to_string(),
                to: DataType::Decimal128(38, 0),
            },
            &layout(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            EvolutionError::LayoutColumn {
                role: ColumnRole::PrimaryKey,
                ..
            }
        ));
        let error = check_evolution(
            &schema(),
            &ColumnChange::Widen {
                name: "rowKinds".to_string(),
                to: DataType::LargeUtf8,
            },
            &layout(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            EvolutionError::LayoutColumn {
                role: ColumnRole::CdcChange,
                ..
            }
        ));
    }

    #[test]
    fn drop_not_null_relaxes_a_value_column() {
        let relaxed = check_evolution(
            &schema(),
            &ColumnChange::DropNotNull("qty".to_string()),
            &layout(),
        )
        .unwrap();
        assert!(relaxed.field(2).is_nullable());
    }

    #[test]
    fn unknown_columns_are_reported() {
        let error = check_evolution(
            &schema(),
            &ColumnChange::DropNotNull("missing".to_string()),
            &layout(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            EvolutionError::UndefinedColumn("missing".to_string())
        );
    }
}
