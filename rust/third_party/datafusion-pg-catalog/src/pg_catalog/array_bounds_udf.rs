//! PostgreSQL `array_upper` / `array_lower` scalar functions.
//!
//! DataFusion does not ship these, so any client query containing them (psql,
//! dbeaver, grafana) was rejected with "Invalid function 'array_upper'". They
//! used to only "work" because the exact query fragments were on the parser's
//! token blacklist and got swapped out before execution.
//!
//! Arrow's list types (`List`/`LargeList`/`FixedSizeList`) are always 1-based
//! and contiguous, so for any present dimension:
//!   * `array_lower(arr, dim)` = 1
//!   * `array_upper(arr, dim)` = `array_length(arr, dim)`
//!
//! Following Postgres, both return NULL when the array is NULL, when `dim` is
//! out of range (< 1 or beyond the array's dimensionality), or when the array
//! is empty on the requested dimension.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Builder};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

/// Length of a list at a given 1-based dimension, with Postgres NULL semantics.
///
/// Returns `Some(len)` only when `dim` is in range and that dimension is
/// non-empty; returns `None` for a null array, an out-of-range dimension, or an
/// empty dimension. This matches the behavior of Postgres `array_length` /
/// `array_upper` / `array_lower`, which all yield NULL in those cases.
fn length_at_dim(arr: Option<ArrayRef>, dim: i64) -> Option<usize> {
    if dim < 1 {
        return None;
    }
    let mut value = arr?;
    let mut current = 1;
    loop {
        let len = value.len();
        if current == dim {
            return if len == 0 { None } else { Some(len) };
        }
        // Cannot descend further into an empty or non-list dimension.
        if len == 0 {
            return None;
        }
        value = match value.data_type() {
            DataType::List(_) => value.as_list::<i32>().value(0),
            DataType::LargeList(_) => value.as_list::<i64>().value(0),
            DataType::FixedSizeList(_, _) => value.as_fixed_size_list().value(0),
            _ => return None,
        };
        current += 1;
    }
}

/// Read the per-row dimension argument as `Option<i64>`, accepting either
/// Postgres `integer` (Int32) or `bigint` (Int64).
fn extract_dims(arg: &ArrayRef) -> Result<Vec<Option<i64>>, DataFusionError> {
    let dims = match arg.data_type() {
        DataType::Int32 => arg
            .as_primitive::<Int32Type>()
            .iter()
            .map(|v| v.map(|x| x as i64))
            .collect(),
        DataType::Int64 => arg.as_primitive::<Int64Type>().iter().collect(),
        other => {
            return Err(DataFusionError::Internal(format!(
                "array dimension must be an integer, got {other}"
            )));
        }
    };
    Ok(dims)
}

/// Core implementation shared by `array_upper` and `array_lower`.
///
/// `upper` selects the upper bound (the length at the dimension); otherwise the
/// lower bound (always 1 for Arrow's 1-based lists) is produced.
fn array_bounds_impl(
    args: &[ColumnarValue],
    upper: bool,
) -> Result<ColumnarValue, DataFusionError> {
    let args = ColumnarValue::values_to_arrays(args)?;
    if args.len() != 2 {
        return Err(DataFusionError::Internal(format!(
            "array_upper/array_lower expect exactly 2 arguments, got {}",
            args.len()
        )));
    }
    let list_arr = &args[0];
    let dims = extract_dims(&args[1])?;

    let rows = list_arr.len();
    let mut out = Int32Builder::with_capacity(rows);

    let build = |row: Option<ArrayRef>, dim: Option<i64>, out: &mut Int32Builder| match dim {
        // Null dimension -> NULL result.
        None => out.append_null(),
        Some(d) => match length_at_dim(row, d) {
            // Empty / out-of-range dimension -> NULL (Postgres semantics).
            None => out.append_null(),
            Some(len) if upper => out.append_value(len as i32),
            // Lower bound of a present 1-based dimension is always 1.
            Some(_) => out.append_value(1),
        },
    };

    match list_arr.data_type() {
        DataType::List(_) => {
            let l = list_arr.as_list::<i32>();
            for (row, dim) in l.iter().zip(dims.iter().copied()) {
                build(row, dim, &mut out);
            }
        }
        DataType::LargeList(_) => {
            let l = list_arr.as_list::<i64>();
            for (row, dim) in l.iter().zip(dims.iter().copied()) {
                build(row, dim, &mut out);
            }
        }
        DataType::FixedSizeList(_, _) => {
            let l = list_arr.as_fixed_size_list();
            for (row, dim) in l.iter().zip(dims.iter().copied()) {
                build(row, dim, &mut out);
            }
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "array_upper/array_lower expect a list argument, got {other}"
            )));
        }
    }

    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

fn two_arg_array_signature() -> Signature {
    Signature::one_of(
        vec![TypeSignature::ArraySignature(
            ArrayFunctionSignature::Array {
                arguments: vec![ArrayFunctionArgument::Array, ArrayFunctionArgument::Index],
                array_coercion: None,
            },
        )],
        Volatility::Immutable,
    )
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ArrayUpperUDF {
    signature: Signature,
}

impl Default for ArrayUpperUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayUpperUDF {
    pub fn new() -> Self {
        Self {
            signature: two_arg_array_signature(),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl ScalarUDFImpl for ArrayUpperUDF {
    fn name(&self) -> &str {
        "array_upper"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        array_bounds_impl(&args.args, true)
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ArrayLowerUDF {
    signature: Signature,
}

impl Default for ArrayLowerUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayLowerUDF {
    pub fn new() -> Self {
        Self {
            signature: two_arg_array_signature(),
        }
    }

    pub fn into_scalar_udf(self) -> ScalarUDF {
        ScalarUDF::new_from_impl(self)
    }
}

impl ScalarUDFImpl for ArrayLowerUDF {
    fn name(&self) -> &str {
        "array_lower"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        array_bounds_impl(&args.args, false)
    }
}

/// Create the PostgreSQL `array_upper` UDF.
pub fn create_array_upper_udf() -> ScalarUDF {
    ArrayUpperUDF::new().into_scalar_udf()
}

/// Create the PostgreSQL `array_lower` UDF.
pub fn create_array_lower_udf() -> ScalarUDF {
    ArrayLowerUDF::new().into_scalar_udf()
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Int32Builder, ListBuilder};

    /// Build a list array where each entry is one row. `Some(vec![])` is an empty
    /// (non-null) list, `Some(vec![1,2,3])` is `[1,2,3]`, and `None` is a NULL
    /// list value.
    fn int_list(rows: &[Option<Vec<i32>>]) -> ArrayRef {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                Some(elems) => {
                    for v in elems {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }

    fn run_bounds(list: ArrayRef, dim: i64, upper: bool) -> Vec<Option<i32>> {
        let dim_arr = Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            dim;
            list.len()
        ]));
        let args = vec![ColumnarValue::Array(list), ColumnarValue::Array(dim_arr)];
        let out = array_bounds_impl(&args, upper).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => unreachable!(),
        };
        arr.as_primitive::<Int32Type>().iter().collect()
    }

    #[test]
    fn test_length_at_dim() {
        let three = int_list(&[Some(vec![1, 2, 3])]);
        // length_at_dim operates on a single extracted row's list value.
        let row = three.as_list::<i32>().value(0);
        assert_eq!(length_at_dim(Some(row.clone()), 1), Some(3));
        // dim 2 is out of range for a 1-D list
        assert_eq!(length_at_dim(Some(row), 2), None);
        // null array
        assert_eq!(length_at_dim(None, 1), None);
        // dim < 1
        let one = int_list(&[Some(vec![1])]);
        assert_eq!(length_at_dim(Some(one.as_list::<i32>().value(0)), 0), None);
        // empty list at dim 1 -> None (Postgres: empty array has no length)
        let empty = int_list(&[Some(vec![])]);
        assert_eq!(
            length_at_dim(Some(empty.as_list::<i32>().value(0)), 1),
            None
        );
    }

    #[test]
    fn test_array_upper() {
        let arr = int_list(&[Some(vec![1, 2, 3])]);
        assert_eq!(run_bounds(arr, 1, true), vec![Some(3)]);

        // empty (non-null) array -> NULL (Postgres semantics)
        let arr = int_list(&[Some(vec![])]);
        assert_eq!(run_bounds(arr, 1, true), vec![None]);

        // null array value -> NULL
        let arr = int_list(&[None]);
        assert_eq!(run_bounds(arr, 1, true), vec![None]);
    }

    #[test]
    fn test_array_lower() {
        let arr = int_list(&[Some(vec![1, 2, 3])]);
        assert_eq!(run_bounds(arr, 1, false), vec![Some(1)]);

        // empty (non-null) array -> NULL
        let arr = int_list(&[Some(vec![])]);
        assert_eq!(run_bounds(arr, 1, false), vec![None]);

        // null array value -> NULL
        let arr = int_list(&[None]);
        assert_eq!(run_bounds(arr, 1, false), vec![None]);
    }

    #[test]
    fn test_out_of_range_dim() {
        let arr = int_list(&[Some(vec![1, 2, 3])]);
        assert_eq!(run_bounds(arr.clone(), 2, true), vec![None]);
        assert_eq!(run_bounds(arr.clone(), 2, false), vec![None]);
        assert_eq!(run_bounds(arr, 0, true), vec![None]);
    }

    #[test]
    fn test_multiple_rows() {
        // Per-row independence.
        let arr = int_list(&[Some(vec![1]), Some(vec![1, 2]), Some(vec![]), None]);
        assert_eq!(run_bounds(arr, 1, true), vec![Some(1), Some(2), None, None]);
    }
}
