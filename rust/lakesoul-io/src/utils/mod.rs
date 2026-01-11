use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use rand::{Rng, distr::Alphanumeric};

pub mod hash;

pub fn random_str(len: usize) -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn type_name<T>(_: &T) -> &'static str {
    std::any::type_name::<T>()
}

// Generate a unique file name for Lakesoul
pub fn lakesoul_file_name(writer_id: &str, partition: usize) -> String {
    format!("part-{}_{:0>4}.parquet", writer_id, partition)
}

/// According to the schema, create a random record batch with the given row number.
pub fn create_random_batch(
    schema: SchemaRef,
    row_num: usize,
    null_prop: f64,
) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let is_nullable = field.is_nullable();
        let array: ArrayRef = match field.data_type() {
            DataType::Boolean => {
                let data: Vec<Option<bool>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(BooleanArray::from(data))
            }
            DataType::Int8 => {
                let data: Vec<Option<i8>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Int8Array::from(data))
            }
            DataType::Int16 => {
                let data: Vec<Option<i16>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Int16Array::from(data))
            }
            DataType::Int32 => {
                let data: Vec<Option<i32>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Int32Array::from(data))
            }
            DataType::Int64 => {
                let data: Vec<Option<i64>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Int64Array::from(data))
            }
            DataType::UInt8 => {
                let data: Vec<Option<u8>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(UInt8Array::from(data))
            }
            DataType::UInt16 => {
                let data: Vec<Option<u16>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(UInt16Array::from(data))
            }
            DataType::UInt32 => {
                let data: Vec<Option<u32>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(UInt32Array::from(data))
            }
            DataType::UInt64 => {
                let data: Vec<Option<u64>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(UInt64Array::from(data))
            }
            DataType::Float32 => {
                let data: Vec<Option<f32>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Float32Array::from(data))
            }
            DataType::Float64 => {
                let data: Vec<Option<f64>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Float64Array::from(data))
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                let data: Vec<Option<String>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            let len = rand::rng().random_range(0..20);
                            Some(random_str(len))
                        }
                    })
                    .collect();
                Arc::new(StringArray::from(data))
            }
            DataType::Date32 => {
                let data: Vec<Option<i32>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Date32Array::from(data))
            }
            DataType::Date64 => {
                let data: Vec<Option<i64>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(Date64Array::from(data))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let data: Vec<Option<i64>> = (0..row_num)
                    .map(|_| {
                        if is_nullable && rand::rng().random_bool(null_prop) {
                            None
                        } else {
                            Some(rand::random())
                        }
                    })
                    .collect();
                Arc::new(TimestampMicrosecondArray::from(data))
            }
            _ => unimplemented!(
                "Unsupported data type for random generation: {:?}",
                field.data_type()
            ),
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns).expect("Failed to create record batch")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_str_test() {
        let str = random_str(10);
        println!("{str}");
        assert_eq!(str.len(), 10);
    }
}
