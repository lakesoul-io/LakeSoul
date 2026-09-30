// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! 从 Arrow RecordBatch 中提取向量数据的工具函数。

use crate::Result;
use crate::index::key::KeyCodec;
use arrow_array::{
    Array, FixedSizeListArray, Float16Array, Float32Array, Float64Array, RecordBatch,
};
use arrow_schema::DataType;
use lakesoul_vector::IdAndVecBatch;
use rootcause::{bail, report};

/// Casts the values of a Float16/Float32/Float64 array into `f32` (SQL
/// `DOUBLE[]` tables store Float64 and are converted on the way into the
/// index; `Float16` tables store half precision and are widened to `f32`).
fn float32_values(value_array: &dyn Array) -> Result<Vec<f32>> {
    if let Some(arr) = value_array.as_any().downcast_ref::<Float32Array>() {
        return Ok(arr.values().to_vec());
    }
    if let Some(arr) = value_array.as_any().downcast_ref::<Float64Array>() {
        return Ok(arr.values().iter().map(|&v| v as f32).collect());
    }
    if let Some(arr) = value_array.as_any().downcast_ref::<Float16Array>() {
        return Ok(arr.values().iter().map(|v| v.to_f32()).collect());
    }
    bail!(
        "vector column values must be Float16, Float32 or Float64, got {:?}",
        value_array.data_type()
    )
}

/// 从 RecordBatch 中提取主键 key 和向量列，构造 `IdAndVecBatch`。
///
/// 主键列由 [`KeyCodec`] 编码成 arrow-Row 字节 key（支持任意类型与复合主键）；
/// Float16/Float64 向量值会被转换为 Float32 后写入索引。
///
/// # 参数
/// - `batch`: Arrow RecordBatch，包含 PK 列和向量列
/// - `codec`: 主键列的 key 编解码器，其列必须存在于 `batch` 中且非空
/// - `vector_column`: 向量列名，类型必须是 `FixedSizeList<Float16/Float32/Float64, dim>` 或等长 `List<Float16/Float32/Float64>`
/// - `dim`: 向量的维度
///
/// # 返回
/// `IdAndVecBatch`，其中 `ids` 是编码后的主键 key，`vectors` 是展平为 `[n * dim]` 的 f32 数组
pub fn extract_vector_batch(
    batch: &RecordBatch,
    codec: &KeyCodec,
    vector_column: &str,
    dim: usize,
) -> Result<IdAndVecBatch> {
    if batch.num_rows() == 0 {
        return Ok(IdAndVecBatch {
            ids: Vec::new(),
            vectors: Vec::new(),
        });
    }

    // 1. 提取主键 key
    let ids = codec.encode_batch(batch)?;

    let n = ids.len();

    // 2. 提取向量列
    let vec_array = batch
        .column_by_name(vector_column)
        .ok_or_else(|| report!("vector column '{}' not found in batch", vector_column))?;

    let mut vectors = Vec::<f32>::with_capacity(n * dim);

    match vec_array.data_type() {
        DataType::FixedSizeList(_field, list_dim) => {
            let list_dim = *list_dim as usize;
            if list_dim != dim {
                bail!(
                    "vector column '{}' dimension mismatch: expected {}, got {}",
                    vector_column,
                    dim,
                    list_dim
                );
            }
            let fla = vec_array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    report!(
                        "failed to downcast vector column '{}' to FixedSizeListArray",
                        vector_column
                    )
                })?;

            for i in 0..fla.len() {
                let value_array = fla.value(i);
                let floats = float32_values(value_array.as_ref())?;
                vectors.extend_from_slice(&floats);
            }
        }
        DataType::List(_field) | DataType::LargeList(_field) => {
            // 变长列表：逐个提取 float32 值
            use arrow_array::{
                GenericListArray, LargeListArray, ListArray, OffsetSizeTrait,
            };

            // 根据具体类型处理
            fn extract_from_list<O: OffsetSizeTrait>(
                list_array: &GenericListArray<O>,
                dim: usize,
            ) -> Result<Vec<f32>> {
                let n = list_array.len();
                let mut vectors = Vec::<f32>::with_capacity(n * dim);
                for i in 0..list_array.len() {
                    let value_array = list_array.value(i);
                    if value_array.len() != dim {
                        bail!(
                            "vector column dimension mismatch at row {}: expected {}, got {}",
                            i,
                            dim,
                            value_array.len()
                        );
                    }
                    let floats = float32_values(value_array.as_ref())?;
                    vectors.extend_from_slice(&floats);
                }
                Ok(vectors)
            }

            vectors = match vec_array.data_type() {
                DataType::List(_) => {
                    let la = vec_array
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| report!("failed to downcast to ListArray"))?;
                    extract_from_list::<i32>(la, dim)?
                }
                DataType::LargeList(_) => {
                    let la = vec_array
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .ok_or_else(|| report!("failed to downcast to LargeListArray"))?;
                    extract_from_list::<i64>(la, dim)?
                }
                _ => unreachable!(),
            };
        }
        other => {
            bail!(
                "vector column '{}' must be FixedSizeList<Float16/Float32/Float64> or List<Float16/Float32/Float64>, got {:?}",
                vector_column,
                other
            );
        }
    }

    if vectors.len() != n * dim {
        bail!(
            "vector batch size mismatch: {} rows × {} dim = {} floats expected, got {}",
            n,
            dim,
            n * dim,
            vectors.len()
        );
    }

    Ok(IdAndVecBatch { ids, vectors })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::key::KeyLayout;
    use arrow_array::{Int64Array, UInt64Array};
    use lakesoul_vector::IndexKey;
    use std::sync::Arc;

    /// Codec over the single `id UInt64` key column the helpers build.
    fn u64_codec() -> KeyCodec {
        KeyCodec::new(KeyLayout::new(
            vec!["id".to_string()],
            vec![DataType::UInt64],
        ))
        .unwrap()
    }

    fn decode_u64(codec: &KeyCodec, ids: &[IndexKey]) -> Vec<u64> {
        if ids.is_empty() {
            return Vec::new();
        }
        let arrays = codec.decode(ids).unwrap();
        let values = arrays[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        (0..values.len()).map(|i| values.value(i)).collect()
    }

    fn make_fixed_size_list_batch(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    ) -> RecordBatch {
        let id_array = Arc::new(UInt64Array::from(ids)) as arrow_array::ArrayRef;

        // Build FixedSizeList: flatten all values
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let value_array = Float32Array::from(flat);
        let list_array = FixedSizeListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Float32, true)),
            dim as i32,
            Arc::new(value_array),
            None,
        );

        RecordBatch::try_from_iter(vec![
            ("id", id_array),
            ("vec", Arc::new(list_array) as arrow_array::ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn test_extract_u64_pk() {
        let batch = make_fixed_size_list_batch(
            vec![1, 2, 3],
            vec![vec![0.1, 0.2], vec![0.3, 0.4], vec![0.5, 0.6]],
            2,
        );
        let codec = u64_codec();
        let result = extract_vector_batch(&batch, &codec, "vec", 2).unwrap();
        assert_eq!(decode_u64(&codec, &result.ids), vec![1, 2, 3]);
        assert_eq!(result.vectors, vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6]);
    }

    #[test]
    fn test_extract_empty_batch() {
        let batch = make_fixed_size_list_batch(vec![], vec![], 2);
        let result = extract_vector_batch(&batch, &u64_codec(), "vec", 2).unwrap();
        assert!(result.ids.is_empty());
        assert!(result.vectors.is_empty());
    }

    #[test]
    fn test_dimension_mismatch() {
        let batch = make_fixed_size_list_batch(vec![1], vec![vec![0.1, 0.2]], 2);
        // Pass wrong dim
        let result = extract_vector_batch(&batch, &u64_codec(), "vec", 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_column() {
        let batch = make_fixed_size_list_batch(vec![1], vec![vec![0.1]], 1);
        let codec = KeyCodec::new(KeyLayout::new(
            vec!["nonexistent".to_string()],
            vec![DataType::UInt64],
        ))
        .unwrap();
        let result = extract_vector_batch(&batch, &codec, "vec", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_composite_pk() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![1i64, 2])) as arrow_array::ArrayRef,
            ),
            (
                "tenant",
                Arc::new(arrow_array::StringArray::from(vec!["a", "b"]))
                    as arrow_array::ArrayRef,
            ),
            (
                "vec",
                Arc::new(FixedSizeListArray::new(
                    Arc::new(arrow_schema::Field::new("item", DataType::Float32, true)),
                    2,
                    Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                    None,
                )) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let codec = KeyCodec::new(KeyLayout::new(
            vec!["id".to_string(), "tenant".to_string()],
            vec![DataType::Int64, DataType::Utf8],
        ))
        .unwrap();
        let result = extract_vector_batch(&batch, &codec, "vec", 2).unwrap();
        assert_eq!(result.ids.len(), 2);
        assert_ne!(result.ids[0], result.ids[1]);
        let arrays = codec.decode(&result.ids).unwrap();
        let ids = arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        let tenants = arrays[1]
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(tenants.value(0), "a");
        assert_eq!(tenants.value(1), "b");
    }

    fn make_fixed_size_list_f64_batch(
        ids: Vec<u64>,
        vectors: Vec<Vec<f64>>,
        dim: usize,
    ) -> RecordBatch {
        let id_array = Arc::new(UInt64Array::from(ids)) as arrow_array::ArrayRef;
        let flat: Vec<f64> = vectors.iter().flatten().copied().collect();
        let value_array = Float64Array::from(flat);
        let list_array = FixedSizeListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Float64, true)),
            dim as i32,
            Arc::new(value_array),
            None,
        );
        RecordBatch::try_from_iter(vec![
            ("id", id_array),
            ("vec", Arc::new(list_array) as arrow_array::ArrayRef),
        ])
        .unwrap()
    }

    fn make_list_f64_batch(ids: Vec<u64>, vectors: Vec<Vec<f64>>) -> RecordBatch {
        let id_array = Arc::new(UInt64Array::from(ids)) as arrow_array::ArrayRef;
        let values: Vec<f64> = vectors.iter().flatten().copied().collect();
        let offsets: Vec<i32> = std::iter::once(0)
            .chain(vectors.iter().map(|v| v.len() as i32))
            .scan(0i32, |acc, len| {
                *acc += len;
                Some(*acc)
            })
            .collect();
        let value_array = Float64Array::from(values);
        let list_array = arrow_array::ListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Float64, true)),
            arrow_buffer::OffsetBuffer::new(offsets.into()),
            Arc::new(value_array),
            None,
        );
        RecordBatch::try_from_iter(vec![
            ("id", id_array),
            ("vec", Arc::new(list_array) as arrow_array::ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn test_extract_f64_fixed_size_list_converts_to_f32() {
        let batch = make_fixed_size_list_f64_batch(
            vec![1, 2],
            vec![vec![0.1, 0.2], vec![0.3, 0.4]],
            2,
        );
        let codec = u64_codec();
        let result = extract_vector_batch(&batch, &codec, "vec", 2).unwrap();
        assert_eq!(decode_u64(&codec, &result.ids), vec![1, 2]);
        assert_eq!(result.vectors, vec![0.1f32, 0.2, 0.3, 0.4]);
    }

    #[test]
    fn test_extract_f64_var_size_list_converts_to_f32() {
        let batch = make_list_f64_batch(vec![7], vec![vec![1.5, -2.5]]);
        let codec = u64_codec();
        let result = extract_vector_batch(&batch, &codec, "vec", 2).unwrap();
        assert_eq!(decode_u64(&codec, &result.ids), vec![7]);
        assert_eq!(result.vectors, vec![1.5f32, -2.5]);
    }

    fn make_fixed_size_list_f16_batch(
        ids: Vec<u64>,
        vectors: Vec<Vec<half::f16>>,
        dim: usize,
    ) -> RecordBatch {
        let id_array = Arc::new(UInt64Array::from(ids)) as arrow_array::ArrayRef;
        let flat: Vec<half::f16> = vectors.iter().flatten().copied().collect();
        let value_array = Float16Array::from(flat);
        let list_array = FixedSizeListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Float16, true)),
            dim as i32,
            Arc::new(value_array),
            None,
        );
        RecordBatch::try_from_iter(vec![
            ("id", id_array),
            ("vec", Arc::new(list_array) as arrow_array::ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn test_extract_f16_fixed_size_list_converts_to_f32() {
        let batch = make_fixed_size_list_f16_batch(
            vec![1, 2],
            vec![
                vec![half::f16::from_f32(1.5), half::f16::from_f32(-2.5)],
                vec![half::f16::from_f32(0.25), half::f16::from_f32(2.0)],
            ],
            2,
        );
        let codec = u64_codec();
        let result = extract_vector_batch(&batch, &codec, "vec", 2).unwrap();
        assert_eq!(decode_u64(&codec, &result.ids), vec![1, 2]);
        // 1.5 / -2.5 / 0.25 / 2.0 are exactly representable in f16.
        assert_eq!(result.vectors, vec![1.5f32, -2.5, 0.25, 2.0]);
    }

    #[test]
    fn test_extract_f16_var_size_list_converts_to_f32() {
        // The SQL scenario stores vectors as `List<Float16>`.
        let value_array =
            Float16Array::from(vec![half::f16::from_f32(1.5), half::f16::from_f32(-2.5)]);
        let offsets: Vec<i32> = vec![0, 2];
        let list_array = arrow_array::ListArray::new(
            Arc::new(arrow_schema::Field::new("item", DataType::Float16, true)),
            arrow_buffer::OffsetBuffer::new(offsets.into()),
            Arc::new(value_array),
            None,
        );
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(UInt64Array::from(vec![7u64])) as arrow_array::ArrayRef,
            ),
            ("vec", Arc::new(list_array) as arrow_array::ArrayRef),
        ])
        .unwrap();
        let codec = u64_codec();
        let result = extract_vector_batch(&batch, &codec, "vec", 2).unwrap();
        assert_eq!(decode_u64(&codec, &result.ids), vec![7]);
        assert_eq!(result.vectors, vec![1.5f32, -2.5]);
    }
}
