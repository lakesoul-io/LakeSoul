// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! 从 Arrow RecordBatch 中提取向量数据的工具函数。

use crate::rabitq::IdAndVecBatch;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, UInt64Array,
};
use arrow_schema::DataType;
use lakesoul_io::Result;
use rootcause::{bail, report};

/// 从 RecordBatch 中提取 PK 列（u64）和向量列（Float32），构造 `IdAndVecBatch`。
///
/// # 参数
/// - `batch`: Arrow RecordBatch，包含 PK 列和向量列
/// - `pk_column`: PK 列名，类型必须是 `UInt64` 或 `Int64`
/// - `vector_column`: 向量列名，类型必须是 `FixedSizeList<Float32, dim>`
/// - `dim`: 向量的维度
///
/// # 返回
/// `IdAndVecBatch`，其中 `ids` 是 u64 向量，`vectors` 是展平为 `[n * dim]` 的 f32 数组
pub fn extract_vector_batch(
    batch: &RecordBatch,
    pk_column: &str,
    vector_column: &str,
    dim: usize,
) -> Result<IdAndVecBatch> {
    if batch.num_rows() == 0 {
        return Ok(IdAndVecBatch {
            ids: Vec::new(),
            vectors: Vec::new(),
        });
    }

    // 1. 提取 PK 列
    let pk_array = batch
        .column_by_name(pk_column)
        .ok_or_else(|| report!("PK column '{}' not found in batch", pk_column))?;

    let ids: Vec<u64> = match pk_array.data_type() {
        DataType::UInt64 => {
            let arr =
                pk_array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        report!(
                            "failed to downcast PK column '{}' to UInt64Array",
                            pk_column
                        )
                    })?;
            arr.values().to_vec()
        }
        DataType::Int64 => {
            let arr =
                pk_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        report!(
                            "failed to downcast PK column '{}' to Int64Array",
                            pk_column
                        )
                    })?;
            arr.values().iter().map(|&v| v as u64).collect()
        }
        other => {
            bail!(
                "vector index PK column '{}' must be UInt64 or Int64, got {:?}",
                pk_column,
                other
            );
        }
    };

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
                let floats = value_array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        report!(
                            "vector column '{}' values must be Float32, got {:?}",
                            vector_column,
                            value_array.data_type()
                        )
                    })?;
                vectors.extend_from_slice(floats.values());
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
                    let floats = value_array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| report!("vector column values must be Float32"))?;
                    vectors.extend_from_slice(floats.values());
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
                "vector column '{}' must be FixedSizeList<Float32> or List<Float32>, got {:?}",
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
    use arrow_array::types::Float32Type;
    use std::sync::Arc;

    fn make_fixed_size_list_batch(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        dim: usize,
    ) -> RecordBatch {
        let id_array = Arc::new(UInt64Array::from(ids)) as arrow_array::ArrayRef;
        let n = vectors.len();

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
        let result = extract_vector_batch(&batch, "id", "vec", 2).unwrap();
        assert_eq!(result.ids, vec![1, 2, 3]);
        assert_eq!(result.vectors, vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6]);
    }

    #[test]
    fn test_extract_empty_batch() {
        let batch = make_fixed_size_list_batch(vec![], vec![], 2);
        let result = extract_vector_batch(&batch, "id", "vec", 2).unwrap();
        assert!(result.ids.is_empty());
        assert!(result.vectors.is_empty());
    }

    #[test]
    fn test_dimension_mismatch() {
        let batch = make_fixed_size_list_batch(vec![1], vec![vec![0.1, 0.2]], 2);
        // Pass wrong dim
        let result = extract_vector_batch(&batch, "id", "vec", 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_column() {
        let batch = make_fixed_size_list_batch(vec![1], vec![vec![0.1]], 1);
        let result = extract_vector_batch(&batch, "nonexistent", "vec", 1);
        assert!(result.is_err());
    }
}
