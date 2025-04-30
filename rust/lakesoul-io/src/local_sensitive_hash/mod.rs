use arrow::array::{Array, Float32Array, Float64Array, GenericListArray, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result};
use ndarray::{concatenate, s, Array2, ArrayView2, Axis};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::ptr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LSH {
    nbits: u32,
    d: u32,
    seed: u64,
}

impl LSH {
    pub fn new(nbits: u32, d: u32, seed: u64) -> Self {
        Self { nbits, d, seed }
    }

    // generate random digit with fixed seed
    fn create_rng_with_seed(&self) -> StdRng {
        StdRng::seed_from_u64(self.seed)
    }

    // generate random planes
    fn generate_random_array(&self) -> Result<Array2<f64>> {
        if self.nbits > 0 {
            if self.d > 0 {
                let mut rng = self.create_rng_with_seed();
                let random_array =
                    Array2::from_shape_fn((self.nbits as usize, self.d as usize), |_| rng.gen_range(-1.0..1.0));
                Ok(random_array)
            } else {
                Err(DataFusionError::Internal(
                    "the dimension you input in the config must be greater than 0".to_string(),
                ))
            }
        } else {
            Err(DataFusionError::Internal(
                "the number of bits used for binary encoding must be greater than 0".to_string(),
            ))
        }
    }

    // project the input data
    fn project(&self, input_data: &ListArray, random_plans: &Result<Array2<f64>>) -> Result<Array2<f64>> {
        let list_len = input_data.len();
        assert!(list_len > 0, "the length of input data must be large than 0");
        let dimension_len = input_data.value(0).len();

        let input_values = if let Some(values) = input_data.values().as_any().downcast_ref::<Float32Array>() {
            let float64_values: Vec<f64> = values.iter().map(|x| x.unwrap() as f64).collect();
            Float64Array::from(float64_values)
        } else if let Some(values) = input_data.values().as_any().downcast_ref::<Float64Array>() {
            values.clone()
        } else {
            return Err(DataFusionError::Internal(
                "Unsupported data type in ListArray.".to_string(),
            ));
        };

        let mut re_array2 = Array2::<f64>::zeros((list_len, dimension_len));

        unsafe {
            let data_ptr = input_values.values().as_ptr();
            let data_size = list_len * dimension_len;
            ptr::copy_nonoverlapping(data_ptr, re_array2.as_mut_ptr(), data_size);
        }

        match random_plans {
            Ok(random_array) => {
                assert!(
                    re_array2.shape()[1] == random_array.shape()[1],
                    "the dimension corresponding to the matrix must be the same"
                );

                let batch_size = 1000;
                let num_batches = re_array2.shape()[0] / batch_size;
                let remaining_rows = re_array2.shape()[0] % batch_size;
                let mut result = vec![];

                for batch_idx in 0..num_batches {
                    let batch_start = batch_idx * batch_size;
                    let batch_end = batch_start + batch_size;

                    let current_batch = re_array2.slice(s![batch_start..batch_end, ..]);
                    let random_projection = current_batch.dot(&random_array.t());

                    result.push(random_projection);
                }

                if remaining_rows > 0 {
                    let batch_start = num_batches * batch_size;
                    let batch_end = batch_start + remaining_rows;

                    let remaining_batch = re_array2.slice(s![batch_start..batch_end, ..]);
                    let random_projection = remaining_batch.dot(&random_array.t());

                    result.push(random_projection);
                }

                let result_views: Vec<ArrayView2<f64>> = result.iter().map(|arr| ArrayView2::from(arr)).collect();

                let final_result = concatenate(Axis(0), &result_views).expect("Failed to concatenate results");

                Ok(final_result)
            }
            Err(e) => {
                eprintln!("Error:{}", e);
                Err(DataFusionError::Internal(e.to_string()))
            }
        }
    }

    fn convert_vec_to_byte_u64(array: Vec<Vec<u64>>) -> ListArray {
        let field = Arc::new(Field::new("element", DataType::Int64, true));
        let values = Int64Array::from(array.iter().flatten().map(|&x| x as i64).collect::<Vec<i64>>());
        let mut offsets = vec![];
        for subarray in array {
            let current_offset = subarray.len() as usize;
            offsets.push(current_offset);
        }
        let offsets_buffer = OffsetBuffer::from_lengths(offsets);
        let list_array = GenericListArray::try_new(field, offsets_buffer, Arc::new(values), None)
            .expect("can not create list_array");
        list_array
    }

    fn convert_array_to_u64_vec<T>(array: &Array2<f64>) -> Vec<Vec<T>>
    where
        T: TryFrom<u64> + Copy,
        <T as TryFrom<u64>>::Error: std::fmt::Debug,
    {
        let binary_encode: Vec<Vec<u64>> = array
            .axis_iter(ndarray::Axis(0))
            .map(|row| {
                let mut results = Vec::new();
                let mut acc = 0u64;

                for (i, &bit) in row.iter().enumerate() {
                    acc = (acc << 1) | bit as u64;
                    if (i + 1) % 64 == 0 {
                        results.push(acc);
                        acc = 0;
                    }
                }
                if row.len() % 64 != 0 {
                    results.push(acc);
                }
                results
            })
            .collect();

        binary_encode
            .into_iter()
            .map(|inner_vec| inner_vec.into_iter().map(|x| T::try_from(x).unwrap()).collect())
            .collect()
    }

    // add the input data with their projection
    pub fn compute_lsh(&self, input_embedding: &Option<ListArray>) -> Result<ListArray> {
        match input_embedding {
            Some(data) => {
                let random_plans = self.generate_random_array();
                let data_projection = self.project(data, &random_plans)?;
                let mut projection = data_projection;
                projection.mapv_inplace(|x| if x >= 0.0 { 1.0 } else { 0.0 });
                let convert: Vec<Vec<u64>> = Self::convert_array_to_u64_vec(&projection);
                Ok(Self::convert_vec_to_byte_u64(convert))
            }
            None => Err(DataFusionError::Internal("the input data is None".to_string())),
        }
    }
}
