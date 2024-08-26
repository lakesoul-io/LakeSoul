// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::compute::CastOptions;
use arrow_array::{new_empty_array, new_null_array};
use arrow_schema::DataType;

use lazy_static::lazy_static;

pub static LAKESOUL_TIMEZONE: &str = "UTC";
pub static LAKESOUL_NULL_STRING: &str = "__L@KE$OUL_NULL__";
pub static LAKESOUL_EMPTY_STRING: &str = "__L@KE$OUL_EMPTY_STRING__";
pub static LAKESOUL_EQ: &str = "__L@KE$OUL_EQ__";
pub static LAKESOUL_COMMA: &str = "__L@KE$OUL_COMMA__";

pub static DEFAULT_PARTITION_DESC: &str = "-5";
pub static TBD_PARTITION_DESC: &str = "-4";

pub static DATE32_FORMAT: &str = "%Y-%m-%d";
pub static FLINK_TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.9f";
pub static TIMESTAMP_SECOND_FORMAT: &str = "%Y-%m-%dT%H:%M:%S";
pub static TIMESTAMP_MILLSECOND_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3f";
pub static TIMESTAMP_MICROSECOND_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f";
pub static TIMESTAMP_NANOSECOND_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.9f";

pub static NUM_COLUMN_OPTIMIZE_THRESHOLD: usize = 200;

lazy_static! {
    pub static ref ARROW_CAST_OPTIONS: CastOptions<'static> = CastOptions::default();
}

#[derive(Debug, Default)]
pub struct ConstNullArray {
    inner: HashMap<DataType, ArrayRef>,
}

impl ConstNullArray {
    pub fn new() -> Self {
        ConstNullArray { inner: HashMap::new() }
    }

    pub fn get(&mut self, datatype: &DataType) -> ArrayRef {
        match self.inner.get(datatype) {
            Some(array) => array.clone(),
            None => {
                let array = new_null_array(datatype, 1);
                self.inner.insert(datatype.clone(), array.clone());
                array.clone()
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ConstEmptyArray {
    inner: HashMap<DataType, ArrayRef>,
}

impl ConstEmptyArray {
    pub fn new() -> Self {
        ConstEmptyArray { inner: HashMap::new() }
    }

    pub fn get(&mut self, datatype: &DataType) -> ArrayRef {
        match self.inner.get(datatype) {
            Some(array) => array.clone(),
            None => {
                let array = new_empty_array(datatype);
                self.inner.insert(datatype.clone(), array.clone());
                array.clone()
            }
        }
    }
}
