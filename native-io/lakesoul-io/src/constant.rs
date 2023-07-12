/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;


use arrow::array::ArrayRef;
use arrow_array::{new_null_array, new_empty_array};
use arrow_schema::DataType;
use arrow::compute::CastOptions;

pub const LAKESOUL_TIMEZONE : &str = "UTC";
pub const LAKESOUL_NULL_STRING : &str = "__L@KE$OUL_NULL__";
pub const LAKESOUL_EMPTY_STRING : &str = "__L@KE$OUL_EMPTY_STRING__";
pub const ARROW_CAST_OPTIONS: CastOptions = CastOptions{safe: true};


#[derive(Debug, Default)]
pub struct ConstNullArray{
    inner: HashMap<DataType, ArrayRef>
}

impl ConstNullArray{
    pub fn new() -> Self {
        ConstNullArray{ inner: HashMap::new()}
    }

    pub fn get(&mut self, datatype: &DataType) -> ArrayRef {
        match self.inner.get(datatype) {
            Some(array) => array.clone(),
            None=> {
                let array = new_null_array(datatype, 1);
                self.inner.insert(datatype.clone(), array.clone());
                array.clone()
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct ConstEmptyArray{
    inner: HashMap<DataType, ArrayRef>
}

impl ConstEmptyArray{
    pub fn new() -> Self {
        ConstEmptyArray{ inner: HashMap::new()}
    }

    pub fn get(&mut self, datatype: &DataType) -> ArrayRef {
        match self.inner.get(datatype) {
            Some(array) => array.clone(),
            None=> {
                let array = new_empty_array(datatype);
                self.inner.insert(datatype.clone(), array.clone());
                array.clone()
            }
        }
    }
}

