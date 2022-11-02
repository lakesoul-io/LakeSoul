use std::collections::HashMap;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;

use std::iter::Iterator;


struct KeyIndex{

}

struct FieldInfo{

}

pub struct MergePartitionedFile {
    partition_values: HashMap<String, String>,
    file_path: String,
    start: usize,
    length: usize,
    qualified_name: String,
    range_key: String,
    key_info: Vec<KeyIndex>, //(hash key index of file, dataType)
    result_schema: Vec<FieldInfo>, //all result columns name and type
    file_info: Vec<FieldInfo>, //file columns name and type
    write_version: usize,
    range_version: String,
    file_bucket_id: usize, //hash split id
}

impl MergePartitionedFile{

    pub fn get_path_as_str(&self) -> &str{
        self.file_path.as_str()
    }
}

pub struct RecordBatchIterWithFile{
    file: Box<MergePartitionedFile>,
    current_batch: Option<Box<RecordBatch>>,
    current_row_idx: Option<usize>,
}

impl Iterator for RecordBatchIterWithFile{
    type Item = Box<RecordBatch>;

    fn next(&mut self) -> Option<Box<RecordBatch>>{
        None
    }
}