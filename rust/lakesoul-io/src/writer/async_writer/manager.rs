use std::collections::HashMap;

use crate::writer::async_writer::MultiPartAsyncWriter;

struct SharedMemoryWriterManager {
    writers: HashMap<usize, MultiPartAsyncWriter>,
    global_mem_limit: usize,
}
