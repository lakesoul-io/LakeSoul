use std::sync::Arc;

use datafusion_datasource::file_sink_config::FileSink;

pub struct FileSinkWriter {
    sink: Arc<dyn FileSink>,
}

impl FileSinkWriter {
    fn new() {
        // ParquetSink::new();
    }
}
