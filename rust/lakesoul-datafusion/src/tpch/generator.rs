use arrow::array::RecordBatch;
use tpchgen_arrow::LineItemArrow;

pub trait TpchGenerator: Iterator<Item = RecordBatch> + 'static {}

impl TpchGenerator for LineItemArrow {}
