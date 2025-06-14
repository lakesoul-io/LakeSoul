// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::physical_plan::{
    memory::{LazyBatchGenerator, LazyMemoryStream}, stream::RecordBatchStreamAdapter,
};
use tpchgen_arrow::LineItemArrow;

trait Source {}

impl Source for LineItemArrow {}

fn all_for_one<G>(schema: Arc<Schema>, sources: impl Iterator<Item = LineItemArrow>) {
    let x = RecordBatchStreamAdapter::new(schema, futures::stream::iter(sources));
    // sources.into_iter().map(|line| {
    //     line.into_iter()
    // })
}

//  struct TcphStream
//


