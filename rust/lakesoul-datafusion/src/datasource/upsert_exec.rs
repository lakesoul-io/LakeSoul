// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The sequential per-key state machine of `INSERT ... ON CONFLICT DO UPDATE`.
//!
//! A relational plan cannot express PostgreSQL's rule that a later input row
//! is rejected with `21000` *before* its own conflict `WHERE` is evaluated,
//! while an earlier skipped row stays unmodified. This node consumes the
//! source-ordered, joined rows one at a time and keeps exactly one piece of
//! per-key state: whether this command has already modified the key. It does
//! not cache old payloads or whole key groups.
//!
//! Input schema: the proposed row (`excluded.<col>`), the live target row
//! (`<target>.<col>`), an explicit `__lakesoul_matched` marker and the
//! `__lakesoul_order` token. The node is logical [`UpsertNode`] and physical
//! [`UpsertExec`]; the latter is encoded for a worker by
//! [`crate::distributed::codec`].

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr,
    PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::StreamExt;

use crate::write_mode::{EncodedViolation, WriteViolation};

/// How the node treats the matched target rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UpsertMode {
    /// General `DO UPDATE`: match, filter on the conflict `WHERE`, merge `SET`.
    Update,
    /// The legacy full-row `EXCLUDED` form on a CDC/custom-merge table: the
    /// output is the proposed row and a repeated source key is `21000`.
    LegacyInsertOnly,
}

/// Number of output rows buffered before a batch is emitted.
const OUTPUT_BATCH_ROWS: usize = 1024;

/// Logical form of the per-key sequential upsert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertNode {
    pub input: LogicalPlan,
    pub output_schema: DFSchemaRef,
    pub key_columns: Vec<usize>,
    pub order_column: usize,
    pub old_columns: Vec<usize>,
    pub new_columns: Vec<usize>,
    pub matched_column: Option<usize>,
    pub not_null_columns: Vec<(usize, String)>,
    pub where_expr: Expr,
    pub assignments: Vec<Option<Expr>>,
    pub table_name: String,
    pub mode: UpsertMode,
}

impl UpsertNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: LogicalPlan,
        output_schema: DFSchemaRef,
        key_columns: Vec<usize>,
        order_column: usize,
        old_columns: Vec<usize>,
        new_columns: Vec<usize>,
        matched_column: Option<usize>,
        not_null_columns: Vec<(usize, String)>,
        where_expr: Expr,
        assignments: Vec<Option<Expr>>,
        table_name: String,
        mode: UpsertMode,
    ) -> Self {
        Self {
            input,
            output_schema,
            key_columns,
            old_columns,
            order_column,
            new_columns,
            matched_column,
            not_null_columns,
            where_expr,
            assignments,
            table_name,
            mode,
        }
    }
}

impl std::hash::Hash for UpsertNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.mode.hash(state);
        self.key_columns.hash(state);
        self.order_column.hash(state);
        self.old_columns.hash(state);
        self.new_columns.hash(state);
        self.matched_column.hash(state);
        self.not_null_columns.hash(state);
        self.where_expr.hash(state);
        self.assignments.hash(state);
    }
}

impl PartialOrd for UpsertNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Schema and input plans need equality, not an allocating Debug-string
        // total order. Unequal extensions are intentionally incomparable.
        (self == other).then_some(std::cmp::Ordering::Equal)
    }
}

impl UserDefinedLogicalNodeCore for UpsertNode {
    fn name(&self) -> &str {
        "LakeSoulUpsert"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut expressions = Vec::with_capacity(1 + self.assignments.len());
        expressions.push(self.where_expr.clone());
        for assignment in &self.assignments {
            expressions.push(assignment.clone().unwrap_or_else(null_literal));
        }
        expressions
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulUpsert: mode={:?}", self.mode)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        let [input] = inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "LakeSoulUpsert expects exactly one input".to_string(),
            ));
        };
        if exprs.len() != 1 + self.assignments.len() {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulUpsert expected {} expressions, got {}",
                1 + self.assignments.len(),
                exprs.len()
            )));
        }
        let mut node = self.clone();
        node.input = input.clone();
        node.where_expr = exprs[0].clone();
        for (slot, expression) in node.assignments.iter_mut().zip(exprs.iter().skip(1)) {
            if slot.is_some() {
                *slot = Some(expression.clone());
            }
        }
        Ok(node)
    }

    fn necessary_children_exprs(
        &self,
        _output_columns: &[usize],
    ) -> Option<Vec<Vec<usize>>> {
        // Every input column is needed: the proposed row is the insert branch
        // of every output row, and the operator's expressions are opaque to
        // the optimizer. Returning `None` stops projection pruning from
        // dropping a nullable CAST or a volatile proposed-row expression.
        None
    }
}

fn null_literal() -> Expr {
    Expr::Literal(ScalarValue::Null, None)
}

/// Physical execution of [`UpsertNode`].
pub struct UpsertExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    key_columns: Vec<usize>,
    order_column: usize,
    input_ordering: OrderingRequirements,
    old_columns: Vec<usize>,
    new_columns: Vec<usize>,
    matched_column: Option<usize>,
    not_null_columns: Vec<(usize, String)>,
    where_expr: Arc<dyn PhysicalExpr>,
    assignments: Vec<Option<Arc<dyn PhysicalExpr>>>,
    table_name: String,
    mode: UpsertMode,
    properties: Arc<PlanProperties>,
}

impl UpsertExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        key_columns: Vec<usize>,
        order_column: usize,
        old_columns: Vec<usize>,
        new_columns: Vec<usize>,
        matched_column: Option<usize>,
        not_null_columns: Vec<(usize, String)>,
        where_expr: Arc<dyn PhysicalExpr>,
        assignments: Vec<Option<Arc<dyn PhysicalExpr>>>,
        table_name: String,
        mode: UpsertMode,
    ) -> DFResult<Self> {
        let input_fields = input.schema().fields().len();
        for index in key_columns
            .iter()
            .chain(old_columns.iter())
            .chain(new_columns.iter())
            .chain(matched_column.iter())
            .chain(std::iter::once(&order_column))
            .chain(not_null_columns.iter().map(|(index, _)| index))
        {
            if *index >= input_fields {
                return Err(DataFusionError::Plan(format!(
                    "LakeSoulUpsert column index {index} is outside its input schema"
                )));
            }
        }
        let width = output_schema.fields().len();
        if key_columns.is_empty()
            || new_columns.len() != width
            || assignments.len() != width
            || (mode == UpsertMode::Update
                && (old_columns.len() != width || matched_column.is_none()))
        {
            return Err(DataFusionError::Plan(
                "LakeSoulUpsert requires keys, complete row mappings and update expressions"
                    .to_string(),
            ));
        }
        if let Some(index) = matched_column
            && input.schema().field(index).data_type()
                != &arrow::datatypes::DataType::Boolean
        {
            return Err(DataFusionError::Plan(
                "LakeSoulUpsert match marker must be Boolean".to_string(),
            ));
        }
        if !matches!(
            where_expr.data_type(input.schema().as_ref())?,
            arrow::datatypes::DataType::Boolean | arrow::datatypes::DataType::Null
        ) {
            return Err(DataFusionError::Plan(
                "LakeSoulUpsert conflict predicate must be Boolean".into(),
            ));
        }
        if !matches!(
            input.schema().field(order_column).data_type(),
            arrow::datatypes::DataType::Int64 | arrow::datatypes::DataType::UInt64
        ) {
            return Err(DataFusionError::Plan(
                "LakeSoulUpsert source order must be a 64-bit integer".into(),
            ));
        }
        let ordering = LexOrdering::new(
            key_columns
                .iter()
                .chain(std::iter::once(&order_column))
                .map(|index| PhysicalSortExpr {
                    expr: Arc::new(Column::new(
                        input.schema().field(*index).name(),
                        *index,
                    )),
                    options: arrow::compute::SortOptions::default(),
                })
                .collect::<Vec<_>>(),
        )
        .ok_or_else(|| {
            DataFusionError::Plan("LakeSoulUpsert requires an ordering".into())
        })?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            output_schema,
            key_columns,
            order_column,
            input_ordering: ordering.into(),
            old_columns,
            new_columns,
            matched_column,
            not_null_columns,
            where_expr,
            assignments,
            table_name,
            mode,
            properties,
        })
    }

    pub fn mode(&self) -> UpsertMode {
        self.mode
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn key_columns(&self) -> &[usize] {
        &self.key_columns
    }

    pub fn order_column(&self) -> usize {
        self.order_column
    }

    pub fn old_columns(&self) -> &[usize] {
        &self.old_columns
    }

    pub fn new_columns(&self) -> &[usize] {
        &self.new_columns
    }

    pub fn matched_column(&self) -> Option<usize> {
        self.matched_column
    }

    pub fn not_null_columns(&self) -> &[(usize, String)] {
        &self.not_null_columns
    }

    pub fn where_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.where_expr
    }

    pub fn assignments(&self) -> &[Option<Arc<dyn PhysicalExpr>>] {
        &self.assignments
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl fmt::Debug for UpsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpsertExec")
            .field("mode", &self.mode)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl DisplayAs for UpsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulUpsertExec: mode={:?}", self.mode)
    }
}

impl ExecutionPlanProperties for UpsertExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.properties.eq_properties
    }
}

impl ExecutionPlan for UpsertExec {
    fn name(&self) -> &str {
        "LakeSoulUpsertExec"
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        )
            -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        datafusion::physical_plan::execution_plan::apply_expression_roots(
            std::iter::once(&self.where_expr).chain(self.assignments.iter().flatten()),
            f,
        )
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // All rows of one key must be processed in one place, in source order.
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(self.input_ordering.clone())]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(DataFusionError::Internal(
                "LakeSoulUpsertExec expects exactly one child".to_string(),
            ));
        };
        Ok(Arc::new(Self::try_new(
            Arc::clone(input),
            self.output_schema.clone(),
            self.key_columns.clone(),
            self.order_column,
            self.old_columns.clone(),
            self.new_columns.clone(),
            self.matched_column,
            self.not_null_columns.clone(),
            Arc::clone(&self.where_expr),
            self.assignments.clone(),
            self.table_name.clone(),
            self.mode,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::NotImplemented(
                "LakeSoulUpsertExec is single-partition".to_string(),
            ));
        }
        // This planner replaces the default physical planner, so declared
        // distribution requirements are not enforced automatically. Refuse a
        // multi-partition input instead of silently reading partition 0 and
        // upserting only part of the statement.
        if self.input.output_partitioning().partition_count() != 1 {
            return Err(DataFusionError::Plan(format!(
                "LakeSoulUpsertExec requires a single input partition, got {}",
                self.input.output_partitioning().partition_count()
            )));
        }
        let input = self.input.execute(0, Arc::clone(&context))?;
        let machine = Machine::new(&context, self)?;
        let output_schema = self.output_schema.clone();
        let stream = futures::stream::try_unfold(
            (input, machine),
            |(mut input, mut machine)| async move {
                loop {
                    if machine.current.is_some()
                        && let Some(batch) = machine.next_batch()?
                    {
                        return Ok(Some((batch, (input, machine))));
                    }
                    match input.next().await {
                        Some(Ok(batch)) => machine.set_batch(batch)?,
                        Some(Err(error)) => return Err(error),
                        None => return Ok(None),
                    }
                }
            },
        );
        let stream = RecordBatchStreamAdapter::new(output_schema, stream);
        Ok(Box::pin(stream))
    }
}

/// State of one execution of the node.
struct Machine {
    current: Option<(RecordBatch, Rows, usize)>,
    output_schema: SchemaRef,
    converter: RowConverter,
    key_columns: Vec<usize>,
    old_columns: Vec<usize>,
    new_columns: Vec<usize>,
    matched_column: Option<usize>,
    not_null_columns: Vec<(usize, String)>,
    where_expr: Arc<dyn PhysicalExpr>,
    assignments: Vec<Option<Arc<dyn PhysicalExpr>>>,
    table_name: String,
    mode: UpsertMode,
    /// The key of the group currently being processed. The input is sorted by
    /// `(keys, source_order)`, so only one key can be live at a time.
    last_key: Option<Vec<u8>>,
    /// Whether this command already inserted or updated `last_key`.
    modified: bool,
    output: Vec<Vec<ScalarValue>>,
    rows: usize,
    batch_reservation: MemoryReservation,
    state_reservation: MemoryReservation,
    output_reservation: MemoryReservation,
}

impl Machine {
    fn new(context: &TaskContext, exec: &UpsertExec) -> DFResult<Self> {
        let input_schema = exec.input.schema();
        let key_fields = exec
            .key_columns
            .iter()
            .map(|index| input_schema.field(*index))
            .collect::<Vec<_>>();
        let converter = RowConverter::new(
            key_fields
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        let state_reservation = MemoryConsumer::new("LakeSoulUpsertState")
            .register(&context.runtime_env().memory_pool);
        let batch_reservation = MemoryConsumer::new("LakeSoulUpsertInput")
            .register(&context.runtime_env().memory_pool);
        let output_reservation = MemoryConsumer::new("LakeSoulUpsertOutput")
            .register(&context.runtime_env().memory_pool);
        Ok(Self {
            current: None,
            output_schema: exec.output_schema.clone(),
            converter,
            key_columns: exec.key_columns.clone(),
            old_columns: exec.old_columns.clone(),
            new_columns: exec.new_columns.clone(),
            matched_column: exec.matched_column,
            not_null_columns: exec.not_null_columns.clone(),
            where_expr: Arc::clone(&exec.where_expr),
            assignments: exec.assignments.clone(),
            table_name: exec.table_name.clone(),
            mode: exec.mode,
            last_key: None,
            modified: false,
            output: vec![Vec::new(); exec.output_schema.fields().len()],
            rows: 0,
            batch_reservation,
            state_reservation,
            output_reservation,
        })
    }

    fn set_batch(&mut self, batch: RecordBatch) -> DFResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let key_arrays = self
            .key_columns
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect::<Vec<_>>();
        // Reserve before row encoding. The row representation expands packed
        // values and escapes variable-width keys; include offsets and fields.
        let key_estimate = key_arrays.iter().fold(0usize, |size, array| {
            let bytes = if let Some(width) = array.data_type().primitive_width() {
                array.len().saturating_mul(width.saturating_add(1))
            } else {
                array.get_array_memory_size().saturating_mul(16)
            };
            size.saturating_add(bytes)
        });
        let estimate = batch
            .get_array_memory_size()
            .saturating_add(key_estimate)
            .saturating_add(batch.num_rows().saturating_mul(
                self.key_columns.len().saturating_mul(16).saturating_add(16),
            ));
        self.batch_reservation.try_resize(estimate)?;
        let keys = self.converter.convert_columns(&key_arrays)?;
        self.batch_reservation
            .try_resize(batch.get_array_memory_size().saturating_add(keys.size()))?;
        self.state_reservation.try_resize(
            self.converter.size() + self.last_key.as_ref().map_or(0, Vec::capacity),
        )?;
        self.current = Some((batch, keys, 0));
        Ok(())
    }

    fn next_batch(&mut self) -> DFResult<Option<RecordBatch>> {
        let Some((batch, keys, mut row)) = self.current.take() else {
            return Ok(None);
        };
        while row < batch.num_rows() && self.rows < OUTPUT_BATCH_ROWS {
            for (index, column) in &self.not_null_columns {
                if batch.column(*index).is_null(row) {
                    return Err(DataFusionError::External(Box::new(
                        EncodedViolation::new(WriteViolation::NotNull {
                            column: column.clone(),
                        }),
                    )));
                }
            }
            let key = keys.row(row);
            if self.last_key.as_deref() != Some(key.as_ref()) {
                let stored = self.last_key.get_or_insert_with(Vec::new);
                self.state_reservation.try_resize(
                    self.converter
                        .size()
                        .saturating_add(stored.capacity().max(key.as_ref().len())),
                )?;
                stored.reserve_exact(key.as_ref().len().saturating_sub(stored.len()));
                stored.clear();
                stored.extend_from_slice(key.as_ref());
                self.modified = false;
            } else if self.modified {
                return Err(DataFusionError::External(Box::new(EncodedViolation::new(
                    WriteViolation::DuplicateUpsertRow {
                        table: self.table_name.clone(),
                    },
                ))));
            }
            let matched = self.mode == UpsertMode::Update && self.matched(&batch, row)?;
            let one = matched.then(|| batch.slice(row, 1));
            let produce = match &one {
                Some(one) => matches!(
                    evaluate_scalar(self.where_expr.as_ref(), one)?,
                    ScalarValue::Boolean(Some(true))
                ),
                None => true,
            };
            if produce {
                for index in 0..self.output.len() {
                    let value = if let Some(one) = &one {
                        match &self.assignments[index] {
                            Some(expression) => {
                                evaluate_scalar(expression.as_ref(), one)?
                            }
                            None => ScalarValue::try_from_array(
                                batch.column(self.old_columns[index]),
                                row,
                            )?,
                        }
                    } else {
                        ScalarValue::try_from_array(
                            batch.column(self.new_columns[index]),
                            row,
                        )?
                    };
                    // A scan may expose a view array while the proposed row is
                    // a plain string; one batch can mix both encodings, so
                    // normalise each value to the declared output type.
                    let value =
                        value.cast_to(self.output_schema.field(index).data_type())?;
                    if !self.output_schema.field(index).is_nullable() && value.is_null() {
                        return Err(DataFusionError::External(Box::new(
                            EncodedViolation::new(WriteViolation::NotNull {
                                column: self.output_schema.field(index).name().clone(),
                            }),
                        )));
                    }
                    let values = &mut self.output[index];
                    if values.len() == values.capacity() {
                        let capacity = values
                            .capacity()
                            .max(4)
                            .saturating_mul(2)
                            .min(OUTPUT_BATCH_ROWS);
                        self.output_reservation.try_grow(
                            (capacity - values.capacity())
                                * std::mem::size_of::<ScalarValue>(),
                        )?;
                        values.reserve_exact(capacity - values.capacity());
                    }
                    // Include heap payloads and the array built from these scalars.
                    self.output_reservation
                        .try_grow(value.size().saturating_mul(2))?;
                    values.push(value);
                }
                self.modified = true;
                self.rows += 1;
            }
            row += 1;
        }
        if row < batch.num_rows() {
            self.current = Some((batch, keys, row));
        } else {
            drop(keys);
            drop(batch);
            self.batch_reservation.free();
        }
        if self.rows == 0 {
            Ok(None)
        } else {
            self.flush().map(Some)
        }
    }

    fn matched(&self, batch: &RecordBatch, row: usize) -> DFResult<bool> {
        let Some(index) = self.matched_column else {
            return Ok(false);
        };
        let value = ScalarValue::try_from_array(batch.column(index), row)?;
        Ok(matches!(value, ScalarValue::Boolean(Some(true))))
    }

    fn flush(&mut self) -> DFResult<RecordBatch> {
        let arrays = self
            .output
            .iter_mut()
            .zip(self.output_schema.fields())
            .map(|(values, field)| {
                let values = std::mem::take(values);
                let array = ScalarValue::iter_to_array(values)?;
                if array.data_type() == field.data_type() {
                    Ok(array)
                } else {
                    // Scans may expose a view array for the table's string type.
                    arrow::compute::cast(&array, field.data_type()).map_err(Into::into)
                }
            })
            .collect::<DFResult<Vec<_>>>()?;
        self.rows = 0;
        self.output_reservation.free();
        RecordBatch::try_new(self.output_schema.clone(), arrays)
            .map_err(DataFusionError::from)
    }
}

fn evaluate_scalar(
    expression: &dyn PhysicalExpr,
    batch: &RecordBatch,
) -> DFResult<ScalarValue> {
    match expression.evaluate(batch)? {
        datafusion::logical_expr::ColumnarValue::Scalar(value) => Ok(value),
        datafusion::logical_expr::ColumnarValue::Array(array) => {
            if array.len() != 1 {
                return Err(DataFusionError::Internal(
                    "LakeSoulUpsert scalar expression must return exactly one row".into(),
                ));
            }
            ScalarValue::try_from_array(&array, 0)
        }
    }
}

/// Build the output `DFSchema` of the node from the table schema.
pub fn output_dfschema(
    schema: &Schema,
    table: datafusion::common::TableReference,
) -> DFResult<DFSchemaRef> {
    Ok(Arc::new(DFSchema::try_from_qualified_schema(
        table, schema,
    )?))
}
