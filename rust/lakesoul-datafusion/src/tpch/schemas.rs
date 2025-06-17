use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

/// Schema for the LineItem
pub static LINEITEM_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_lineitem_schema);
fn make_lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_tax", DataType::Decimal128(15, 2), false),
        Field::new("l_returnflag", DataType::Utf8View, false),
        Field::new("l_linestatus", DataType::Utf8View, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8View, false),
        Field::new("l_shipmode", DataType::Utf8View, false),
        Field::new("l_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the Nation
pub static NATION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_nation_schema);
fn make_nation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8View, false),
        Field::new("n_regionkey", DataType::Int64, false),
        Field::new("n_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the Order
pub static ORDER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_order_schema);
fn make_order_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderstatus", DataType::Utf8View, false),
        Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8View, false),
        Field::new("o_clerk", DataType::Utf8View, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the Part
pub static PART_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_part_schema);
fn make_part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_name", DataType::Utf8View, false),
        Field::new("p_mfgr", DataType::Utf8View, false),
        Field::new("p_brand", DataType::Utf8View, false),
        Field::new("p_type", DataType::Utf8View, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8View, false),
        Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
        Field::new("p_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the PartSupp
pub static PARTSUPP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_partsupp_schema);
fn make_partsupp_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int32, false),
        Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
        Field::new("ps_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the Region
pub static REGION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_region_schema);
fn make_region_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8View, false),
        Field::new("r_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the PartSupp
pub static SUPPLIER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_supplier_schema);
fn make_supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_name", DataType::Utf8View, false),
        Field::new("s_address", DataType::Utf8View, false),
        Field::new("s_nationkey", DataType::Int64, false),
        Field::new("s_phone", DataType::Utf8View, false),
        Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("s_comment", DataType::Utf8View, false),
    ]))
}

/// Schema for the Customer
pub static CUSTOMER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_customer_schema);
fn make_customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8View, false),
        Field::new("c_address", DataType::Utf8View, false),
        Field::new("c_nationkey", DataType::Int64, false),
        Field::new("c_phone", DataType::Utf8View, false),
        Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
        Field::new("c_mktsegment", DataType::Utf8View, false),
        Field::new("c_comment", DataType::Utf8View, false),
    ]))
}

pub fn tpch_schema(name: &str) -> SchemaRef {
    match name {
        "nation" => NATION_SCHEMA.clone(),
        "lineitem" => LINEITEM_SCHEMA.clone(),
        "part" => PART_SCHEMA.clone(),
        "supplier" => SUPPLIER_SCHEMA.clone(),
        "customer" => CUSTOMER_SCHEMA.clone(),
        "order" => ORDER_SCHEMA.clone(),
        "region" => REGION_SCHEMA.clone(),
        "partsupp" => PARTSUPP_SCHEMA.clone(),
        _ => unimplemented!(),
    }
}

pub fn schema_to_def(schema: &Schema) -> String {
    let intermediate = schema
        .fields
        .iter()
        .map(|f| field_to_def(f))
        .collect::<Vec<_>>()
        .join(",\n\t");
    if intermediate.is_empty() {
        intermediate
    } else {
        format!("\n\t{intermediate}\n")
    }
}

fn field_to_def(field: &Field) -> String {
    format!(
        "{} {} {}",
        field.name(),
        type_to_def(field.data_type()),
        if field.is_nullable() { "" } else { "NOT NULL" }
    )
}

fn type_to_def(ty: &DataType) -> String {
    match ty {
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Utf8View => "STRING".to_string(),
        DataType::Decimal128(s, p) => format!("DECIMAL({},{})", s, p),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use crate::tpch::schemas::schema_to_def;

    #[test]
    fn test_schema_to_sql() {
        let schema = super::LINEITEM_SCHEMA.as_ref();
        let left = "\
l_orderkey BIGINT NOT NULL,
l_partkey BIGINT NOT NULL,
l_suppkey BIGINT NOT NULL,
l_linenumber INT NOT NULL,
l_quantity DECIMAL(15,2) NOT NULL,
l_extendedprice DECIMAL(15,2) NOT NULL,
l_discount DECIMAL(15,2) NOT NULL,
l_tax DECIMAL(15,2) NOT NULL,
l_returnflag STRING NOT NULL,
l_linestatus STRING NOT NULL,
l_shipdate DATE NOT NULL,
l_commitdate DATE NOT NULL,
l_receiptdate DATE NOT NULL,
l_shipinstruct STRING NOT NULL,
l_shipmode STRING NOT NULL,
l_comment STRING NOT NULL";
        let right = schema_to_def(schema);
        assert_eq!(left, right.as_str());
    }
}
