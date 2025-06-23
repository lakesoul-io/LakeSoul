use crate::tpch::schemas::{schema_to_def, tpch_schema};

pub fn tpch_gen_sql(
    table_name: &str,
    tpch_name: &str,
    sf: f64,
    num_parts: i32,
    path: &str,
) -> Vec<String> {
    let drop = format!("DROP TABLE IF EXISTS {};", table_name);
    let col_def = schema_to_def(&tpch_schema(tpch_name));
    let create = format!(
        "CREATE EXTERNAL TABLE {table_name} ({col_def})
STORED AS LAKESOUL
LOCATION '{path}';"
    );
    let insert = format!(
        "INSERT INTO {table_name} SELECT * FROM tpch_{tpch_name}({sf:?},{num_parts});"
    );
    vec![drop, create, insert]
}

#[cfg(test)]
mod tests {
    use crate::tpch::sql::tpch_gen_sql;

    #[test]
    fn sql_test() {
        let left = vec![
            "DROP TABLE IF EXISTS sf10.LineITEM;".to_string(),
            "CREATE EXTERNAL TABLE sf10.LineITEM (
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
	l_comment STRING NOT NULL
)
STORED AS LAKESOUL
LOCATION 'file:///tmp/lakesoul/tpch_data/lineitem';"
                .to_string(),
            "INSERT INTO sf10.LineITEM SELECT * FROM tpch_lineitem(10.0,8);".to_string(),
        ];
        let right = tpch_gen_sql(
            "sf10.LineITEM",
            "lineitem",
            10.0,
            8,
            "file:///tmp/lakesoul/tpch_data/lineitem",
        );
        assert_eq!(left, right);
    }
}
