const DROP: &'static str = "DROP TABLE IF EXISTS {}";
const CREATE: &'static str = "EXTERNAL TABLE  tpch_sf10.lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL,
    l_returnflag STRING NOT NULL,
    l_ilinestatus STRING NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct STRING NOT NULL,
    l_shipmode STRING NOT NULL,
    l_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/lineitem';
INSERT INTO tpch_sf10.lineitem
SELECT *
FROM tpch_lineitem(10.0,8);";

macro_rules! tpch_gen {
    () => {
        vec!["?"];
    };
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_test() {
        use std::fmt::Write;

        let v = vec![1, 2, 3];
        let mut s = String::new();

        // println!(DROP,);
    }
}
