-- SPDX-FileCopyrightText: LakeSoul Contributors
-- 
-- SPDX-License-Identifier: Apache-2.0
DROP TABLE IF EXISTS lineitem;
CREATE
EXTERNAL TABLE  lineitem (
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
LOCATION 'file:///tmp/lakesoul/tpch_data/lineitem';
INSERT INTO lineitem
SELECT *
FROM tpch_lineitem(0.1,5);
SELECT * FROM lineitem limit 10;
-- EXPLAIN INSERT INTO lineitem
-- SELECT *
-- FROM tpch_lineitem(0.1);
-- EXPLAIN select * from tpch_lineitem(0.1);
