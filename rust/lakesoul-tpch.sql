CREATE SCHEMA IF NOT EXISTS tpch_sf10;

--lineitem
DROP TABLE IF EXISTS tpch_sf10.lineitem;
CREATE
EXTERNAL TABLE  tpch_sf10.lineitem (
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
    l_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/lineitem';
INSERT INTO tpch_sf10.lineitem
SELECT *
FROM tpch_lineitem(10.0,8);

--nation
DROP TABLE IF EXISTS tpch_sf10.nation;
CREATE
EXTERNAL TABLE tpch_sf10.nation (
    n_nationkey BIGINT NOT NULL,
    n_name STRING NOT NULL,
    n_regionkey BIGINT NOT NULL,
    n_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/nation';
INSERT INTO tpch_sf10.nation
SELECT *
FROM tpch_nation(10.0,8);

-- order
DROP TABLE IF EXISTS tpch_sf10.order;
CREATE
EXTERNAL TABLE  tpch_sf10.order (
    o_orderkey BIGINT NOT NULL,
    o_custkey BIGINT NOT NULL,
    o_orderstatus STRING NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority STRING NOT NULL,
    o_clerk STRING NOT NULL,
    o_shippriority INT NOT NULL,
    o_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/order';
INSERT INTO tpch_sf10.order
SELECT *
FROM tpch_order(10.0,8);

--PART
DROP TABLE IF EXISTS tpch_sf10.part;
CREATE
EXTERNAL TABLE  tpch_sf10.part (
    p_partkey BIGINT NOT NULL,
    p_name STRING NOT NULL,
    p_mfgr STRING NOT NULL,
    p_brand STRING NOT NULL,
    p_type STRING NOT NULL,
    p_size INT NOT NULL,
    p_container STRING NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/part';
INSERT INTO tpch_sf10.part
SELECT *
FROM tpch_part(10.0,8);

--partsupp
DROP TABLE IF EXISTS tpch_sf10.partsupp;
CREATE
EXTERNAL TABLE  tpch_sf10.partsupp (
    ps_partkey BIGINT NOT NULL,
    ps_suppkey BIGINT NOT NULL,
    ps_availqty INT NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/partsupp';
INSERT INTO tpch_sf10.partsupp
SELECT *
FROM tpch_partsupp(10.0,8);

--
DROP TABLE IF EXISTS tpch_sf10.region;
CREATE
EXTERNAL TABLE  tpch_sf10.region (
    r_regionkey BIGINT NOT NULL,
    r_name STRING NOT NULL,
    r_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/region';
INSERT INTO tpch_sf10.region
SELECT *
FROM tpch_region(10.0,8);


--supplier
DROP TABLE IF EXISTS tpch_sf10.supplier;
CREATE
EXTERNAL TABLE  tpch_sf10.supplier (
    s_suppkey BIGINT NOT NULL,
    s_name STRING NOT NULL,
    s_address STRING NOT NULL,
    s_nationkey BIGINT NOT NULL,
    s_phone STRING NOT NULL,
    s_acctbal DECIMAL(15,2) NOT NULL,
    s_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/supplier';
INSERT INTO tpch_sf10.supplier
SELECT *
FROM tpch_supplier(10.0,8);

--customer
DROP TABLE IF EXISTS tpch_sf10.customer;
CREATE
EXTERNAL TABLE  tpch_sf10.customer (
    c_custkey BIGINT NOT NULL,
    c_name STRING NOT NULL,
    c_address STRING NOT NULL,
    c_nationkey BIGINT NOT NULL,
    c_phone STRING NOT NULL,
    c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment STRING NOT NULL,
    c_comment STRING NOT NULL,
)
STORED AS LAKESOUL
LOCATION 'file:///data/lakesoul/tpch_sf10/customer';
INSERT INTO tpch_sf10.customer
SELECT *
FROM tpch_customer(10.0,8);
