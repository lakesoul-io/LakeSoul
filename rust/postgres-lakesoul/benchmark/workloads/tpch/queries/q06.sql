-- TPC-H Q06, specification qualification (qgen -d) parameters.
-- Source: DuckDB v1.5.6 tpch_queries(), derived from TPC-H specification section 2.4.
-- Deviation: qualification date/interval arithmetic is constant-folded.
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24
