-- TPC-H Q14, specification qualification (qgen -d) parameters.
-- Source: DuckDB v1.5.6 tpch_queries(), derived from TPC-H specification section 2.4.
-- Deviation: qualification date/interval arithmetic is constant-folded.
SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
    AND l_shipdate < CAST('1995-10-01' AS date)
