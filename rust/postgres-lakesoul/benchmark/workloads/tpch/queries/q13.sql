-- TPC-H Q13, specification qualification (qgen -d) parameters.
-- Source: DuckDB v1.5.6 tpch_queries(), derived from TPC-H specification section 2.4.
-- Deviation: none apart from SQL dialect formatting and LIMIT for specified row limits.
SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey)
    FROM
        customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders (c_custkey,
        c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
