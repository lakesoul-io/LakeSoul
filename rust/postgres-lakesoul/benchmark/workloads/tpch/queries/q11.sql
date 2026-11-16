-- TPC-H Q11, specification qualification (qgen -d) parameters.
-- Source: DuckDB v1.5.6 tpch_queries(), derived from TPC-H specification section 2.4.
-- Render {{SF}} as a float literal; qualification FRACTION is 0.0001 / SF.
SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM
    partsupp,
    supplier,
    nation
WHERE
    ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'GERMANY'
GROUP BY
    ps_partkey
HAVING
    sum(ps_supplycost * ps_availqty) > (
        SELECT
            sum(ps_supplycost * ps_availqty) * (0.0001 / {{SF}})
        FROM
            partsupp,
            supplier,
            nation
        WHERE
            ps_suppkey = s_suppkey
            AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY')
ORDER BY
    value DESC
