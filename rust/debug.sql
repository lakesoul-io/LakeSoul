CREATE EXTERNAL TABLE
IF NOT EXISTS
test_lakesoul_meta.prod_debug (id int, range date)
STORED AS lakesoul
PARTITIONED BY (range)
LOCATION  'file:///tmp/lakesoul/prod/prod_debug';

INSERT INTO test_lakesoul_meta.prod_debug VALUES (1, NULL);
