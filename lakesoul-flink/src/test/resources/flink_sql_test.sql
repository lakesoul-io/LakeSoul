SET 'parallelism.default' = '1';

CREATE CATALOG lakesoul_catalog WITH ('type'='lakesoul');

USE CATALOG lakesoul_catalog;

CREATE TABLE IF NOT EXISTS test_cdc.mysql_test_1_2 (
    id int,
    name string,
    dt int,
    PRIMARY KEY (id) NOT ENFORCED )
PARTITIONED BY (dt)
WITH ('connector'='lakesoul',
'format'='parquet',
'path'='/tmp/lakesoul/test_cdc/mysql_test_1_2',
'useCDC'='true',
'hashBucketNum'='2');

USE CATALOG default_catalog;

CREATE TABLE mysql_test_1(id INTEGER PRIMARY KEY NOT ENFORCED, name string, dt int)
    WITH (
        'connector'='mysql-cdc',
        'hostname'='127.0.0.1',
        'port'='3306',
        'server-id'='1',
        'username'='root',
        'password'='root',
        'database-name'='test_cdc',
        'table-name'='mysql_test_1');

INSERT INTO lakesoul_catalog.test_cdc.mysql_test_1_2 SELECT * FROM mysql_test_1;
