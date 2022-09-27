SET 'parallelism.default' = '1';

CREATE CATALOG lakesoul_catalog WITH ('type'='lakesoul');

use catalog lakesoul_catalog;

CREATE TABLE IF NOT EXISTS test_cdc.mysql_test_1_2 ( id int,
                                       name string,
                                       dt int,
                                       primary key (id) NOT ENFORCED )
    PARTITIONED BY (dt)
    with ('connector'='lakesoul',
    'format'='parquet',
    'path'='/tmp/lakesoul/test_cdc/mysql_test_1_2',
    'useCDC'='true',
    'hashBucketNum'='2');

use catalog default_catalog;

create table mysql_test_1(id INTEGER primary key NOT ENFORCED, name string, dt int)
    with (
        'connector'='mysql-cdc',
        'hostname'='127.0.0.1',
        'port'='3306',
        'server-id'='1',
        'username'='root',
        'password'='root',
        'database-name'='test_cdc',
        'table-name'='mysql_test_1');

insert into lakesoul_catalog.test_cdc.mysql_test_1_2 select * from mysql_test_1;
