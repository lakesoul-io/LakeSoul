 DROP TABLE IF EXISTS test_lfs;
 CREATE EXTERNAL TABLE
            test_lfs
            (c1 VARCHAR NOT NULL, c2 INT NOT NULL, c3 DOUBLE , PRIMARY KEY(c1))
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION 'file:///tmp/lakesoul/test_lfs_data';
INSERT INTO test_lfs(c1,c2,c3) VALUES
    ('test', 1, 1.0),
    ('hello', 1, 2.5),
    ('world', 2, 3.14),
    ('foo', 2, 0.0),
    ('bar', 3, -1.5),
    ('baz', 3, 100.0),
    ('qux', 3, 42.0);
