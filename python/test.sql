 DROP TABLE IF EXISTS test_lfs;
 CREATE EXTERNAL TABLE
            test_lfs
            (c1 VARCHAR NOT NULL, c2 INT NOT NULL, c3 DOUBLE , PRIMARY KEY(c1))
            STORED AS LAKESOUL
            PARTITIONED BY (c2)
            LOCATION 'file:///tmp/LAKESOUL/test_lfs_data';
INSERT INTO test_lfs VALUES ('test', 1, 1.0);
