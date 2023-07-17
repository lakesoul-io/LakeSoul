// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

# Native IO Performance Comparison Results

## 1. Parquet Scan

### Settings

Code: lakesoul-spark/src/test/scala/org/apache/spark/sql/lakesoul/benchmark/io/ParquetScanBenchmark.scala

Tested on Spark 3.3.1 with Parquet-mr 1.12.2, Arrow-rs(parquet) 31.0.0.
Parquet file size: 894.3MB, compressed with snappy. Metadata:

```
############ file meta data ############
created_by: parquet-mr version 1.12.2 (build 77e30c8093386ec52c3cfa6c34b7ef3321322c94)
num_columns: 8
num_rows: 10000000
num_row_groups: 7
format_version: 1.0
serialized_size: 7688
```

File is read with only one parallelism in Spark.

### Results
1. MinIO

|          | Parquet-mr | Native-IO | Improvement |
|----------|------------|-----------|-------------|
| Time(ms) | 11417      | 4381      | 2.61x       |

2. AWS S3

|          | Parquet-mr | Native-IO | Improvement |
|----------|------------|-----------|-------------|
| Time(ms) | 25190      | 6965      | 3.62x       |

## 2. Parquet Write

### Settings

Code: lakesoul-spark/src/test/scala/org/apache/spark/sql/lakesoul/benchmark/io/ParquetScanBenchmark.scala

Tested on Spark 3.3.1 with Parquet-mr 1.12.2, Arrow-rs(parquet) 31.0.0.
Original parquet file size: 894.3MB, cached in memory in advance before write.

### Results

1. MinIO

|          | Parquet-mr | Native-IO | Improvement |
|----------|------------|-----------|-------------|
| Time(ms) | 22869      | 15576     | 1.47x       |

2. AWS S3

|          | Parquet-mr | Native-IO | Improvement |
|----------|------------|-----------|-------------|
| Time(ms) | 27218      | 18944     | 1.44x       |

## 3. Upsert Write

### Settings
Writing a base file of 20 million lines, and upsert 10 times with 5 million lines each.

Data is read from local file system and written to MinIO

Tested on Spark 3.3.1 with 4 local executor threads and 16G memory.

### Results

1. MinIO

|          | Parquet-mr | Native-IO | Improvement |
|----------|------------|-----------|-------------|
| Time(ms) | 72612      | 59607     | 1.47x       |
