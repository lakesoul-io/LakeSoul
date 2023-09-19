# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from lakesoul.spark import LakeSoulTable

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[4]") \
        .config("spark.driver.memoryOverhead", "1500m") \
        .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
        .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
        .config("spark.sql.defaultCatalog", "lakesoul") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    dataset_table = "imdb"
    tablePath = "s3://lakesoul-test-bucket/imdb"
    trainFilePath = "/opt/spark/work-dir/imdb/dataset/imdb-train.parquet"    
    testFilePath = "/opt/spark/work-dir/imdb/dataset/imdb-test.parquet"
    print("Debug -- Show tables before importing data")
    spark.sql("show tables").show()

    trainDf = spark.read.format("parquet").load(trainFilePath)
    testDf = spark.read.format("parquet").load(testFilePath)

    print("Debug -- Load data into dataframe")
    trainDf.show()

    spark.sql("drop table if exists imdb")
    trainDf.withColumn("split", lit("train"))\
        .write.mode("append").format("lakesoul")\
        .option("rangePartitions","split")\
        .option("shortTableName", dataset_table)\
        .save(tablePath)
    
    testDf.withColumn("split", lit("test"))\
        .write.mode("append").format("lakesoul")\
        .option("rangePartitions","split")\
        .option("shortTableName", dataset_table)\
        .save(tablePath)
    
    print("Debug -- Show tables after importing data")
    spark.sql("show tables").show()
    LakeSoulTable.forName(spark, dataset_table).toDF().where(col("split").eqNullSafe("train")).show(20)
    LakeSoulTable.forName(spark, dataset_table).toDF().where(col("split").eqNullSafe("test")).show(20)

    spark.stop()
