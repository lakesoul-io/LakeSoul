# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
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

    tablePath = "s3://lakesoul-test-bucket/titanic_raw"
    trainFilePath = "/opt/spark/work-dir/titanic/dataset/train.csv"
    print("Debug -- Show tables before importing data")
    spark.sql("show tables").show()

    trainDf = spark.read.format("csv").option("header", "true").load(trainFilePath)
    trainDf = trainDf.withColumn("split", lit("train"))
    print("Debug -- Load data into dataframe")
    trainDf.show()

    spark.sql("drop table if exists titanic_raw")
    trainDf.write.mode("append").format("lakesoul")\
        .option("rangePartitions", "split")\
        .option("shortTableName","titanic_raw")\
        .save(tablePath)
    print("Debug -- Show tables after importing data")
    spark.sql("show tables").show()
    LakeSoulTable.forName(spark, "titanic_raw").toDF().show()

    spark.stop()
