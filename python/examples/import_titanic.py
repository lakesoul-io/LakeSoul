import os

from lakesoul.spark import LakeSoulTable

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit

    lakesoul_spark_jar = os.getenv("LAKESOUL_SPARK_JAR")
    assert lakesoul_spark_jar

    spark = (
        SparkSession.builder.master("local[4]")
        .config("spark.driver.memoryOverhead", "1500m")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dhttp.nonProxyHosts=localhost|127.0.0.1|rustfs "
            "-Dhttps.nonProxyHosts=localhost|127.0.0.1|rustfs",
        )
        .config(
            "spark.sql.extensions",
            "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.lakesoul",
            "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog",
        )
        .config("spark.sql.defaultCatalog", "lakesoul")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", "rustfsadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "rustfsadmin")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.180",
        )
        .config("spark.jars", lakesoul_spark_jar)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    dataset_table = "titanic_raw"
    trainFilePath = "/data/spark/work-dir/Titanic/train.csv"
    trainDf = spark.read.format("csv").option("header", "true").load(trainFilePath)
    trainDf = trainDf.withColumn("split", lit("train"))
    tablePath = "s3://lakesoul-test-bucket/titanic_raw"

    spark.sql("drop table if exists titanic_raw")
    trainDf.write.mode("append").format("lakesoul").option(
        "rangePartitions", "split"
    ).option("shortTableName", dataset_table).save(tablePath)
    spark.sql("show tables").show()
    LakeSoulTable.forName(spark, dataset_table).toDF().show()
    spark.stop()
