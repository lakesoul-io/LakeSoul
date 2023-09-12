# LakeSoul Python Examples

## Prerequisites

### Deploy Docker compose env

```bash
cd docker/lakesoul-docker-compose-env
docker compose up -d
```

### Pull spark image

```bash
docker pull bitnami/spark:3.3.1
```

### Download LakeSoul release jar

```bash
wget https://github.com/lakesoul-io/LakeSoul/releases/download/v2.3.1/lakesoul-spark-2.3.1-spark-3.3.jar 
```

## Import Datasets Into LakeSoul

### Download datasets

- [titanic]((https://github.com/meta-soul/lakesoul-ml/tree/master/titanic/dataset/output))
- [imdb](https://huggingface.co/datasets/imdb/tree/refs%2Fconvert%2Fparquet/plain_text)

### Start spark-shell

```bash
sudo docker run --rm -ti --net lakesoul-docker-compose-env_default -v $PWD/lakesoul-spark-2.3.1-spark-3.3.jar:/opt/spark/work-dir/jars/lakesoul-spark-2.3.1-spark-3.3.jar \
    -v $PWD/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
    -v $PWD/Titanic:/opt/spark/work-dir/Titanic \
    -v $PWD/imdb:/opt/spark/work-dir/imdb \
    -v /home/wenbin/output:/opt/spark/work-dir/clip \
    --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties \
    bitnami/spark:3.3.1 spark-shell \
    --jars /opt/spark/work-dir/jars/lakesoul-spark-2.3.0-spark-3.3-SNAPSHOT.jar \
    --driver-memory 4G \
    --executor-memory 4G \
    --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m \
    --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
    --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog \
    --conf spark.sql.defaultCatalog=lakesoul \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin1 \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin1 

```

### Import Datasets by Spark

#### Titanic

```scala
import org.apache.spark.sql.types._


val trainFilePath = "/opt/spark/work-dir/Titanic/train_data.csv"
var trainDf = spark.read.format("csv").option("header", "true").load(trainFilePath)
trainDf = trainDf.withColumn("label", col("label").cast(FloatType)).withColumn("f1", col("f1").cast(FloatType)).withColumn("f2", col("f2").cast(FloatType)).withColumn("f3", col("f3").cast(FloatType)).withColumn("f4", col("f4").cast(FloatType)).withColumn("f5", col("f5").cast(FloatType)).withColumn("f6", col("f6").cast(FloatType)).withColumn("f7", col("f7").cast(FloatType)).withColumn("f8", col("f8").cast(FloatType)).withColumn("f9", col("f9").cast(FloatType)).withColumn("f10", col("f10").cast(FloatType)).withColumn("f11", col("f11").cast(FloatType)).withColumn("f12", col("f12").cast(FloatType)).withColumn("f13", col("f13").cast(FloatType)).withColumn("f14", col("f14").cast(FloatType)).withColumn("f15", col("f15").cast(FloatType)).withColumn("f16", col("f16").cast(FloatType)).withColumn("f17", col("f17").cast(FloatType)).withColumn("f18", col("f18").cast(FloatType)).withColumn("f19", col("f19").cast(FloatType)).withColumn("split", typedLit("train"))

val valFilePath = "/opt/spark/work-dir/Titanic/val_data.csv"
var valDf = spark.read.format("csv").option("header", "true").load(valFilePath)
valDf = valDf.withColumn("label", col("label").cast(FloatType)).withColumn("f1", col("f1").cast(FloatType)).withColumn("f2", col("f2").cast(FloatType)).withColumn("f3", col("f3").cast(FloatType)).withColumn("f4", col("f4").cast(FloatType)).withColumn("f5", col("f5").cast(FloatType)).withColumn("f6", col("f6").cast(FloatType)).withColumn("f7", col("f7").cast(FloatType)).withColumn("f8", col("f8").cast(FloatType)).withColumn("f9", col("f9").cast(FloatType)).withColumn("f10", col("f10").cast(FloatType)).withColumn("f11", col("f11").cast(FloatType)).withColumn("f12", col("f12").cast(FloatType)).withColumn("f13", col("f13").cast(FloatType)).withColumn("f14", col("f14").cast(FloatType)).withColumn("f15", col("f15").cast(FloatType)).withColumn("f16", col("f16").cast(FloatType)).withColumn("f17", col("f17").cast(FloatType)).withColumn("f18", col("f18").cast(FloatType)).withColumn("f19", col("f19").cast(FloatType)).withColumn("split", typedLit("val"))

val testFilePath = "/opt/spark/work-dir/Titanic/test_data.csv"
var testDf = spark.read.format("csv").option("header", "true").load(testFilePath)
testDf = testDf.withColumn("f1", col("f1").cast(FloatType)).withColumn("f2", col("f2").cast(FloatType)).withColumn("f3", col("f3").cast(FloatType)).withColumn("f4", col("f4").cast(FloatType)).withColumn("f5", col("f5").cast(FloatType)).withColumn("f6", col("f6").cast(FloatType)).withColumn("f7", col("f7").cast(FloatType)).withColumn("f8", col("f8").cast(FloatType)).withColumn("f9", col("f9").cast(FloatType)).withColumn("f10", col("f10").cast(FloatType)).withColumn("f11", col("f11").cast(FloatType)).withColumn("f12", col("f12").cast(FloatType)).withColumn("f13", col("f13").cast(FloatType)).withColumn("f14", col("f14").cast(FloatType)).withColumn("f15", col("f15").cast(FloatType)).withColumn("f16", col("f16").cast(FloatType)).withColumn("f17", col("f17").cast(FloatType)).withColumn("f18", col("f18").cast(FloatType)).withColumn("f19", col("f19").cast(FloatType)).withColumn("split", typedLit("test"))


val tablePath = "s3://lakesoul-test-bucket/titanic"
trainDf.write.mode("append").format("lakesoul").option("rangePartitions", "split").option("shortTableName", "titanic").save(tablePath)

testDf.write.mode("append").format("lakesoul").option("rangePartitions", "split").option("shortTableName", "titanic").save(tablePath)

valDf.write.mode("append").format("lakesoul").option("rangePartitions", "split").option("shortTableName", "titanic").save(tablePath)
```

#### imdb

```scala
val trainFilePath = "/opt/spark/work-dir/imdb/imdb-train.parquet"
var trainDf = spark.read.format("parquet").load(trainFilePath)

val testFilePath = "/opt/spark/work-dir/imdb/imdb-test.parquet"
var testDf = spark.read.format("parquet").load(testFilePath)

val tablePath = "s3://lakesoul-test-bucket/imdb"
trainDf.withColumn("split", typedLit("train")).write.mode("append").format("lakesoul").option("rangePartitions", "split").option("shortTableName", "imdb").save(tablePath)

testDf.withColumn("split", typedLit("test")).write.mode("append").format("lakesoul").option("rangePartitions", "split").option("shortTableName", "imdb").save(tablePath)
```

#### clip

```scala

val tablePath = "s3://lakesoul-test-bucket/clip_dataset"

val path0 = "/opt/spark/work-dir/clip/0000.parquet"
var df0 = spark.read.format("parquet").load(path0)
df0.withColumn("range", typedLit("0000")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path1 = "/opt/spark/work-dir/clip/0001.parquet"
var df1 = spark.read.format("parquet").load(path1)
df1.withColumn("range", typedLit("0001")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path2 = "/opt/spark/work-dir/clip/0002.parquet"
var df2 = spark.read.format("parquet").load(path2)
df2.withColumn("range", typedLit("0002")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path3 = "/opt/spark/work-dir/clip/0003.parquet"
var df3 = spark.read.format("parquet").load(path3)
df3.withColumn("range", typedLit("0003")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path4 = "/opt/spark/work-dir/clip/0004.parquet"
var df4 = spark.read.format("parquet").load(path4)
df4.withColumn("range", typedLit("0004")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path5 = "/opt/spark/work-dir/clip/0005.parquet"
var df5 = spark.read.format("parquet").load(path5)
df5.withColumn("range", typedLit("0005")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path6 = "/opt/spark/work-dir/clip/0006.parquet"
var df6 = spark.read.format("parquet").load(path6)
df6.withColumn("range", typedLit("0006")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

val path7 = "/opt/spark/work-dir/clip/0007.parquet"
var df7 = spark.read.format("parquet").load(path7)
df7.withColumn("range", typedLit("0007")).write.mode("append").format("lakesoul").option("rangePartitions", "range").option("shortTableName", "clip_dataset").save(tablePath)

```