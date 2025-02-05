echo "Start prepare data for LSH ANN benchmark..."

docker exec -it lakesoul-ann-spark spark-submit \
  --class com.dmetasoul.lakesoul.spark.ann.LocalSensitiveHashANN \
  --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/bitnami/spark/jars/lakesoul-spark-3.3-2.6.2-SNAPSHOT.jar load --hdf5-file /data/embeddings/fashion-mnist-784-euclidean.hdf5 --warehouse s3://lakesoul-test-bucket --table-name ann_table --embedding-dim 784 --bit-width 512