docker exec -it lakesoul-ann-spark spark-submit \
  --class org.apache.spark.sql.lakesoul.benchmark.ann.LocalSensitiveHashANN \
  --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  /opt/bitnami/spark/jars/lakesoul-spark-3.3-2.6.2-SNAPSHOT.jar --HDF5_FILE /data/embeddings/fashion-mnist-784-euclidean.hdf5 --LAKESOUL_WAREHOUSE s3://lakesoul-test-bucket