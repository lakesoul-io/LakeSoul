application mode

# PG

docker run -d --name lakesoul-e2e-pg -p4321:5432 -e POSTGRES_USER=lakesoul_e2e -e POSTGRES_PASSWORD=lakesoul_e2e -e POSTGRES_DB=lakesoul_e2e -d swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/postgres:14.5 

docker cp meta_init.sql lakesoul-e2e-pg:/ 

docker exec -i lakesoul-e2e-pg sh -c "PGPASSWORD=lakesoul_e2e psql -h localhost -p 5432 -U lakesoul_e2e -f meta_init.sql"

# important
pipx install is slow

# K8S

## flink 

flink run-application --target kubernetes-application \
--class com.dmetasoul.FlinkDataInit \
-Dkubernetes.cluster-id=lakesoul-e2etest \
-Dkubernetes.container.image.ref=swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/flink:1.20.1-scala_2.12-java11 \
-Dcontainerized.master.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar \
-Dcontainerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.20.1.jar \
-Duser.artifacts.artifact-list=s3://dmetasoul-bucket/jiax/target/lakesoul-flink.jar \
s3://dmetasoul-bucket/jiax/target/first-love-0.1.jar


## Spark
用镜像的时候指定 k8s 环境变量

spark-submit \
   --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
   --jars
   https://.....
   --deploy-mode cluster \
   --name spark-pi \
   --class org.apache.spark.examples.SparkPi \
   --conf spark.executor.instances=5 \
   --conf spark.kubernetes.container.image=<spark-image> \
   --packages org.apache.hadoop:hadoop-aws:3.4.1 # maven coordinate
   --conf spark.kubernetes.file.upload.path=s3a://<s3-bucket>/path
   --conf spark.hadoop.fs.s3a.access.key=...
   --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
   --conf spark.hadoop.fs.s3a.fast.upload=true
   --conf spark.hadoop.fs.s3a.secret.key=....
   --conf spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp
   --conf spark.kubernetes.driver.podTemplateFile=s3a://bucket/driver.yml
   --conf spark.kubernetes.executor.podTemplateFile=s3a://bucket/executor.yml
   file:///full/path/to/app.jar
   local:///path/to/examples.jar
   

e2e 用法

RABC:
   serviceaccount spark/flink is needed