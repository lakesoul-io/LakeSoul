# An Introduction on LakeSoul CDC Ingestion

## 1. LakeSoul CDC Pipeline
LakeSoul supports ingesting any source of CDC by transforming CDC markups to LakeSoul's own field.

There are two ways of CDC ingestion for LakeSoul: 1) Write CDC stream into Kafka and use spark streaming to transform and write into LakeSoul (already supported); 2) Use Flink CDC to directly write into LakeSoul (Under development: [Support Flink CDC Write](https://github.com/meta-soul/LakeSoul/projects/1)).

In this demo, we'll demonstrate the first way. We'll setup a MySQL instance, use scripts to generate DB modifications and use Debezium to sync them into Kafka, and then into LakeSoul.

## 2. Setup MySQL
### 2.1 Create database and table
```sql
Create database cdc;
CREATE TABLE test(
 id int primary key,
 rangeid int,
 value varchar(100) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```
### 2.2 Use cdc benchmark generator:
We provide a mysql data generator for testing and benchmarking cdc sync. The generator is located under diretory `examples/cdc_ingestion_debezium/MysqlBenchmark`.
1. Modify mysqlcdc.conf as needed
   ```ini
    user=user name of mysql
    passwd=password of mysql
    host=host of mysql
    port=port of mysql
   ```
2. Insert data into table
   ```bash
   # Inside () are comments of parameters, remove them before execution
    bash MysqlCdcBenchmark.sh  insert  cdc(db name) test(table name) 10(lines to insert) 1(thread number)
   ```
3. Update data into table
   ```bash
   bash MysqlCdcBenchmark.sh  update  cdc test id(primary key) value(column to update) 10(lines to update) 
   ```
4. Delete data from table
   ```bash
    bash  MysqlCdcBenchmark.sh  delete  cdc  test  10(lines to delete)
   ```

## 3. Setup Kafka (Ignore this step if you already have Kafka running)
### 3.1 Install Kafka via K8s (https://strimzi.io/docs/operators/latest/deploying.html#deploying-cluster-operator-str):
```bash
kubectl create -f install/cluster-operator -n my-cluster-operator-namespace
kubectl apply -f examples/kafka/kafka-persistent-single.yaml
```

## 4. Setup Debezium (Ignore if you already have it)
### 4.1 Install Debezium
To quickly setup a running container of Debezium on K8s:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: dbz-pod-claim
spec:
  accessModes:
    - ReadWriteOnce
  # replace to actual StorageClass in your cluster
  storageClassName: 
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: dbz-pod
  namespace: dmetasoul
spec:
  restartPolicy: Never
  containers:
  - name: dbs
    image: debezium/connect:latest
    env:
      - name: BOOTSTRAP_SERVERS
        # replace to actual kafka host
        value: ${kafka_host}:9092
      - name: GROUP_ID
        value: "1"
      - name: CONFIG_STORAGE_TOPIC
        value: my_connect_configs
      - name: OFFSET_STORAGE_TOPIC
        value: my_connect_offsets
      - name: STATUS_STORAGE_TOPIC
        value: my_connect_statuses
    resources:
      requests:
        cpu: 500m
        memory: 4Gi
      limits:
        cpu: 4
        memory: 8Gi
    volumeMounts:
      - mountPath: "/kafka/data"
        name: dbz-pv-storage

  volumes:
    - name: dbz-pv-storage
      persistentVolumeClaim:
        claimName: dbz-pod-claim
```
Then apply this yaml file:
```bash
kubectl apply -f pod.yaml
```

### 4.2 Setup Debezium sync task
```bash
# remember to replace {dbzhost} to actual dbz deployment ip address
# replace database parameters accordingly
curl -X POST http://{dbzhost}:8083/connectors/ -H 'Cache-Control: no-cache' -H 'Content-Type: application/json' -d '{
    "name": "cdc",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "tasks.max": "1",
        "database.hostname": "mysqlhost",
        "database.port": "mysqlport",
        "database.user": "mysqluser",
        "database.password": "mysqlpassword",
        "database.server.id": "1",
        "database.server.name": "cdcserver",
        "database.include.list": "cdc",
        "database.history.kafka.bootstrap.servers": "kafkahost:9092",
        "database.history.kafka.topic": "schema-changes.cdc",
        "decimal.handling.mode": "double",
        "table.include.list":"cdc.test" 
    }
}'
```

Then check if sync task has been succcessfully created:
```bash
curl -H "Accept:application/json" dbzhost:8083 -X GET http://dbzhost:8083/connectors/
```

You could delete sync task after testing finished:
```bash
curl -i  -X DELETE http://dbzhost:8083/connectors/cdc
```

## 5. Start Spark Streaming Sink to LakeSoul
### 5.1 Setup
Please refer to [Quick Start](https://github.com/meta-soul/LakeSoul/wiki/02.-QuickStart) on how to setup LakeSoul and Spark environment.

### 5.2 Start Spark Shell
Spark shell needs to be started with kafka dependencies:
```bash
./bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --conf spark.dmetasoul.lakesoul.meta.host=localhost --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.dmetasoul.lakesoul.meta.database.name=test_lakesoul_meta --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
```

### 5.3 Create a LakeSoul Table
We'll create a LakeSoul table called MysqlCdcTest, which will sync with the MySQL table we just setup. The LakeSoul table also has a primary key `id`, and we need an extra field `op` to represent CDC ops and add a table property `lakesoul_cdc_change_column` with `op` field.
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val path="/opt/spark/cdctest"
val data=Seq((1L,1L,"hello world","insert")).toDF("id","rangeid","value","op")
LakeSoulTable.createTable(data, path).shortTableName("cdc").hashPartitions("id").hashBucketNum(2).rangePartitions("rangeid").tableProperty("lakesoul_cdc_change_column" -> "op").create()
```

### 5.4 Start spark streaming to sync Debezium CDC data into LakeSoul
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val path="/opt/spark/cdctest"
val lakeSoulTable = LakeSoulTable.forPath(path)
var strList = List.empty[String]

//js1 is just a fake data to help generate the schema
val js1 = """{
          |  "before": {
          |    "id": 2,
          |    "rangeid": 2,
          |    "value": "sms"
          |  },
          |  "after": {
          |    "id": 2,
          |    "rangeid": 2,
          |    "value": "sms"
          |  },
          |  "source": {
          |    "version": "1.8.0.Final",
          |    "connector": "mysql",
          |    "name": "cdcserver",
          |    "ts_ms": 1644461444000,
          |    "snapshot": "false",
          |    "db": "cdc",
          |    "sequence": null,
          |    "table": "sms",
          |    "server_id": 529210004,
          |    "gtid": "de525a81-57f6-11ec-9b60-fa163e692542:1621099",
          |    "file": "binlog.000033",
          |    "pos": 54831329,
          |    "row": 0,
          |    "thread": null,
          |    "query": null
          |  },
          |  "op": "c",
          |  "ts_ms": 1644461444777,
          |  "transaction": null
          |}""".stripMargin
strList = strList :+ js1
val rddData = spark.sparkContext.parallelize(strList)
val resultDF = spark.read.json(rddData)
val sche = resultDF.schema

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

// Specify kafka settings
val kfdf = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafkahost:9092")
  .option("subscribe", "cdcserver.cdc.test")
  .option("startingOffsets", "latest")
  .load()

// parse CDC json from debezium, and transform `op` field into one of 'insert', 'update', 'delete' into LakeSoul
val kfdfdata = kfdf
  .selectExpr("CAST(value AS STRING) as value")
  .withColumn("payload", from_json($"value", sche))
  .filter("value is not null")
  .drop("value")
  .select("payload.after", "payload.before", "payload.op")
  .withColumn(
    "op",
    when($"op" === "c", "insert")
      .when($"op" === "u", "update")
      .when($"op" === "d", "delete")
      .otherwise("unknown")
  )
  .withColumn(
    "data",
    when($"op" === "insert" || $"op" === "update", $"after")
      .when($"op" === "delete", $"before")
  )
  .drop($"after")
  .drop($"before")
  .select("data.*", "op")

// upsert into LakeSoul with microbatch
kfdfdata.writeStream
  .foreachBatch { (batchDF: DataFrame, _: Long) =>
    {
      lakeSoulTable.upsert(batchDF)
      batchDF.show
    }
  }
  .start()
  .awaitTermination()
```

### 5.5 Read from LakeSoul to view synchronized data:
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val path="/opt/spark/cdctest"
val lakeSoulTable = LakeSoulTable.forPath(path)
lakeSoulTable.toDF.select("*").show()
```