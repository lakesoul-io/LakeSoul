## 1. CDC 入湖介绍
LakeSoul 提供了一套独立的 CDC 语义表达规范，通过表属性设置一个 CDC Op 列，即可表示每条数据的操作类型，在后续 Merge 时会自动根据操作语义进行合并。可以通过 Debezium、Canal、Flink 等将 CDC 数据转换后导入 LakeSoul。这里提供一个端到端的完整示例。

## 2. Mysql 设置
### 2.1 创建库和测试表
    ```sql
    Create database cdc
    CREATE TABLE test(
    id int primary key,
    rangeid int,
    value varchar(100) 
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 
    ```

### 2.2 Benchmark 数据生成器

等流程全部组件搭建完后，使用MysqlBenchMark进行测试  

使用步骤：
  - unzip MysqlBenchmark.zip
  - 手工修改mysqlcdc.conf相关配置

    ```ini
    ###Mysql configure###
    user=Mysql用户名
    passwd=密码
    host=Mysql主机
    port=Mysql端口
    ```

  - 往指定表插入数据（最后使用）
    ```
    bash MysqlCdcBenchmark.sh  insert  cdc(库名) test(表名) 10(插入行数) 1(线程数)
    ```

  - 更新指定表数据（最后使用）
    ```
    bash MysqlCdcBenchmark.sh  update  cdc(库名) test(表名) id(主键) value(更新列) 10(更新行数)
    ```

  - 删除指定表数据（最后使用）
    ```
    bash  MysqlCdcBenchmark.sh  delete  cdc(库名)  test(表名)  10(删除行数)
    ```


## 3. Kafka 安装和设置

### 3.1 安装 Kafka(已有请忽略)

- 通过 Kafka K8s operator-Deploying and Upgrading (0.28.0) (strimzi.io) 
    ```bash
    kubectl create -f install/cluster-operator -n my-cluster-operator-namespace
    kubectl apply -f examples/kafka/kafka-persistent-single.yaml
    ```

- 其他方法安装请参考官方文档 - [Apache Kafka](https://kafka.apache.org/downloads)

## 4. Debezium 安装设置
### 4.1 安装(已有请忽略)
  - 通过 K8s 来快速启动一个 Debezium 服务的容器 pod，pod 配置
    ```yaml
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: dbz-pod-claim
    spec:
    accessModes:
        - ReadWriteOnce
    # 这里填写 k8s 集群中 storage class
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
            # 这里替换成实际 kafka 的 IP 地址
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

    kubectl apply -f pod.yaml
    # 注：确认storage和namespace信息
    ```

-  其他-参考官网 [Debezium Release Series 1.8](https://debezium.io/releases/1.8/)

### 4.2 使用 Debezium 创建CDC同步任务
- 对接 Mysql 和 kafka 并创建 Debezium CDC 同步任务
（dbzhost 需要替换为 debezium 服务 IP）
    ```bash
    curl -X POST http://dbzhost:8083/connectors/ -H 'Cache-Control: no-cache' -H 'Content-Type: application/json' -d '{
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

    注：host 等字段需要根据实际情况修改；输出到kafka topic为"database.server.name". "table.include.list"两个字段值的拼接

- 查询 CDC 同步任务是否创建成功
    ```bash
    curl -H "Accept:application/json" dbzhost:8083 -X GET http://dbzhost:8083/connectors/
    ```
- 测试完成后可以删除同步任务
    ```
    curl -i -X DELETE http://dbzhost:8083/connectors/cdc
    ```

## 5. Spark + Lakesoul CDC 入湖
### 5.1 安装

请参考 [快速开始](../../../../wiki/快速开始)

### 5.2 使用 Spark Streaming，消费 Kafka 数据并同步更新至 LakeSoul

- Spark-shell启动
    ```bash
    ./bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --conf spark.dmetasoul.lakesoul.meta.host=localhost --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.dmetasoul.lakesoul.meta.database.name=test_lakesoul_meta --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
    ```

- 创建lakesoul表
  
    我们创建一个 LakeSoul 表 MysqlCdcTest，这个表会准实时流批一体地同步 MySQL 的数据。这个表同样使用 id 列作为主键，用 "op" 列表示CDC的更新。并且我们需要通过 lakesoul_cdc_change_column 这个表属性，指定 LakeSoul 表中，表示 CDC 状态更新的列名，这个示例中该列的名字为 "op"。
    ```scala
    import com.dmetasoul.lakesoul.tables.LakeSoulTable
    val path="/opt/spark/cdctest"
    val data=Seq((1L,1L,"hello world","insert")).toDF("id","rangeid","value","op")
    LakeSoulTable.createTable(data, path).shortTableName("cdc").hashPartitions("id").hashBucketNum(2).rangePartitions("rangeid").tableProperty("lakesoul_cdc_change_column" -> "op").create()
    ```

- 启动 streaming 写入 LakeSoul

    读取 kafka，转换 Debezium 读取出的 json 格式，使用lakesoul upsert更新 LakeSoul 表：
    ```scala
    import com.dmetasoul.lakesoul.tables.LakeSoulTable
    val path="/opt/spark/cdctest"
    val lakeSoulTable = LakeSoulTable.forPath(path)
    var strList = List.empty[String]

    //js1 是示例数据，我们这里也用于生成schema，在下文from_json函数中转换数据使用，before和after中内容对应于mysql表字段
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

    //对接kafka 需要指定kafka.bootstrap.server ip地址和debezium输出到kafka的topic
    val kfdf = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafkahost:9092")
    .option("subscribe", "cdcserver.cdc.test")
    .option("startingOffsets", "latest")
    .load()

    //解析debezium中产生的json，对于Mysql insert、update、delete操作会自动生成一列，列名op
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

    //使用lakesoul upsert更新表中数据并在屏幕上输出解析后的数据
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

- 模拟Mysql负载

    参考上面的 Mysql Benchmark 工具，对线上 MySQL 表进行增、改、删操作
查看lakesoul数据
    ```scala
    import com.dmetasoul.lakesoul.tables.LakeSoulTable
    val path="/opt/spark/cdctest"
    val lakeSoulTable = LakeSoulTable.forPath(path)
    lakeSoulTable.toDF.select("*").show()
    ```