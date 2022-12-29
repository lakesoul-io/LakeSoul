## Start A Local PostgreSQL DB
The quickest way to start a pg DB is via docker container:
```shell
docker run -d --name yugabyte  -p7000:7000 -p9000:9000 -p5433:5433 -p9042:9042\
 yugabytedb/yugabyte:latest bin/yugabyted start\
 --daemon=false
```
If you would like to deploy a pg cluster, please refer to docs of YugabyteDB: https://docs.yugabyte.com/preview/quick-start/install/linux/ or use a cloud PG DB service.

## PG Database Initialization
First, you should connect to pg DB
```shell
export PGPASSWORD='yugabyte';psql -U yugabyted  -h localhost -d postgres
```
After connected to the pg DB, execute the following statement to create a database
```sql
create database test_lakesoul_meta ;
```

Then you could init pg database of LakeSoul by `script/meta_init.sql`.

  ```
  PGPASSWORD=yugabyte psql -h localhost -p 5433 -U yugabyte -f script/meta_init.sql
  ```
## Lakesoul PG Database Configuration Description:
By default, the PG database is connected to the local database. The configuration information is as follows,
```txt
driver=org.postgresql.Driver,
jdbcUrl=jdbc:postgresql://127.0.0.1:5433/test_lakesoul_meta?stringtype=unspecified
username=yugabyte
password=yugabyte
```

LakeSoul supports to customize PG database configuration information. Add an environment variable `lakesoul_home` before starting the Spark program to include the configuration file information.

For example, the PG database configuration information file path name is: `/opt/soft/pg.property`, you need to add this environment variable before the program starts:
```
export lakesoul_home=/opt/soft/pg.property
```
The content format of the configuration file is as follows:
```txt
lakesoul.pg.driver=org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://127.0.0.1:5433/test_lakesoul_meta?stringtype=unspecified
lakesoul.pg.username=yugabyte
lakesoul.pg.password=yugabyte
```
You can use customized database configuration information here.

## Install an Apache Spark environment
You could download spark distribution from https://spark.apache.org/downloads.html , and please choose spark version 3.1.2.

After unpacking spark package, you could find LakeSoul distribution jar from https://github.com/meta-soul/LakeSoul/releases. Download the tar.gz file corresponding to your spark version and put it into `jars` directory of your spark environment:
```bash
tar xf lakesoul-2.0.0-spark-3.1.2-jars.tar.gz -C $SPARK_HOME/jars
```

## Start spark-shell for testing LakeSoul
cd into the spark installation directory, and start an interactive spark-shell:
```shell
 ./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
```
  
## LakeSoul Spark Conf Parameters
Before start to use Lakesoul, we should add some paramethers in `spark-defaults.conf` or `Spark Session Builder`。

| Key | Value | Description |
|---|---|---|
spark.sql.extensions | com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension | extention name for spark sql