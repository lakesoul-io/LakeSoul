#!/bin/bash

# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Note: Please ensure that these ports on the machine are not occupied: 5432, 3306, 8081, 9000, 9001
# Note: Please install python3 before running auto_test.sh
# Note: It is recommended that you install Docker-Desktop(or docker and docker-compose) in advance before running auto_test.sh
# Note: Due to different machines' performance, you may need to adjust the sleep time in the function "sleep_after_ddl_or_dml"
# Note: Please use command "bash auto_test.sh" to run auto_test.sh

# Check if docker is installed. If not, then install it automatically
function docker_install {
  echo "====== Check if docker is installed ======"
  docker -v
  if [ $? -eq 0 ]; then
    echo "====== Docker is installed! ======"
  else
    echo "====== Installing Docker ======"
    curl -sSL https://get.daocloud.io/docker | sh
    echo "====== Docker is installed! ======!"
  fi
}

# Check if docker-compose is installed. If not, then install it automatically
function docker_compose_install {
  echo "====== Check if docker-compose is installed ======"
  docker compose version
  if [ $? -eq 0 ]; then
    echo "====== docker-compose is installed! ======"
  else
    echo "====== Installing docker-compose ======"
    DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}
    mkdir -p $DOCKER_CONFIG/cli-plugins
    curl -SL https://github.com/docker/compose/releases/download/v2.12.2/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose
    chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
    echo "====== Docker is installed! ======"
  fi
}

# Check if the modules we need are installed. If not, then install them
function python_module_install {
  # Check if pymysql is installed
  if python3 -c "import pymysql"; then
    echo "====== pymysql is installed ======"
  else
    echo "====== Installing pymysql ======"
    pip3 install pymysql
    echo "====== pymysql is installed ======"
  fi
}

# Change current path from Lakesoul/script/benchmark to LakeSoul/docker/lakesoul-docker-compose-env
function change_dir_from_script_benchmark_to_docker_compose {
  cd ../../docker/lakesoul-docker-compose-env
}

# Change current path from LakeSoul/docker/lakesoul-docker-compose-env to Lakesoul/script/benchmark
function change_path_from_docker_compose_to_script_benchmark {
  cd ../../script/benchmark
}

# Start Flink CDC Job
function start_flink_cdc_job {
  docker exec -ti lakesoul-docker-compose-env-jobmanager-1 flink run -d -c org.apache.flink.lakesoul.entry.MysqlCdc /opt/flink/work-dir/lakesoul-flink-2.1.1-flink-1.14-SNAPSHOT.jar --source_db.host mysql --source_db.port 3306 --source_db.db_name test_cdc --source_db.user root --source_db.password root --source.parallelism 4 --sink.parallelism 4 --warehouse_path s3://lakesoul-test-bucket/data/ --flink.checkpoint s3://lakesoul-test-bucket/chk --flink.savepoint s3://lakesoul-test-bucket/svp --job.checkpoint_interval 10000 --server_time_zone UTC # Start Flink CDC Job
}

# Insert data into tables in mysql:test_cdc database
function insert_data_into_tables {
  echo "====== Inserting data into tables ======"
  docker exec -ti lakesoul-docker-compose-env-mysql-1 sh /2_insert_table_data.sh
  echo "====== Data has been inserted successfully! ======"
}

# Insert data into tables by pass db info
function insert_data_into_tables_pass_db_info {
  echo "====== Inserting data into tables ======"
  for ((i = 0; i < $table_num; i++)); do ./mysql_random_data_insert --no-progress -h $host -u $user -p$password --max-threads=10 $db random_table_$i $row_num; done
  echo "====== Data has been inserted successfully! ======"
}

# Sleep for a while to make sure the flink cdc task syncs mysql data to LakeSoul
function sleep_after_ddl_or_dml {
  echo "====== Flink cdc is synchronizing mysql data to lakesoul ======"
  sleep 3m
  echo "====== Flink cdc has synchronized mysql data to lakesoul ======"
}

# Verify data consistency between mysql and LakeSoul
function verify_data_consistency {
  echo "====== Verifing data consistency ======"
  docker run --cpus 4 -m 16000m --net lakesoul-docker-compose-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --jars /opt/spark/work-dir/lakesoul-spark-2.2.0-spark-3.3.jar,/opt/spark/work-dir/mysql-connector-java-8.0.30.jar --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.apache.spark.sql.lakesoul.benchmark.Benchmark --master local[4] /opt/spark/work-dir/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar
  echo "====== Verifing has been completed! ======"
}

# Verify data consistency
function verify_data_consistency_pass_db_info {
  echo "====== Verifing data consistency ======"
  docker run --cpus 4 -m 16000m --net lakesoul-docker-compose-env_default --rm -t -v ${PWD}/work-dir:/opt/spark/work-dir --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 spark-submit --driver-memory 14G --executor-memory 14G --conf spark.driver.memoryOverhead=1500m --conf spark.executor.memoryOverhead=1500m --jars /opt/spark/work-dir/lakesoul-spark-2.2.0-spark-3.3.jar,/opt/spark/work-dir/mysql-connector-java-8.0.30.jar --conf spark.hadoop.fs.s3.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.buffer.dir=/tmp --conf spark.hadoop.fs.s3a.fast.upload.buffer=disk --conf spark.hadoop.fs.s3a.fast.upload=true --class org.apache.spark.sql.lakesoul.benchmark.Benchmark --master local[4] /opt/spark/work-dir/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar $host $db $user $password $port $timezone
  echo "====== Verifing has been completed! ======"
}

# Clean up metadata in lakesoul-meta-db
function clean_up_metadata {
  echo "====== Cleaning up metadata in lakesoul-meta-db ======"
  docker exec -ti lakesoul-docker-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
  echo "====== Cleaning up metadata in lakesoul-meta-db has been completed! ======"
}

# Clean up data in minio
function clean_up_minio_data {
  echo "====== Cleaning up data in minio ======"
  docker run --net lakesoul-docker-compose-env_default --rm -t bitnami/spark:3.3.1 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://lakesoul-test-bucket/
  echo "====== Cleaning up data in minio has been completed! ======"
}

# Note: If cluster is on k8s, mysql, pg and s3 services will not be started through the docker in the following ways.
# By default, the Flink cdc task has been started by yourself, this script only checks the data accuracy,
IS_ON_K8S=false
if [ $# -ge 1 ]; then
  if [ $1 = "k8s" ]; then
    IS_ON_K8S=true
  fi
fi

table_num=$(cat ./properties | grep -v '^#' | grep table_num= | awk -F'=' '{print $2}')
row_num=$(cat ./properties | grep -v '^#' | grep row_num= | awk -F'=' '{print $2}')
host=$(cat ./properties | grep -v '^#' | grep host= | awk -F'=' '{print $2}')
port=$(cat ./properties | grep -v '^#' | grep port= | awk -F'=' '{print $2}')
db=$(cat ./properties | grep -v '^#' | grep db= | awk -F'=' '{print $2}')
user=$(cat ./properties | grep -v '^#' | grep user= | awk -F'=' '{print $2}')
password=$(cat ./properties | grep -v '^#' | grep password= | awk -F'=' '{print $2}')
timezone=$(cat ./properties | grep -v '^#' | grep timezone= | awk -F'=' '{print $2}')

#docker_install
#docker_compose_install
#python_module_install
cd ./work-dir
if [ ! -e lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar ]; then
  wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-spark-2.2.0-spark-3.3-SNAPSHOT-tests.jar
fi
if [ ! -e lakesoul-spark-2.2.0-spark-3.3.jar ]; then
  wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-spark-2.2.0-spark-3.3.jar
fi
if [ ! -e mysql-connector-java-8.0.30.jar ]; then
  wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
fi
#pull spark docker image
docker pull bitnami/spark:3.3.1

cd ..

if [ !$IS_ON_K8S ]; then
  change_dir_from_script_benchmark_to_docker_compose
  echo "====== Start docker compose services ======"
  docker compose up -d # Use docker-compose to start services
  sleep 30s
  echo "====== Docker compose services have been started! ======"
  echo "====== Starting flink cdc job ======"
  start_flink_cdc_job
  sleep 30s
  echo "====== Flink cdc job has been started ======"
  change_path_from_docker_compose_to_script_benchmark
fi

echo "====== Creating tables in mysql:test_cdc database ======"
python3 1_create_table.py # Create tables in mysql:test_cdc database
echo "====== Tables have been created! ======"

if [ !$IS_ON_K8S ]; then
  #docker exec -ti lakesoul-docker-compose-env-mysql-1 chmod 755 /2_insert_table_data.sh # Modify permissions for 2_insert_table_data.sh
  insert_data_into_tables
  sleep_after_ddl_or_dml
  verify_data_consistency
else
  insert_data_into_tables_pass_db_info
  sleep_after_ddl_or_dml
  verify_data_consistency_pass_db_info
fi

sleep 5s
echo "====== Adding columns for tables and deleting some data from tables ======"
python3 3_add_column.py # Add columns for tables in mysql:test_cdc database
python3 delete_data.py
echo "====== Adding columns and deleting some data have been completed! ======"

if [ !$IS_ON_K8S ]; then
  insert_data_into_tables
  sleep_after_ddl_or_dml
  verify_data_consistency
else
  insert_data_into_tables_pass_db_info
  sleep_after_ddl_or_dml
  verify_data_consistency_pass_db_info
fi

sleep 5s
echo "====== Updating data in tables ======"
python3 4_update_data.py # Update data in mysql:test_cdc tables
echo "====== Data in tables has been updated! ======"
sleep_after_ddl_or_dml

if [ !$IS_ON_K8S ]; then
  verify_data_consistency
else
  verify_data_consistency_pass_db_info
fi

sleep 5s
# TODO: change column type is not supported now, it will be supported in the future
#echo "====== Changing columns and deleting some data in tables ======"
#python3 5_change_column.py # Change columns in mysql:test_cdc tables
#python3 delete_data.py
#echo "====== hanging columns and deleting some data have been completed! ======"
#if [ !$IS_ON_K8S ]; then
#  insert_data_into_tables
#  sleep_after_ddl_or_dml
#  verify_data_consistency
#else
#  insert_data_into_tables_pass_db_info
#  sleep_after_ddl_or_dml
#  verify_data_consistency_pass_db_info
#fi
#sleep 5s
echo "====== Dropping columns and deleting some data in tables ======"
python3 6_drop_column.py # Drop columns in mysql:test_cdc tables
python3 delete_data.py
echo "====== Dropping columns and deleting some data have been completed! ======"

if [ !$IS_ON_K8S ]; then
  insert_data_into_tables
  sleep_after_ddl_or_dml
  verify_data_consistency
else
  insert_data_into_tables_pass_db_info
  sleep_after_ddl_or_dml
  verify_data_consistency_pass_db_info
fi

sleep 5s

if [ !$IS_ON_K8S ]; then
  echo "====== Dropping tables ======"
  python3 drop_table.py
  echo "====== Dropping tables has been completed! ======"
  clean_up_metadata
  clean_up_minio_data
  change_dir_from_script_benchmark_to_docker_compose
  echo "====== Stopping docker compose services ======"
  docker compose down # Use docker-compose to stop services
  echo "====== Docker compose services have been stopped! ======"
  echo "====== Congratulations! You have successfully tested LakeSoul for correctness! ======"
fi
