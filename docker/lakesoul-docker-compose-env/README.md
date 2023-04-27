# How to start docker compose:

1. Download hadoop:

```bash
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz -O $HOME/hadoop-3.3.5.tar.gz && tar xf $HOME/hadoop-3.3.5.tar.gz -C $HOME
export HADOOP_HOME=$HOME/hadoop-3.3.5
```

2. Download flink-s3-hadoop:

```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.0/flink-s3-fs-hadoop-1.17.0.jar -O $HOME/flink-s3-fs-hadoop-1.17.0.jar
```

3. Start docker compose

```bash
cd docker/lakesoul-docker-compose-env
docker compose up -d
```