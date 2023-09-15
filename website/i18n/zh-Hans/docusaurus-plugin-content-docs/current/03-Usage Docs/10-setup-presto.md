# 使用 Presto 查询 LakeSoul 表

:::tip
从 2.4 版本起提供。
:::

LakeSoul 实现了 Presto Connector，目前支持读取湖仓表，能够读取无主键表、有主键表（包括 [CDC 格式表](04-cdc-ingestion-table.mdx)），读取时会自动执行 Merge on Read 获取最新的数据。

## 下载 Jar 包
可以从 Github Release 页面下载 Presto 的包：https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-presto-2.4.0-presto-0.28.jar

## 配置 Presto
在 Presto 目录下的 plugin 子目录下，新建 lakesoul 子目录。将上面下载好的 jar 放入该子目录。

新建 `etc/catalog/lakesoul.properties` 文件（具体的 etc 目录位置根据 presto 部署情况修改）：
```properties
connector.name=lakesoul
fs.s3a.access.key=minioadmin1
fs.s3a.secret.key=minioadmin1
fs.s3a.bucket=lakesoul-test-bucket
fs.s3a.endpoint=http://minio:9000
```
其中 `fs.s3a` 开头的设置项为访问 S3 的配置，可以根据需要修改。

## 配置 LakeSoul 元数据
参考 [配置元数据](01-setup-meta-env.md) 文档中的方法，使用环境变量或 JVM Property 等方式。例如可以在 [Presto JVM Config](https://prestodb.io/docs/current/installation/deployment.html#jvm-config) 中配置 JVM Properties。

## 启动 Presto Client
```shell
./bin/presto --catalog lakesoul --schema default
```