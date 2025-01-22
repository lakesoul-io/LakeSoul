# LakeSoul ANN Benchmark Environment

## 运行步骤

1. 编译测试类
```bash
bash compile_test_class.sh
```
这个脚本会编译所需的测试类文件。

2. 启动所有必需的容器
```bash
bash start_all.sh
```
此脚本将启动以下服务:
- PostgreSQL
- Spark
- MinIO

3. 准备 ANN 测试表
```bash
bash prepare_ann_table.sh
```
此脚本将创建并填充用于 ANN 测试的表。

4. 运行查询测试
```bash
bash run_query.sh
```
此脚本将执行预定义的查询测试用例。
