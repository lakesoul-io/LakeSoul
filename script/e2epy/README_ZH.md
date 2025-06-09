# Overview

每个source读取文件然后写入湖中

no currency issue

2 stage

第一阶段
从文件中读取数据

table name: lakesoul-integ-test-basic

根据schema构建通过data gen csv文件


# Implementation



## Status
- [ ] data init
- [ ] flink write data (deploy: local)
- [ ] flink write data (deploy: k8s)
- [ ] flink read data (deploy: local)
- [ ] flink read data (deploy: k8s)
- [ ] spark ... 