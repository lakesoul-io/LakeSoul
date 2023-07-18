# Flink SQL 使用教程

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

本教程完整演示使用 Flink SQL ，完成从一个 MySQL 数据库中，使用 CDC 同步数据到 LakeSoul ODS 层表中，并流式执行聚合计算，更新 LakeSoul DWS 层表的完整流程。

## 搭建环境
请参考 [Flink Connector](../03-Usage%20Docs/06-flink-lakesoul-connector.md)，在本地启动一个 Flink local cluster ，并启动 SQL Client 。

## 