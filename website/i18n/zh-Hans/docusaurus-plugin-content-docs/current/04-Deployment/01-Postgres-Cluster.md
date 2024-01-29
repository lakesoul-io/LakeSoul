# 部署 PostgreSQL 高可用集群

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 使用Pigsty安装部署

我们推荐使用自动化工具部署 PostgreSQL 高可用集群，例如 Pigsty。

Pigsty 官方介绍： https://doc.pigsty.cc/#/zh/README

### 下载Pigsty安装包
```ini
curl -L https://get.pigsty.cc/v2.5.1/pigsty-v2.5.1.tgz -o ~/pigsty.tgz 
curl -L https://get.pigsty.cc/v2.5.1/pigsty-pkg-v2.5.1.el7.x86_64.tgz -o /tmp/pkg.tgz
```
### 安装部署

1、各节点ssh免密登陆
```ini
ssh-keygen -t rsa
ssh-copy-id root@hosts
```
:::tip
hosts 为包含自己在内的所有节点，都要免密
:::

2、各节点初始化

```ini
bash -c "$(curl -fsSL https://get.pigsty.cc/latest)"  
cd ~/pigsty
./bootstrap 
```

3、主节点配置集群信息，修改~/pigsty/pigsty.yml，参考如下：

```yaml
all:
  children:
    # infra cluster for proxy, monitor, alert, etc..
    infra: { hosts: { 192.168.17.91: { infra_seq: 1 } } }
    # etcd cluster for ha postgres
    etcd: { hosts: { 192.168.17.91: { etcd_seq: 1 } }, vars: { etcd_cluster: etcd } }
    # minio cluster, optional backup repo for pgbackrest
    #minio: { hosts: { 192.168.17.91: { minio_seq: 1 } }, vars: { minio_cluster: minio } }
    # postgres cluster 'pg-meta' with single primary instance
    pg-test:
      hosts:
        192.168.17.91: { pg_seq: 1, pg_role: primary }   # primary instance, leader of cluster
        192.168.25.215: { pg_seq: 2, pg_role: replica }   # replica instance, follower of leader
        192.168.28.137: { pg_seq: 3, pg_role: replica, pg_offline_query: true } # replica with offline access
      vars:
        pg_cluster: pg-test           # define pgsql cluster name
        pg_users:  [{ name: test , password: test , pgbouncer: true , roles: [ dbrole_admin ] }]
        pg_databases: [{ name: test }]
        pg_vip_enabled: true
        pg_vip_address: 192.168.17.92/24
        pg_vip_interface: eth0
        node_tune: tiny
        pg_conf: tiny.yml
        node_crontab:  # make a full backup on monday 1am, and an incremental backup during weekdays
          - '00 01 * * 1 postgres /pg/bin/pg-backup full'
          - '00 01 * * 2,3,4,5,6,7 postgres /pg/bin/pg-backup'
  vars:                               # global parameters
    version: v2.5.1                   # pigsty version string
    admin_ip: 192.168.17.91             # admin node ip address
    region: china                     # upstream mirror region: default,china,europe
```
:::tip
pg_vip_address需要配置
:::

4、主节点执行安装脚本
```ini
./install.yml 
```

### 验证
```ini
patronictl -c /etc/patroni/patroni.yml list
```
:::tip
手工切换：patronictl -c /etc/patroni/patroni.yml switchover

数据备份：/pg/bin/pg-backup full;在/etc/crontab中存在全量和增量备份定时任务
:::

### 监控
Grafana
```ini
http://192.168.17.91:3000
```
:::tip
使用正确的ip地址
:::