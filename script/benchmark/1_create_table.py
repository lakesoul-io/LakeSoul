# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pymysql
import random

table_num = 100
host = 'localhost'
user = 'root'
password = 'root'
port = 3306
db = 'ddf_1'

property = {}

with open("./properties") as file:
    for line in file.readlines():
        line = line.strip()
        if line.find('=') > 0 and not line.startswith('#'):
            strs = line.split('=')
            property[strs[0].strip()] = strs[1].strip()

table_num = int(property['table_num'])
host = property['host']
user = property['user']
password = property['password']
port = int(property['port'])
db = property['db']

connect = pymysql.connect(host=host,
                          user=user,
                          password=password,
                          port=port,
                          db=db,
                          charset='utf8')

cur = connect.cursor()

element_list = ["col_1 bigint default null,", "col_2 char(1) default null,", "col_3 date default null,",
                "col_4 datetime default null,", "col_5 decimal(10,2) default null,", "col_6 double default null,",
                "col_7 enum('spring','summer','autumn','winter'),", "col_8 int default null,",
                "col_9 longtext default null,", "col_10 mediumint default null,", "col_11 mediumtext default null,",
                "col_12 decimal(10,0) default null,", "col_13 double default null,",
                "col_14 set('fisrt','second','third','fourth','fifth'),", "col_15 smallint default null,",
                "col_16 text default null,", "col_17 timestamp default null,", "col_18 tinyint default null,",
                "col_19 tinytext default null,", "col_20 varchar(255) default null,"]

create_sql = """create table random_table_%s (id int NOT NULL, %s PRIMARY KEY (`id`))"""
for i in range(table_num):
    column_num = random.randint(1, 10)
    index_set = set()
    column_str = ""
    for j in range(column_num):
        index = random.randint(0, len(element_list) - 1)
        while index in index_set:
            index = random.randint(0, len(element_list) - 1)
        index_set.add(index)
        column_str = column_str + element_list[index]
    exec_sql = create_sql % (str(i), column_str)
    print(exec_sql)
    cur.execute("DROP TABLE IF EXISTS `random_table_%s`" % (str(i)))
    cur.execute(exec_sql)

connect.commit()
cur.close()
connect.close()
