# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pymysql

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

default_sql = """alter table default_init_1 modify column extra_1 float default NULL, modify column extra_2 varchar(255) default NULL"""
sql = """alter table random_table_%s modify column extra_1 float default NULL, modify column extra_2 varchar(255) default NULL"""

print(default_sql)
cur.execute(default_sql)

for i in range(table_num):
    exec_sql = sql % str(i)
    print(exec_sql)
    cur.execute(exec_sql)

connect.commit()
cur.close()
connect.close()
