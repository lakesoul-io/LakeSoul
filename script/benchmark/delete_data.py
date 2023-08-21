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
delete_num = 1000

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
delete_num = property['delete_num']

connect = pymysql.connect(host=host,
                          user=user,
                          password=password,
                          port=port,
                          db=db,
                          charset='utf8')

cur = connect.cursor()

sql_default = """delete from default_init limit %s""" % delete_num
cur.execute(sql_default)

sql_1 = """delete from random_table_%s limit %s"""

for i in range(table_num):
    exec_sql = sql_1 % (str(i), delete_num)
    print(exec_sql)
    cur.execute(exec_sql)

connect.commit()
cur.close()
connect.close()
