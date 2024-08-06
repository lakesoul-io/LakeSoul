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

default_sql = """update default_init set col_8 = round(RAND() * 10000, 6), col_10 = FLOOR(RAND() * 10000), col_23 = MD5(RAND() * 10000), col_24 = MD5(RAND() * 10000)"""
print(default_sql)
cur.execute(default_sql)

default_sql_1 = """update default_init_1 set extra_1 = round(RAND() * 10000, 6), extra_2 = FLOOR(RAND() * 10000), extra_3 = MD5(RAND() * 10000), col_24 = MD5(RAND() * 10000)"""
print(default_sql_1)
cur.execute(default_sql_1)

sql = """update random_table_%s set extra_1 = FLOOR(RAND() * 10000), extra_2 = round(RAND() * 10000, 6), extra_3 = MD5(RAND() * 10000)"""

for i in range(table_num):
    exec_sql = sql % str(i)
    print(exec_sql)
    cur.execute(exec_sql)

connect.commit()
cur.close()
connect.close()
