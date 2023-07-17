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

sql_1 = """alter table random_table_%s drop column extra_1"""
sql_2 = """alter table random_table_%s drop column extra_2"""
sql_3 = """alter table random_table_%s drop column extra_3"""

sql_list = [sql_1, sql_2, sql_3]

for i in range(table_num):
    index = random.randint(0, len(sql_list) - 1)
    exec_sql = sql_list[index] % str(i)
    print(exec_sql)
    cur.execute(exec_sql)

connect.commit()
cur.close()
connect.close()
