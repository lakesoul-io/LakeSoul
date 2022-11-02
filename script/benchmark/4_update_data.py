import pymysql
import random

table_num = 100
host='5bfe414e935c4a6a8e5f5d4696f33940in01.internal.cn-southwest-2.mysql.rds.myhuaweicloud.com'
user='root'
password='@Dmetasoul_1#23'
port=3306
db='ddf_1'

property = {}

with open("./properties") as file:
  for line in file.readlines():
    line = line.strip()
    if(line.find('=') > 0 and not line.startswith('#')):
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

sql_1 = """update random_table_%s set extra_1 = FLOOR(RAND() * 10000)"""
sql_2 = """update random_table_%s set extra_2 = round(RAND() * 10000, 6)"""
sql_3 = """update random_table_%s set extra_3 = MD5(RAND() * 10000)"""


for i in range(table_num):
  exec_sql = sql_1 % str(i)
  print(exec_sql)
  cur.execute(exec_sql)


for i in range(table_num):
  exec_sql = sql_2 % str(i)
  print(exec_sql)
  cur.execute(exec_sql)


for i in range(table_num):
  exec_sql = sql_3 % str(i)
  print(exec_sql)
  cur.execute(exec_sql)

connect.commit()
cur.close()
connect.close()