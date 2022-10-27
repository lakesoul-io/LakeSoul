import pymysql

table_num = 15
table_type = 4
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
table_type = int(property['table_type'])
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

for i in range(table_num):
  cur.execute("drop table if exists random_table_%s" % str(i))

connect.commit()
cur.close()
connect.close()