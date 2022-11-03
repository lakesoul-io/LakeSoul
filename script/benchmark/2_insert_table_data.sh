#!/bin/sh

table_num=$(cat ./properties | grep table_num |awk -F'=' '{print $2}')
row_num=$(cat ./properties | grep row_num |awk -F'=' '{print $2}')

host=$(cat ./properties | grep host |awk -F'=' '{print $2}')
user=$(cat ./properties | grep user |awk -F'=' '{print $2}')
password=$(cat ./properties | grep password |awk -F'=' '{print $2}')
db=$(cat ./properties | grep db |awk -F'=' '{print $2}')

for((i=0;i<$table_num;i++));do ./mysql_random_data_insert -u $user -p$password --max-threads=10 $db random_table_$i $row_num ;done

