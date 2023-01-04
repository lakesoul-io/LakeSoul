#!/bin/bash

table_num=$(cat ./properties | grep -v '^#' | grep table_num= | awk -F'=' '{print $2}')
row_num=$(cat ./properties | grep -v '^#' | grep row_num= | awk -F'=' '{print $2}')

host=$(cat ./properties | grep -v '^#' | grep host= | awk -F'=' '{print $2}')
user=$(cat ./properties | grep -v '^#' | grep user= | awk -F'=' '{print $2}')
password=$(cat ./properties | grep -v '^#' | grep password= | awk -F'=' '{print $2}')
db=$(cat ./properties | grep -v '^#' | grep db= | awk -F'=' '{print $2}')

for ((i = 0; i < $table_num; i++)); do ./mysql_random_data_insert --no-progress -u "$user" -p"$password" --max-threads=10 "$db" random_table_"$i" "$row_num"; done
