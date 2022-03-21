#!/bin/bash
. ./mysqlcdc.conf
function randomStr() {
     echo $RANDOM|base64
}
function genUpdateSql() {
   table=$1
   key=$2
   keyval=$3
   upcol=$4
   upval=$5
   echo "update $table set ${upcol}=\"${upval}\" where ${key}=${keyval};"
}
function execUpdataSql(){
   echo "-------------------------------------"
   echo "#start updating at `date`"
   prikey=$3	
   database=$1
   table=$2
   num=$5
   field=$4   
   mysql -N -h$host -u$user -p$passwd -P$port -e  "select $prikey from $database.$table limit $num" > /tmp/result
   for line in `cat /tmp/result`
   do
	str=`randomStr`
        genUpdateSql "$table" "$prikey" "$line" "$field" $str >> /tmp/updateresult.sql
   done
   mysql -h$host -u$user -p$passwd -P$port -D$database < /tmp/updateresult.sql
	if [ $? == 0 ];then
		echo "#Finish update at `date`"
	else
		echo "#Error update "
	fi
   rm -rf /tmp/updateresult.sql
}
function execInsert(){
database=$1
table=$2
num=$3
threads=$4
./mysql_random_data_insert -h $host -u $user -p $passwd --max-threads=$threads  $database $table $num
}
function execDelte(){
	echo "-------------------------------------"
	echo "#start deleting at `date`"
	mysql -h$host -u$user -p$passwd -P$port -e  "delete from $1.$2 limit $3"
	if [ $? == 0 ];then
		echo "#Finish Delete at `date`"
	else
		echo "#Error Delete "
	fi
}
echo "####usage#####"
echo "#### sh MysqlCdcBenchmark.sh insert database table insertnum threadnums ####"
echo "#### sh MysqlCdcBenchmark.sh delete database table deletenum  ####"
echo "#### sh MysqlCdcBenchmark.sh update database table primarykey(int) updatefield updatenum ####"
if [ "$1" == "insert" ];then
	execInsert $2 $3 $4 $5 $6
fi
if [ "$1" == "update" ];then
	execUpdataSql $2 $3 $4 $5 $6  
fi
if [ "$1" == "delete" ];then
	execDelte $2 $3 $4 
fi
