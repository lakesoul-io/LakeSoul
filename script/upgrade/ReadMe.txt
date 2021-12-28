use this script to upgrade meta tables in cassandra

usage:
1.use cqlsh to execute upgrade sql script, like
cqlsh --file="test_upgrade.sql"
or
cqlsh -u cassandra -p cassandra --file="test_upgrade.sql"
if you use PasswordAuthenticator.