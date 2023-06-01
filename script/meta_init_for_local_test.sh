#!/bin/bash

# execute this script to initalize meta data db for local testing

set -ex

BASEDIR=$(dirname "$0")

docker run -d --name lakesoul-test-pg -p5432:5432 -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_DB=lakesoul_test -d postgres:14.5
sleep 30s

PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f "$BASEDIR"/meta_init.sql lakesoul_test
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -c "CREATE DATABASE lakesoul_test_1"
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f "$BASEDIR"/meta_init.sql lakesoul_test_1
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -c "CREATE DATABASE lakesoul_test_2"
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f "$BASEDIR"/meta_init.sql lakesoul_test_2