#!/bin/bash

# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# execute this script to initalize meta data db for local testing

set -ex

BASEDIR=$(dirname "$0")

PARALLELISM=1

usage() {
  echo "Usage: $0 -j NUM_PARALLEL_FOR_TEST"
}

while getopts "hj:" arg; do
  case $arg in
    j)
      PARALLELISM=${OPTARG}
      echo "Create $PARALLELISM database for testing"
      ;;
    h | *)
      usage
      exit 0
      ;;
  esac
done

init_database() {
  PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -tc "SELECT 1 FROM pg_database WHERE datname = '$1'" | grep -q 1 || PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -c "CREATE DATABASE $1"
  PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f "$BASEDIR"/meta_rbac_init.sql $1
  PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f "$BASEDIR"/meta_rbac_init_domains_for_test.sql $1
  PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f "$BASEDIR"/meta_rbac_init_users_for_test.sql $1
}

init_database "lakesoul_test"

if [ "$PARALLELISM" -gt "1" ]; then
  for i in $(seq 1 "$PARALLELISM");
  do
    init_database "lakesoul_test_$i"
  done
fi