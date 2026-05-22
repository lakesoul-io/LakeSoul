# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

docker exec -ti lakesoul-docker-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
docker run --net lakesoul-docker-compose-env_default --rm -t -e AWS_ACCESS_KEY_ID=rustfsadmin -e AWS_SECRET_ACCESS_KEY=rustfsadmin -e AWS_DEFAULT_REGION=us-east-1 swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/spark:v3.1.2 aws --endpoint-url http://rustfs:9000 s3 rm --recursive s3://lakesoul-test-bucket/
