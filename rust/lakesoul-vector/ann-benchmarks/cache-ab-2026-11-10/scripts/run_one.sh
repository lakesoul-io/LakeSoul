#!/bin/bash
set -u
ds=$1
cd /home/zhibin/LakeSoul/python
ROOT=/tmp/bench2
BASE="LAKESOUL_PG_URL=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test PYTHONPATH=src"
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_DEFAULT_REGION=us-east-1
mkdir -p $ROOT/results
rm -rf $ROOT/cache
# clean any previous
env $BASE LAKESOUL_VECTOR_CACHE=0 timeout 300 .venv/bin/python $ROOT/cleanup2.py $ds >/dev/null 2>&1
aws --endpoint-url http://127.0.0.1:9000 s3 rm --recursive s3://lakesoul-bench2/bench2_$ds/ >/dev/null 2>&1
echo "SETUP $ds"
env $BASE LAKESOUL_VECTOR_CACHE=0 timeout 3600 .venv/bin/python $ROOT/setup2.py $ds 2>/dev/null | tail -1 > $ROOT/results/${ds}_setup.json
cat $ROOT/results/${ds}_setup.json
if [ "$ds" = deep ]; then alt=4096; primary=1024; else alt=256; primary=1024; fi
: > $ROOT/results/${ds}_build.jsonl
: > $ROOT/results/${ds}_search.jsonl
for cfg in "alt 1" "alt 7" "primary 1" "primary 7"; do
  set -- $cfg; which=$1; bits=$2
  if [ "$which" = alt ]; then nlist=$alt; else nlist=$primary; fi
  echo "BUILD $ds $nlist/$bits"
  env $BASE LAKESOUL_VECTOR_CACHE=0 timeout 3600 .venv/bin/python $ROOT/build2.py $ds $nlist $bits 2>/dev/null | tail -1 | tee -a $ROOT/results/${ds}_build.jsonl
  if [ "$which" = primary ]; then
    if [ "$bits" = 1 ]; then nps="8 16 32"; else nps="16 32"; fi
  else nps="16"; fi
  for np in $nps; do
    echo "SEARCH $ds $nlist/$bits np=$np cache=on"
    rm -rf $ROOT/cache
    env $BASE LAKESOUL_VECTOR_CACHE=1 LAKESOUL_VECTOR_MEMORY_BYTES=8GiB LAKESOUL_CACHE_PATH=$ROOT/cache LAKESOUL_CACHE_SIZE=16GiB LAKESOUL_VECTOR_LAZY_INDEX=auto \
      timeout 3600 .venv/bin/python $ROOT/search2.py $ds $nlist $bits $np on 2>/dev/null | tail -1 | tee -a $ROOT/results/${ds}_search.jsonl
    rm -rf $ROOT/cache
  done
  if [ "$which" = primary ] && [ "$bits" = 1 ]; then
    echo "SEARCH $ds $nlist/$bits np=16 cache=off"
    rm -rf $ROOT/cache
    env $BASE LAKESOUL_VECTOR_CACHE=0 timeout 3600 .venv/bin/python $ROOT/search2.py $ds $nlist $bits 16 off 2>/dev/null | tail -1 | tee -a $ROOT/results/${ds}_search.jsonl
    rm -rf $ROOT/cache
  fi
done
# release table, S3 and local cache immediately
echo "CLEAN $ds"
env $BASE LAKESOUL_VECTOR_CACHE=0 timeout 300 .venv/bin/python $ROOT/cleanup2.py $ds 2>/dev/null | tail -1
aws --endpoint-url http://127.0.0.1:9000 s3 rm --recursive s3://lakesoul-bench2/bench2_$ds/ >/dev/null 2>&1
rm -rf $ROOT/cache
df -h / | tail -1
