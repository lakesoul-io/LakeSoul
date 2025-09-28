# Image search on Food101 dataset
## Introduction
Validate the inference task of a multimodal model using the Food 101 dataset with LakeSoul. Assumming our current working directory is `LakeSoul/python/examples`.

## Prepare dataset
We can download data from [Hugginface Food101 dataset](https://huggingface.co/datasets/food101/tree/refs%2Fconvert%2Fparquet) into `dataset` directory.
```bash
hf download ethz/food101 --repo-type dataset ethz/food101 --local-dir data
```

## Import data into LakeSoul
```shell
export lakesoul_jar=lakesoul-spark-3.3-3.0.0.jar
docker run --rm -it --net lakesoul-docker-compose-env_default \
-v $PWD/"${lakesoul_jar}":/opt/spark/work-dir/jars/"${lakesoul_jar}" \
-v $PWD/../../dist/lakesoul-1.1.0-cp38-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl/:/opt/bitnami/spark/lakesoul.whl \
-v $PWD/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
-v $PWD/food101:/opt/spark/work-dir/food101 \
--env LAKESOUL_HOME=/opt/spark/work-dir/lakesoul.properties \
swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/bitnami/spark:3.3.1 spark-submit --jars /opt/spark/work-dir/jars/"${lakesoul_jar}" --driver-memory 16G --executor-memory 16G --master "local[4]" --conf spark.pyspark.python=./venv/bin/python3 --py-files /opt/bitnami/spark/lakesoul.whl /opt/spark/work-dir/food101/import_data.py
```
Note: lakesoul-spark jar file can be downloaded from: https://github.com/lakesoul-io/LakeSoul/releases/download/v3.0.0/lakesoul-spark-3.3-3.0.0.jar

## Vectorizing pictures in LakeSoul
```shell
python food101/embedding.py clip_dataset > food101/embs.tsv
```

## Search for images
Since it is a food dataset, you can try food-related keywords.

```shell
python food101/search.py food101/embs.tsv 5
```
