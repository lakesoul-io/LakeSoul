# Image search on Food101 dataset
## Introduction
Validate the inference task of a multimodal model using the Food 101 dataset with LakeSoul. Assumming our current working directory is `LakeSoul/python/examples/`.

## Prepare dataset
We can download data from [Hugginface Food101 dataset](https://huggingface.co/datasets/food101/tree/refs%2Fconvert%2Fparquet) into `imdb/dataset/` directory.


## Import data into LakeSoul
```shell
export lakesoul_jar=lakesoul-spark-2.3.0-spark-3.3-SNAPSHOT.jar
sudo docker run --rm -ti --net lakesoul-docker-compose-env_default \
-v $PWD/"${lakesoul_jar}":/opt/spark/work-dir/jars/"${lakesoul_jar}" \
-v $PWD/../../python/lakesoul/:/opt/bitnami/spark/lakesoul \
-v $PWD/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
-v $PWD/food101:/opt/spark/work-dir/food101 \
--env lakesoul_home=/opt/spark/work-dir/lakesoul.properties \
bitnami/spark:3.3.1 spark-submit --jars /opt/spark/work-dir/jars/"${lakesoul_jar}" --driver-memory 16G --executor-memory 16G --master "local[4]" --conf spark.pyspark.python=./venv/bin/python3 /opt/spark/work-dir/food101/import_data.py
```

## Vectorizing pictures in LakeSoul
```shell
python food101/embedding.py clip_dataset > food101/embs.tsv
```

## Search for images
Since it is a food dataset, you can try food-related keywords.

```shell
python food101/search.py food101/embs.tsv 5
```
