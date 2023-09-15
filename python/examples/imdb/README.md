# Text classification on IMDB dataset
## Introduction
Demonstrate the capability of fine-tuning a BERT model using the HuggingFace Trainer API on the IMDB dataset available on LakeSoul's data source. Assumming our current working directory is `LakeSoul/python/examples/`.

## Prepare data
We can download data from [Hugginface IMDB dataset](https://huggingface.co/datasets/imdb/tree/refs%2Fconvert%2Fparquet/plain_text/train) into `imdb/dataset/` directory.

## Import data into LakeSoul
```shell
export lakesoul_jar=lakesoul-spark-2.3.0-spark-3.3-SNAPSHOT.jar
sudo docker run --rm -ti --net lakesoul-docker-compose-env_default \
-v $PWD/"${lakesoul_jar}":/opt/spark/work-dir/jars/"${lakesoul_jar}" \
-v $PWD/../../python/lakesoul/:/opt/bitnami/spark/lakesoul \
-v $PWD/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
-v $PWD/imdb:/opt/spark/work-dir/imdb \
--env lakesoul_home=/opt/spark/work-dir/lakesoul.properties \
bitnami/spark:3.3.1 spark-submit --jars /opt/spark/work-dir/jars/"${lakesoul_jar}" --driver-memory 16G --executor-memory 16G --master "local[4]" --conf spark.pyspark.python=./venv/bin/python3 /opt/spark/work-dir/imdb/import_data.py
```

## Train model using HuggingFace Trainer API
```shell
conda activate lakesoul_test
python imdb/train.py 
```

## Inference the trained model
```shell 
python imdb/inference.py
```

##  Reference:
1. https://huggingface.co/docs/transformers/tasks/sequence_classification
2. https://huggingface.co/datasets/imdb

