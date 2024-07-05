# Traing binary classification on PyTorch
## Introduction
In this case, we solve a classification problem based on the LakeSoul and PyTorch framework. Assumming our current working directory is `LakeSoul/python/examples/`.
```shell
cd python/examples
```

## Prepare dataset
We can download the dataset from [Kaggle Titanic Dataset](https://www.kaggle.com/competitions/titanic/data). Then we put the train.csv and test.csv files in `titanic/dataset`:
```txt
titanic/dataset
├── test.csv
└── train.csv
```

## Import data into LakeSoul
```shell
export lakesoul_jar=lakesoul-spark-3.3-2.5.3.jar
docker run --rm -ti --net lakesoul-docker-compose-env_default \
    -v $PWD/"${lakesoul_jar}":/opt/spark/work-dir/jars/"${lakesoul_jar}" \
    -v $PWD/../../python/lakesoul/:/opt/bitnami/spark/lakesoul \
    -v $PWD/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
    -v $PWD/titanic:/opt/spark/work-dir/titanic \
    --env LAKESOUL_HOME=/opt/spark/work-dir/lakesoul.properties \
    bitnami/spark:3.3.1 spark-submit --jars /opt/spark/work-dir/jars/"${lakesoul_jar}" --driver-memory 16G --executor-memory 16G --master "local[4]" --conf spark.pyspark.python=./venv/bin/python3 /opt/spark/work-dir/titanic/import_data.py
```
Note: lakesoul-spark jar file can be downloaded from: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.5.3/lakesoul-spark-3.3-2.5.3.jar

## Feature Transform
```shell
export lakesoul_jar=lakesoul-spark-3.3-2.5.3.jar
docker run --rm -ti --net lakesoul-docker-compose-env_default \
    -v $PWD/"${lakesoul_jar}":/opt/spark/work-dir/jars/"${lakesoul_jar}" \
    -v $PWD/../../python/lakesoul/:/opt/bitnami/spark/lakesoul \
    -v $PWD/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
    -v $PWD/titanic:/opt/spark/work-dir/titanic \
    --env LAKESOUL_HOME=/opt/spark/work-dir/lakesoul.properties \
    bitnami/spark:3.3.1 spark-submit --jars /opt/spark/work-dir/jars/"${lakesoul_jar}" --driver-memory 16G --executor-memory 16G --master "local[4]" --conf spark.pyspark.python=./venv/bin/python3 /opt/spark/work-dir/titanic/feature_transform.py
```

## Train model on PyTorch
``` shell
# make sure $PWD is python/examples
# export necessary environment variables
source lakesoul_env.sh
# activate virtual environment
conda activate lakesoul_test
# run training
python titanic/train.py
```

# Reference:
1. https://www.kaggle.com/competitions/titanic
2. https://www.kaggle.com/code/kiranscaria/titanic-pytorch
