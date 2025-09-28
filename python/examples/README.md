# LakeSoul Python Examples

## Prerequisites

### Deploy LakeSoul Docker Compose Env

```bash
cd lakesoul-docker-compose-env
docker compose up -d
```

### Pull spark image

```bash
docker pull swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/bitnami/spark:3.3.1
```

### Download LakeSoul release jar

1. download Spark release jar from https://github.com/lakesoul-io/LakeSoul/releases

```bash
conda create -n lakesoul_test python=3.8
conda activate lakesoul_test
# replace ${PWD} with your working directory
pip install -r requirements.txt
```

## Run Examples

Before running the examples, please export the LakeSoul environment variables by executing the command:

```bash
source lakesoul_env.sh
```
The content of `lakesoul_env.sh` is the following:
```shell
export AWS_SECRET_ACCESS_KEY=minioadmin1
export AWS_ACCESS_KEY_ID=minioadmin1
export AWS_ENDPOINT=http://localhost:9000

export LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

Afterwards, we can test the examples using the instructions below.

| Project                              | Dataset                              | Base Model                                |
|:-------------------------------------|:-------------------------------------|:------------------------------------------|
| [Titanic](./titanic/) | [Kaggle Titanic Dataset](https://www.kaggle.com/competitions/titanic) | `DNN` |
| [IMDB Sentiment Analysis](./imdb/) | [Hugginface IMDB dataset](https://huggingface.co/datasets/imdb/tree/refs%2Fconvert%2Fparquet/plain_text/train) | [distilbert-base-uncased](https://huggingface.co/distilbert-base-uncased) |
| [Food Image Search](./food101/) | [Hugginface Food101 dataset](https://huggingface.co/datasets/food101/tree/refs%2Fconvert%2Fparquet) | [clip-ViT-B-32](https://huggingface.co/sentence-transformers/clip-ViT-B-32) |
