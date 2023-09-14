# LakeSoul Python Examples

## Prerequisites

### Deploy Docker compose env

```bash
cd docker/lakesoul-docker-compose-env
docker compose up -d
```

### Pull spark image

```bash
docker pull bitnami/spark:3.3.1
```

### Download LakeSoul release jar

```bash
wget https://github.com/lakesoul-io/LakeSoul/releases/download/v2.3.1/lakesoul-spark-2.3.1-spark-3.3.jar 
```

### Download LakeSoul whl file
```bash
```


### Install python virtual enviroment
```bash 
conda create -n lakesoul_test python=3.8
conda acitvate lakesoul_test
pip install -r requirements.txt
```

## Run Examples
Before running the examples, please export the LakeSoul environment variables by executing the command:

```bash
source lakesoul_env.sh
```

Afterwards, we can test the examples using the instructions below.

| Project                              | Dataset                              | Base Model                                | 
|:-------------------------------------|:-------------------------------------|:------------------------------------------|
| [Titanic](./titanic/) | [Kaggle Titanic Dataset](https://www.kaggle.com/competitions/titanic) | `DNN` |
| [IMDB Sentiment Analysis](./imdb/) | [Hugginface IMDB dataset](https://huggingface.co/datasets/imdb/tree/refs%2Fconvert%2Fparquet/plain_text/train) | [distilbert-base-uncased](https://huggingface.co/distilbert-base-uncased) |
| [Food Image Search](./food101/) | [Hugginface Food101 dataset](https://huggingface.co/datasets/food101/tree/refs%2Fconvert%2Fparquet) | [clip-ViT-B-32](https://huggingface.co/sentence-transformers/clip-ViT-B-32) |
