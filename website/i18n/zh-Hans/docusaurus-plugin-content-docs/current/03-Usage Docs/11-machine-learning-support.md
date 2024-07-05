# LakeSoul Python 接口和对机器学习框架的支持

LakeSoul 实现了 PyTorch/ PyArrow/ HuggingFace / Ray 的数据源接口， 用户可以使用 LakeSoul 存储机器学习数据集，并可以通过接口读取 LakeSoul 表的数据，支持分布式读取。目前
Python 接口发布了 1.0 Beta 版。

## 安装方法

### 下载 LakeSoul Wheel文件

对于使用Python 3.8、Python 3.9和Python 3.10的用户，我们为每个版本准备了不同的wheel文件。请根据您的需求下载适当的一个。我们近期会发布正式版的包到 pypi.org.

Python 包目前仅支持 Linux x86_64 系统。Python 包基于 manylinux_2_28 镜像构建，支持 CentOS 8、Debian 10、Ubuntu 18.10 及以上的 OS 版本（具体兼容性可查看[Distro compatibility](https://github.com/mayeut/pep600_compliance?tab=readme-ov-file#distro-compatibility)）。如需要在更低版本的 OS 上运行，建议使用 Docker 容器的方式执行。

Python 3.8：[lakesoul-1.0.0b2-cp38-cp38-manylinux_2_28_x86_644.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp38-cp38-manylinux_2_28_x86_64.whl)

Python 3.9：[lakesoul-1.0.0b2-cp39-cp39-manylinux_2_28_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp39-cp39-manylinux_2_28_x86_64.whl)

Python 3.10：[lakesoul-1.0.0b2-cp310-cp310-manylinux_2_28_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp310-cp310-manylinux_2_28_x86_64.whl)

假设我们使用Python 3.8，我们可以按照以下方式下载wheel文件

```shell
wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b2-cp38-cp38-manylinux_2_28_x86_64.whl
```

### 安装 python 虚拟环境

我们提供了多个 Python 读取 LakeSoul 表的示例，可以通过以下方式安装环境并体验：

```bash
# change python version if needed
conda create -n lakesoul_test python=3.8
conda activate lakesoul_test
git clone https://github.com/lakesoul-io/LakeSoul.git
cd LakeSoul/python/examples
# replace ${PWD} with your wheel file directory in requirements.txt
pip install -r requirements.txt
```

### LakeSoul 环境搭建
使用时需要参考 [LakeSoul 快速搭建运行环境](../01-Getting%20Started/01-setup-local-env.md) 文档中的方法，搭建 LakeSoul 环境，并通过 `LAKESOUL_PG_URL`, `LAKESOUL_PG_USERNAME`, `LAKESOUL_PG_PASSWORD` 这几个环境变量配置 LakeSoul 元数据库的连接信息。如果是按照文档中使用 docker compose 启动的本地测试环境，则这几个环境变量的配置为：
```bash
export LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

## PyTorch 使用说明

LakeSoul 实现 PyTorch/ HuggingFace 接口，可以直接将 LakeSoul 表的数据读取成 HuggingFace 的 datasets.

读取表的 API：

```python
import datasets
import lakesoul.huggingface

dataset = datasets.IterableDataset.from_lakesoul("lakesoul_table", partitions={'split': 'train'})
```

即可创建 PyTorch/HuggingFace 的 dataset，进行训练。分布式训练环境会自动感知，在 dataset 初始化时不需要添加额外参数。

下面给出从 LakeSoul 中导出经过特征转换的 [Titanic](https://www.kaggle.com/competitions/titanic) 数据集，然后用 DNN 模型进行训练和验证的示例代码。

```python
import argparse
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.nn.init as init

import datasets
import lakesoul.huggingface

from torch.autograd import Variable

# hyper parameters
SEED = 0
torch.manual_seed(SEED)
torch.cuda.manual_seed(SEED)
batch_size = 50
num_epochs = 50
learning_rate = 0.01
weight_decay = 0.005

# label and feature columns
label_column = 'label'
feature_columns = 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26'.split(
    ',')


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.bn = nn.BatchNorm1d(26)
        self.fc1 = nn.Linear(26, 256, bias=True)
        self.fc2 = nn.Linear(256, 2, bias=True)
        self._initialize_weights()

    def forward(self, x):
        x = self.bn(x)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.fc2(x)
        x = torch.sigmoid(x)
        return x

    def _initialize_weights(self):
        for m in self.modules():
            if isinstance(m, nn.Linear):
                init.xavier_uniform_(m.weight)
                if m.bias is not None:
                    init.constant_(m.bias, 0)


def batchify(dataset, batch_size):
    X_train = []
    y_train = []
    for i, item in enumerate(dataset):
        feature_list = [item[feature] for feature in feature_columns]
        X_train.append(feature_list)
        y_train.append(int(item[label_column]))
        if len(y_train) == batch_size:
            yield X_train, y_train
            X_train = []
            y_train = []
    # Handle the remaining records that don't fill up a full batch
    if len(y_train) > 0:
        yield X_train, y_train


def train_model(net, datasource, num_epochs, batch_size, learning_rate):
    dataset = datasets.IterableDataset.from_lakesoul(datasource, partitions={'split': 'train'})

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.AdamW(net.parameters(), lr=learning_rate, weight_decay=weight_decay)

    for epoch in range(num_epochs):
        if epoch % 5 == 0:
            print('Epoch {}'.format(epoch + 1))
        for X_train, y_train in batchify(dataset, batch_size):
            x_var = Variable(torch.FloatTensor(X_train))
            y_var = Variable(torch.LongTensor(y_train))
            optimizer.zero_grad()
            ypred_var = net(x_var)
            loss = criterion(ypred_var, y_var)
            loss.backward()
            optimizer.step()


def evaluate_model(net, datasource, batch_size):
    dataset = datasets.IterableDataset.from_lakesoul(datasource, partitions={'split': 'val'})
    num_samples = 0
    num_correct = 0

    for X_val, y_val in batchify(dataset, batch_size):
        batch_size = len(y_val)
        test_var = Variable(torch.FloatTensor(X_val))
        with torch.no_grad():
            result = net(test_var)
        values, labels = torch.max(result, 1)
        num_right = np.sum(labels.data.numpy() == y_val)
        num_samples += batch_size
        num_correct += num_right

    accuracy = num_correct / num_samples
    print('Accuracy {:.2f}'.format(accuracy))


def main(table):
    net = Net()
    train_model(net, table, batch_size, num_epochs, learning_rate)
    evaluate_model(net, table, batch_size)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str, default='titanic_trans', help='lakesoul table name')
    args = parser.parse_args()

    main(args.table)

```

更多的示例可以参考 [LakeSoul/python/examples](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples)

## Ray 读取 LakeSoul 表

LakeSoul 实现了 Ray 的 [Datasource](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html)。以下为调用代码示例：

```python
import ray.data
import lakesoul.ray

ds = ray.data.read_lakesoul("table_name", partitions={'split': 'train'})
```

## PyArrow/Pandas 读取 LakeSoul 表

LakeSoul 可以支持单机读取数据，并使用 PyArrow、Pandas 进行计算。LakeSoul 读取时返回 PyArrow
的 [Dataset](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html) 对象，支持迭代访问。示例：

```python
from lakesoul.arrow import lakesoul_dataset

ds = lakesoul_dataset("table_name", partitions={'split': 'train'})

# iterate batches in dataset
# this will not load entire table to memory
for batch in ds.to_batches():
    ...

# convert to pandas table
# this will load entire table into memory
df = ds.to_table().to_pandas()
```
