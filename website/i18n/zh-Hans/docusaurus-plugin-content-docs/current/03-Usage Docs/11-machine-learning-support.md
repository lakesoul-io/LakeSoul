# LakeSoul Python 接口和对机器学习框架的支持

LakeSoul 实现了 PyTorch/ PyArrow/ HuggingFace 接口， 用户可以使用 LakeSoul 存储机器学习数据集，并可以通过接口读取 LakeSoul 表的数据。目前 Python 接口发布了 1.0 Beta 版。

## 安装方法

### 下载 LakeSoul Wheel文件

对于使用Python 3.8、Python 3.9和Python 3.10的用户，我们为每个版本准备了不同的wheel文件。请根据您的需求下载适当的一个。我们近期会发布正式版的包到 pypi.org.

Python 包目前仅支持 Linux 系统，在 GLibc 2.17 以上均可使用（Centos 7 及以上，Ubuntu 16.04 及以上）。

对于Python
3.8用户：[lakesoul-1.0.0b0-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b0-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

对于Python
3.9用户：[lakesoul-1.0.0b0-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b0-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

对于Python
3.10用户：[lakesoul-1.0.0b0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

假设我们使用Python 3.8，我们可以按照以下方式下载wheel文件

```shell
wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/python/v1.0/lakesoul-1.0.0b0-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

### 安装 python 虚拟环境

```bash 
conda create -n lakesoul_test python=3.8
conda acitvate lakesoul_test
# replace ${PWD} with your working directory
pip install -r requirements.txt
```

## 使用说明

LakeSoul 实现 PyTorch/ HuggingFace 接口，可以直接将 LakeSoul 表的数据导出成 HuggingFace 的 datasets.

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