# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

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
feature_columns = 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f24,f25,f26'.split(',')

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
            print('Epoch {}'.format(epoch+1))
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