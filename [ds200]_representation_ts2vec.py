# -*- coding: utf-8 -*-
"""[DS200] Representation_ts2vec.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/14bOWAd5aNJjyDFQ-3fVru4lCdzIAjYXm

https://github.com/yuezhihan/ts2vec/blob/main/README.md#code-example
"""

!git clone https://github.com/yuezhihan/ts2vec.git

#from ts2vec.models import TSEncoder
#from ts2vec.models.losses import hierarchical_contrastive_loss
#from ts2vec.utils import take_per_row, split_with_nan, centerize_vary_length_series, torch_pad_nan

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ts2vec.ts2vec import TS2Vec

from google.colab import drive
drive.mount('/content/drive')

"""# 1"""

path = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/'
path_split = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Split/'
path_repr = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Representation/'

def pro(data):
  data['Date'] = pd.to_datetime(data['Date'])
  data = data.sort_values('Date')
  data = data.set_index('Date')
  return data

def load_anomaly(path_split):
    train_data = pro(pd.read_csv(path_split + "train_data.csv"))
    train_labels = pd.read_csv(path_split + "train_labels.csv")
    test_data = pro(pd.read_csv(path_split + "test_data.csv"))
    test_labels = pd.read_csv(path_split + "test_labels.csv")
    return train_data, train_labels, test_data, test_labels

train_data, train_labels, test_data, test_labels = load_anomaly(path_split)

mean, std = train_data.mean(), train_data.std()
train_data = (train_data - mean) / std

mean, std = test_data.mean(), test_data.std()
test_data = (test_data - mean) / std

train_labels.sum()

t2v_train_data = np.expand_dims(train_data, axis=0)#
t2v_test_data = np.expand_dims(test_data, axis=0)
print(t2v_train_data.shape)
print(t2v_test_data.shape)

model = TS2Vec(
        input_dims=t2v_train_data.shape[-1],
        device='cpu',
    )

model.fit(t2v_train_data, verbose=True, n_epochs=5)

def get_all_repr(data):
        ts_repr_wom = model.encode(
            data,
            sliding_length=1,
            sliding_padding=50,
            batch_size=256
        ).squeeze()

        ts_repr = model.encode(
            data,
            mask='binomial',
            sliding_length=1,
            sliding_padding=50,
            batch_size=256
        ).squeeze()

        ts_repr = ts_repr - ts_repr_wom
        '''
        tạo ra một biểu diễn (representation) đặc biệt của dữ liệu bằng cách
        so sánh dữ liệu gốc với dữ liệu đã được ẩn đi một số đặc điểm bằng mask binomial.
        => tập trung vào các đặc trưng quan trọng và bất thường trong dữ liệu.
        '''

        return ts_repr

train_repr = get_all_repr(t2v_train_data)
test_repr = get_all_repr(t2v_test_data)

"""# **Save**"""

train_representation = pd.DataFrame(train_repr)
test_representation = pd.DataFrame(test_repr)

model.save(path_repr+"ts2vec_model.pkl")

train_representation.to_csv(path_repr+'train_repr.csv')
test_representation.to_csv(path_repr+'test_repr.csv')

train_representation

test_representation