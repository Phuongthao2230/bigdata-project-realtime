# -*- coding: utf-8 -*-
"""[DS200] DNN Feature.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Dn57N-mrthR8T18f737DTTy4eHfaG8k1
"""

import pandas as pd
import numpy as np
from collections import deque
import math
import collections
from sklearn.decomposition import PCA

from google.colab import drive
drive.mount('/content/drive')

path = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/'
path_split = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Split/'
path_repr = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Representation/'
path_uad = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/UAD/'
path_feature = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Feature/'

"""# **1. Hàm**"""

def pro(data):
  data['Date'] = pd.to_datetime(data['Date'])
  data = data.sort_values('Date')
  data = data.set_index('Date')
  return data

def load_anomaly(path):
    train_data = pro(pd.read_csv(path + "train_data.csv"))
    train_labels = pd.read_csv(path + "train_labels.csv")
    test_data = pro(pd.read_csv(path + "test_data.csv"))
    test_labels = pd.read_csv(path + "test_labels.csv")
    return train_data, train_labels, test_data, test_labels

def mean_std_normalize_df(p: 'DataFrame'):
    mean = p.mean()
    std = p.std()
    if std < ZERO:
        std = ZERO
    rs = (p - mean) / std
    return rs

ZERO = 0.00000001

def mean_std_normalize(p: list):
    p = [0 if _ is None else _ for _ in p]
    p = np.array(p)
    mean = p.mean()
    std = p.std()
    if std < ZERO:
        std = ZERO
    rs = (p - mean) / std
    return rs.tolist()

def ma_diff_ratio_features(values, windows):
    names = []
    features = []

    for w in windows:
        names.append('ma_' + str(w))
        tmp_feature = []
        tmp_sum = 0
        for i in range(len(values)):
            if i - w < -1:
                tmp_sum += values[i]
                tmp_feature.append(None)
            else:
                if i - w == -1:
                    tmp_sum += values[i]
                else:
                    tmp_sum += values[i] - values[i - w]
                tmp_feature.append(tmp_sum / w)
        features.append(tmp_feature)

        names.append('ma_diff_' + str(w))
        tmp_feature_1 = []
        for i in range(len(tmp_feature)):
            if tmp_feature[i] is None:
                tmp_feature_1.append(None)
            else:
                tmp_feature_1.append(values[i] - tmp_feature[i])
        features.append(tmp_feature_1)

        names.append('ma_ratio_' + str(w))
        tmp_feature_2 = []
        for i in range(len(tmp_feature)):
            if tmp_feature[i] is None:
                tmp_feature_2.append(None)
            else:
                safe_divisor = tmp_feature[i]
                if tmp_feature[i] < ZERO:
                    safe_divisor = ZERO
                tmp_feature_2.append(values[i] / safe_divisor)
        features.append(tmp_feature_2)

    return names, features


def ewma_diff_ratio_features(values, decays, windows):
    names = []
    features = []
    for w in windows:
        for d in decays:
            base = 0
            tmp = 1
            for i in range(w):
                base += tmp
                tmp *= (1 - d)

            names.append('ewma_' + str(w) + '_' + str(d))
            tmp_feature = []

            for i in range(len(values)):
                if i - w < -1:
                    tmp_feature.append(None)
                else:
                    factor = 1
                    numeraotr = 0
                    for j in range(w):
                        numeraotr += values[i - j] * factor
                        factor *= (1 - d)
                    tmp_feature.append(numeraotr / base)
            features.append(tmp_feature)

            names.append('ewma_diff_' + str(w) + '_' + str(d))
            tmp_feature_1 = []

            for i in range(len(tmp_feature)):
                if tmp_feature[i] is None:
                    tmp_feature_1.append(None)
                else:
                    tmp_feature_1.append(values[i] - tmp_feature[i])
            features.append(tmp_feature_1)

            names.append('ewma_ratio_' + str(w) + '_' + str(d))
            tmp_feature_2 = []

            for i in range(len(tmp_feature)):
                if tmp_feature[i] is None:
                    tmp_feature_2.append(None)
                else:
                    safe_divisor = tmp_feature[i]
                    if tmp_feature[i] < ZERO:
                        safe_divisor = ZERO
                    tmp_feature_2.append(values[i] / safe_divisor)
            features.append(tmp_feature_2)

    return names, features


def win_min_max_diff_std_quantile(values, windows):
    names = []
    features = []
    for w in windows:
        tmp_min = []
        tmp_deque = deque()
        for i in range(len(values)):
            while len(tmp_deque) > 0 and tmp_deque[-1][0] >= values[i]:
                tmp_deque.pop()
            tmp_deque.append((values[i], i))

            if i < w - 1:
                tmp_min.append(None)
            else:
                if tmp_deque[0][1] == i - w:
                    tmp_deque.popleft()
                tmp_min.append(tmp_deque[0][0])
        names.append('min_' + str(w))
        features.append(tmp_min)

        tmp_max = []
        tmp_deque = deque()
        for i in range(len(values)):
            while len(tmp_deque) > 0 and tmp_deque[-1][0] <= values[i]:
                tmp_deque.pop()
            tmp_deque.append((values[i], i))

            if i < w - 1:
                tmp_max.append(None)
            else:
                if tmp_deque[0][1] == i - w:
                    tmp_deque.popleft()
                tmp_max.append(tmp_deque[0][0])
        names.append('max_' + str(w))
        features.append(tmp_max)

        tmp_diff = []
        for i in range(len(tmp_min)):
            if tmp_min[i] is None:
                tmp_diff.append(None)
            else:
                tmp_diff.append(tmp_max[i] - tmp_min[i])
        names.append('min_max_diff_' + str(w))
        features.append(tmp_diff)

        ss, ss2 = 0, 0
        tmp_std = []
        for i in range(len(values)):
            ss += values[i]
            ss2 += values[i] * values[i]

            if i < w - 1:
                tmp_std.append(None)
            else:
                if i > w - 1:
                    ss -= values[i - w]
                    ss2 -= values[i - w] * values[i - w]

                tmp_std.append(math.sqrt(math.fabs((ss2 - ss * ss / w) / (w - 1))))

        names.append('std_' + str(w))
        features.append(tmp_std)

        tmp_quantile = []
        for i in range(len(values)):
            if i < w - 1:
                 tmp_quantile.append(None)
            else:
                tmp_q = 1
                for j in range(1, w):
                    if values[i - j] >= values[i]:
                        tmp_q += 1
                tmp_quantile.append(tmp_q / w)
        names.append('quantile_' + str(w))
        features.append(tmp_quantile)

    return names, features


def diff_ratio(values):
    names = []
    features = []

    tmp_diff = [None]
    for i in range(1, len(values)):
        tmp_diff.append(values[i] - values[i - 1])
    names.append('diff')
    features.append(tmp_diff)

    tmp_diff_ratio = [None]
    for i in range(1, len(values)):
        divisor = values[i - 1]
        if divisor <= ZERO:
            divisor = ZERO
        tmp_diff_ratio.append(tmp_diff[i] / divisor)
    names.append('diff_ratio')
    features.append(tmp_diff_ratio)

    return names, features


def log_minus_divid(values):
    names = []
    features = []

    values_t = [math.fabs(x) + 1.1 for x in values]

    tmp_log = []
    tmp_minus = []
    tmp_divid = []
    for i in range(len(values_t)):
        tmp_log.append(math.log2(values_t[i]))
        tmp_minus.append(values_t[i] - tmp_log[i])
        tmp_divid.append(values_t[i] / tmp_log[i])
    names.append('log')
    features.append(tmp_log)
    names.append('log_minus')
    features.append(tmp_minus)
    names.append('log_divid')
    features.append(tmp_divid)

    return names, features


def spectral_residual(values, win_size):

    def average_filter(a, n=3):
        res = np.cumsum(a, dtype=float)
        res[n:] = res[n:] - res[:-n]
        res[n:] = res[n:] / n
        for i in range(1, n):
            res[i] /= (i + 1)
        return res

    def backadd_new(a):
        backaddnum = 5
        kkk = (a[-2] - a[-5])/3
        kk = (a[-2] - a[-4])/2
        k = a[-2] - a[-3]
        kkkk = (a[-2] - a[-5])/4
        kkkkk = (a[-2] - a[-6])/5
        toadd = a[-5] + k + kk + kkk + kkkk + kkkkk
        a.append(toadd)
        for i in range(backaddnum-1):
            a.append(a[-1])
        return a

    length = len(values)

    detres = [None] * (win_size - 1)

    for pt in range(win_size - 1, length):

        head = pt + 1 - win_size

        tail = pt + 1

        wave = np.array(backadd_new(values[head:tail]))
        trans = np.fft.fft(wave)
        realnum = np.real(trans)
        comnum = np.imag(trans)
        mag = np.sqrt(realnum ** 2 + comnum ** 2)

        mag = np.clip(mag, ZERO, mag.max())

        spectral = np.exp(np.log(mag) - average_filter(np.log(mag)))
        trans.real = trans.real * spectral / mag
        trans.imag = trans.imag * spectral / mag
        wave = np.fft.ifft(trans)
        mag = np.sqrt(wave.real ** 2 + wave.imag ** 2)
        judgeavg = average_filter(mag, n=21)

        idx = win_size - 1
        safe_divisor = judgeavg[idx]
        if abs(safe_divisor) < ZERO:
            safe_divisor = ZERO
        detres.append(abs(mag[idx] - safe_divisor) / abs(safe_divisor))

    return ['sr'], [detres]


def get_features(timestamp, values, with_sr):
    """
    :param timestamp:
    :param values:
    :return: list of features
    """
    names = []
    features = []

    # MA and MA_Diff
    tmp_names, tmp_features = ma_diff_ratio_features(values, windows=[10, 50, 100, 500, 1440])
    names += tmp_names
    features += tmp_features

    # EWMA and EWMA_Diff
    tmp_names, tmp_features = ewma_diff_ratio_features(values, decays=[0.1, 0.5], windows=[10, 50, 100, 500, 1440])
    names += tmp_names
    features += tmp_features

    # window min, max, min_max_diff, std, quantile
    tmp_names, tmp_features = win_min_max_diff_std_quantile(values, windows=[10, 50, 200, 500, 1440])
    names += tmp_names
    features += tmp_features

    # diff and ratio of change
    tmp_names, tmp_features = diff_ratio(values)
    names += tmp_names
    features += tmp_features

    # log, minus, divid
    tmp_names, tmp_features = log_minus_divid(values)
    names += tmp_names
    features += tmp_features

    # sr
    if with_sr:
        tmp_names, tmp_features = spectral_residual(values, win_size=1440)
        names += tmp_names
        features += tmp_features

    # we add the mean diff ratio, fft and luminol as extra features but see no improvement.
    # median diff ratio
    # tmp_names, tmp_features = median_diff_ratio_features(values, windows=[10, 100])
    # names += tmp_names
    # features += tmp_features

    # # fft
    # tmp_names, tmp_features = fft_base_streaming(values, win=128)
    # names += tmp_names
    # features += tmp_features

    # # luminol
    # tmp_names, tmp_features = luminol_streaming(timestamp, values)
    # names += tmp_names
    # features += tmp_features

    # replace value None with 0 and normalize the features
    rs_fts = []
    for ft in features:
        rs_fts.append(mean_std_normalize(ft))

    return names, rs_fts

"""# **2. Thực thi**"""

train_data, train_label, test_data, test_labels = load_anomaly(path_split)

"""## **2.1. Train set**"""

train_score_uad = pro(pd.read_csv(path_uad + "train_score_uad.csv"))

for column in train_score_uad.columns:
  train_score_uad[column] = mean_std_normalize_df(train_score_uad[column])

pca = PCA(n_components=1)
normalized_data_train = (train_data - train_data.mean()) / train_data.std()
transformed_data_train = pca.fit_transform(normalized_data_train)

names_train, features_train = get_features(train_data.index, transformed_data_train, with_sr=False)

features_train = np.array(features_train).transpose()
features_train = pd.DataFrame(features_train, columns=names_train)

features_train['Date'] = train_score_uad.index
features_train.set_index('Date', inplace=True)

res_train = pd.concat([features_train, train_score_uad], axis=1)

"""## **2.2. Test set**"""

test_score_uad = pro(pd.read_csv(path_uad + "test_score_uad.csv"))

for column in test_score_uad.columns:
  test_score_uad[column] = mean_std_normalize_df(test_score_uad[column])

pca = PCA(n_components=1)
normalized_data_test = (test_data - test_data.mean()) / test_data.std()
transformed_data_test = pca.fit_transform(normalized_data_test)

names_test, features_test = get_features(test_data.index, transformed_data_test, with_sr=False)

features_test = np.array(features_test).transpose()
features_test = pd.DataFrame(features_test, columns=names_test)

features_test['Date'] = test_score_uad.index
features_test.set_index('Date', inplace=True)

res_test = pd.concat([features_test, test_score_uad], axis=1)

"""# **3. Save**"""

res_train.to_csv(path_feature + 'train_features.csv')
res_test.to_csv(path_feature + 'test_features.csv')

res_train

res_test