# -*- coding: utf-8 -*-
"""[DS200] MAIN.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1zh2R28FFxpprRyXpQ1zKW9xuylZCdAWo

Link TỔNG HỢP: https://drive.google.com/drive/folders/1_AXzPfVs9Ul1dyasf7VVb7mQWEQS21GH?usp=drive_link

# **1. Library**
"""

!pip install snorkel

from google.colab import drive
drive.mount('/content/drive')

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

import torch
import copy
import random

import lightgbm as lgb
import warnings
warnings.filterwarnings("ignore")

"""# **Paths**"""

path = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/'
path_split = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Split/'
path_repr = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Representation/'
path_uad = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/UAD/'
path_feature = '/content/drive/MyDrive/Học kì 6/DS200/Dataset/Feature/'

"""# **2. Dataset**

- Tải dữ liệu từ: https://finance.yahoo.com/quote/AAPL/history?period1=1104537600&period2=1689465600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
 + Dữ liệu ban đầu: 21792 rows × 6 columns
- Code phân tích, tiền xử lý, điền khuyết: https://colab.research.google.com/drive/1RMtoTQs8yhZfTfkqwUqeIcP3OT0pSPZq?usp=drive_link
 + Xử lý giá trị khuyết: Dựa trên kết quả dữ liệu ngày: https://colab.research.google.com/drive/1YbtOEa8evpQJMKkODaxi5RSXTc4QxX7N?usp=sharing
 + Kết quả dữ liệu sau tiền xử lý : 24000 x 6  (dữ liệu trong 5 tuần)

## 2.1. **Create Anomaly (Sử dụng trong quá trình Human Labeling)**

Không chạy lại
"""

'''
Tạo Anomaly: (Thay thế human trong phần dưới)
 - Dựa trên cơ sở thống kê phổ biến để xác định các giá trị bất thường dựa trên giá trị
trung bình và độ lệch chuẩn của một phân phối dữ liệu. Cụ thể, đây là một phương pháp
sử dụng phân phối chuẩn (normal distribution) hoặc phân phối Gaussian.
  => Tính toán giá trị trung bình và độ lệch chuẩn cho dữ liệu 10 ngày gần nhất,
  sau đó so sánh giá trị của mỗi cột với giá trị trung bình và độ lệch chuẩn đó
   => Số điểm bất thường được ghi nhận: 4381

Từ file Anomaly này sẽ tiến hành chia tập 6:4
'''

'''
Bước thực hiện tạo anomaly:
 - Xác định số ngày trong khoảng thời gian nhất định (10 ngày gần nhất)
 - Xác định ngưỡng cho giao dịch bất thường (lớn hơn 2 độ lệch chuẩn)
 - Xác định các cột dữ liệu cần xét
 - Tạo cột nhãn mới để lưu trữ kết quả gán nhãn
 - Tạo các cột mới để lưu trữ kết quả (giá trị trung bình, phương sai (độ lệch chuẩn))
 - Xác định giao dịch bất thường: Vượt qua ngưỡng bất thường trong tất cả các cột để gán nhãn là điểm bất thường
 - Lưu dữ liệu đã gán nhãn vào file CSV
'''

data = pd.read_csv(path+'Data_processed.csv')
data['Date'] = pd.to_datetime(data['Date'])
data = data.set_index('Date')

time_window = 5*16*60 # 1tuan
threshold = 3
columns_to_check = ['Open', 'High', 'Low', 'Close', 'Volume']

df = data[columns_to_check].copy()
df['Anomaly'] = 0

for column in columns_to_check:
    df[column + '_mean'] = df[column].rolling(window=time_window, min_periods=1).mean()
    df[column + '_std'] = df[column].rolling(window=time_window, min_periods=1).std()

for index, row in df.iterrows():
    for column in columns_to_check:
        column_mean = row[column + '_mean']
        column_std = row[column + '_std']
        value = row[column]

        if value > column_mean + (threshold * column_std) or value < column_mean - (threshold * column_std):
            df.at[index, 'Anomaly'] = 1

columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Anomaly']

data_anomaly = df[columns].copy()
#data_anomaly.to_csv(path+'Data_Anomaly.csv')

print("Số điểm bất thường được ghi nhận:", data_anomaly['Anomaly'].sum())
print("Tỷ lệ số điểm bất thường trong dữ liệu:", (data_anomaly['Anomaly'].sum() / len(data_anomaly))*100)

"""## 2.2. **Chia train:test (6:4)**

Không chạy lại
"""

# Split
def split_data(data):
  X = data.drop('Anomaly', axis=1)
  y = data['Anomaly']
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42, stratify=y)
  return X_train, X_test, y_train, y_test

"""train_data, test_data, train_labels, test_labels = split_data(data_anomaly)
# Save
train_data.to_csv(path_split+"train_data.csv")
train_labels.to_csv(path_split+"train_labels.csv", index = False)
test_data.to_csv(path_split+"test_data.csv")
test_labels.to_csv(path_split+"test_labels.csv", index = False)"""

"""# **3. LEIAD**

## 3.1. **Các Class**

### 3.1.1. Weak Supervision Module
"""

class WeakSupervisionModel():
    def __init__(self):
            from snorkel.labeling.model import LabelModel
            self.model = LabelModel(cardinality=2, verbose=False)

    def fit(self, LFs, alpha=0.01, seed=123):
            LFs = np.array(LFs)
            # Snorkel
            self.model.fit(
                L_train=np.array(LFs),
                class_balance=np.array([1-alpha, alpha]),
                lr=0.001,
                n_epochs=200,
                seed=seed,
                optimizer = 'adam',
                progress_bar = False
            )
    def pred_pos(self, LFs):
        ''' Dự đoán xác suất thuộc lớp dương (positive class) cho các mẫu dữ liệu trong tập LFs '''
        LFs_np = np.array(LFs)
        pred = self.model.predict_proba(LFs_np) # xác xuất neg sẽ ở đầu, và pos ở sau
        proba = pred[:, 1] # chọn cột thứ 2 trong mảng (cột pos)
        proba = np.array(pd.DataFrame(proba).fillna(0))
        return pd.DataFrame(proba, index=LFs.index, columns=['score'])

    def gen_fake_label(self, proba_pos, proba_neg, thres_pos, thres_neg):
        fake_label = proba_pos.copy()
        fake_label.columns = ['Label']
        fake_label[:] = -1
        fake_label[proba_pos.score >= thres_pos] = 1
        fake_label[proba_neg.score <= thres_neg] = 0

        return fake_label

    def thres_adaptive_decend(self, num_LF, score):
        thres = 99.7
        return np.percentile(score, thres)

"""### 3.1.2. End Model"""

class LightGBM():
    def __init__(self, **params):
        self.param = {
            'num_leaves': 200,
            'objective': 'binary',
            # 'metric':'average_precision',
            'verbosity': -1,
            'is_unbalance': True,
            # 'device': 'gpu'
        }
        self.param.update(params)

    def train(self, data, label, weight=None):
        data = np.array(data)
        label = np.array(label)

        X_train, X_test, y_train, y_test = train_test_split(
        data, label, test_size = 0.2, random_state = 0)

        train_data = lgb.Dataset(X_train, label=y_train)
        valid_data = lgb.Dataset(X_test, label=y_test)

        train_data = lgb.Dataset(data, label=label, weight=weight) # sử dụng trọng số để cân =
        # print(self.param)
        self.model = lgb.train(
            self.param,
            train_data,
            valid_sets=[valid_data],
            early_stopping_rounds=50,
            verbose_eval=False
        )

    def predict(self, data):
        '''
        Giá trị dự đoán này thường là các số thực trong khoảng từ 0 đến 1,
        biểu thị xác suất mẫu thuộc vào lớp tích cực (positive class) của mô hình.
        '''
        data = np.array(data)
        pred = self.model.predict(data)

        return pred

"""### 3.1.3. Active Learning"""

class ActiveLearning():
    '''
    Học chủ động là một phương pháp trong đó một hệ thống tự động chọn các điểm dữ liệu
    cần được người dùng đánh nhãn để cải thiện mô hình.
    => thực hiện học chủ động và chọn các điểm dữ liệu cần được đánh nhãn tiếp theo.
    (Tính toán các giá trị không chắc chắn dựa trên các thông số đầu vào)

    '''
    def __init__(self, anomaly_prob_init):
        self.index_list = anomaly_prob_init.index.tolist()
        # init uncerntainty: các giá trị không chắc chắn của dữ liệu
        self.uncertainty = pd.concat([anomaly_prob_init for _ in range(5)], axis=1)
        self.uncertainty[:] = 0
        self.uncertainty.columns = ["lf_dis", "lf_abs", "uncertainty_s", "diversity", "anomaly_prob_init"]
        # Anomaly probability of UADs
        self.uncertainty['anomaly_prob_init'] = anomaly_prob_init
        # Edit here if you want to remove some metrics [1, 0.5, 0.5, 1, 0.2]
        self.weight = pd.DataFrame(pd.Series([1, 0.5, 0.5, 1, 0.2], index=self.uncertainty.columns, name=0))

    # Tính toán các giá trị không chắc chắn dựa trên các thông số đầu vào
    def cal_uncertainty(self, dnn_pred, LFs, ts2vec, golden_labels):
        # dnn_pred = limit_min_max_normalize(dnn_pred, limit=10)
        pred_1 = LFs.replace(-1,0).sum(axis=1)
        pred_0 = LFs.replace(1,-1).replace(0,1).replace(-1,0).sum(axis=1)
        pred_anomaly = pred_1/(pred_1+pred_0)
        pred_anomaly = pred_anomaly.fillna(0)
        # Agreement of labeling functions
        self.uncertainty["lf_dis"] = -np.array(pred_anomaly * np.log(pred_anomaly + 1e-20) + (1 - pred_anomaly) * np.log(1 - pred_anomaly + 1e-20))
        # Hit time with labeling functions
        self.uncertainty["lf_abs"] = np.log(-LFs.where(LFs == -1, other = 0).sum(axis=1) + 1)
        # Uncertainty of the supervised end model
        self.uncertainty["uncertainty_s"] = -np.array(dnn_pred * np.log(dnn_pred + 1e-20) + (1 - dnn_pred) * np.log(1 - dnn_pred + 1e-20))
        # Diversity
        self.uncertainty["diversity"] = ts2vec.cal_diversity(golden_labels)

    def get_next_sample(self, golden_labels, ts_num=1, len_padding=None):

        uncertainty_norm = min_max_normalize(self.uncertainty)
        uncertainty_weighted = uncertainty_norm.dot(self.weight)

        uncertainty_weighted_sort = uncertainty_weighted.loc[golden_labels.Label == -1]
        uncertainty_weighted_sort = uncertainty_weighted_sort.sort_values(by=0, ascending=False)



        top_k = uncertainty_weighted_sort.index[:ts_num].tolist()
        # print(top_k)
        return top_k

    def get_next_sample_random(self, golden_labels, ts_num=1, len_padding=None, empty_keys = None):

        random.shuffle(self.index_list)
        top_k = self.index_list[:ts_num]
        return top_k

"""### 3.1.4. Labeling Function Generation"""

class LFGenerator():
    '''
    Được sử dụng để tạo các Labeling Functions (LFs)
    dựa trên mô hình được chọn, có thể là ts2vec hoặc dnn_features.
    all_reper: biến để lưu trữ các biểu diễn (representations) của tập dữ liệu huấn luyện (kiểu các feature đầu vào)
    '''
    def __init__(self, model='ts2vec'):
        # load all repr
        self.model = model

        if model=='ts2vec':
            self.all_repr = load_reper(path_repr)
            l2 = np.sqrt((self.all_repr**2).sum(axis=1))
            self.all_repr_norm = self.all_repr.div(l2, axis=0)
            self.thr = 8

        elif model=='dnn_features':
            self.all_repr = load_feature(path_feature)
            self.thr = 8

    def predict(self, sample_index):
        sample_repr = self.all_repr.iloc[sample_index]

        if self.model=='dnn_features':
            # inner product
            distance = np.dot(self.all_repr, sample_repr)
            pred = self.gen_lf(distance)

        elif self.model=='ts2vec':
            # L1 Distance with norm
            sample_repr_norm = np.divide(sample_repr, np.sqrt((sample_repr**2).sum()))
            distance = np.abs(self.all_repr_norm - sample_repr).sum(axis=1)
            pred = self.gen_lf(distance)

        return pred

    def get_repr(self, index):
        return pd.DataFrame(self.all_repr).iloc[index]

    def gen_lf(self, distance):
        thr = np.mean(distance) - self.thr * np.std(distance)
        pred = distance < thr

        pred_int = np.zeros_like(pred).astype(int)
        pred_int[pred==True] = 1
        pred_int[pred==False] = -1
        return pred_int

    def cal_diversity(self, golden_labels):
        num_golden_labels = (golden_labels != -1).sum().item()
        if num_golden_labels:
            golden_labels_sort = golden_labels.sort_values(by='Label', ascending=False)
            index_list = golden_labels_sort[:num_golden_labels].index
            labeled_repr = self.all_repr.loc[index_list]

            # diversity calculation controled under 1 min
            if num_golden_labels > 1000:
                labeled_repr = labeled_repr.sample(1000)

            # use pytorch to accelerate
            all_repr = torch.from_numpy(np.array(self.all_repr)).to(torch.float)
            labeled_repr = torch.from_numpy(np.array(labeled_repr).transpose()).to(torch.float)
            diversity = 1-torch.mm(all_repr, labeled_repr).sum(axis=1)/num_golden_labels
            diversity = np.array(diversity)
        else:
            diversity = np.zeros_like(golden_labels)
        return diversity

"""## 3.2.**Các thông số và hàm lẻ**"""

seed = 123

use_uad = True
use_t2v = True
use_stat = True

ABSTAIN = -1
n_iter = 0
n_interact = 0

# Hyper-parameters
WS_RATIO = 0.1
LEN_PADDING = 50 #200
ANOMALY_PERCENTAGE = 1
ZERO = 0.00000001
UAD_POS_THR = 0.8
UAD_NEG_THR = 0.2
N_A_RATIO = 100/ANOMALY_PERCENTAGE
NORMAL_PERCENTAGE = 100 - ANOMALY_PERCENTAGE

def load_data(path_split):
    train_data = pd.read_csv(path_split + "train_data.csv")
    train_labels = pd.read_csv(path_split + "train_labels.csv")
    return train_data, train_labels

def load_uad_score(path_uad):
    uad_score = pd.read_csv(path_uad + "train_score_uad.csv")
    return uad_score

def load_feature(path_feature):
   train_features = pd.read_csv(path_feature + "train_features.csv")
   train_features = train_features.drop(columns='Date')
   object_cols = train_features.select_dtypes(include='object').columns
   for col in object_cols:
    train_features[col] = train_features[col].str.replace('[','').str.replace(']','')
    train_features[col] = pd.to_numeric(train_features[col], errors='coerce')
   return train_features

def load_reper(path_repr):
    train_repr = pd.read_csv(path_repr + "train_repr.csv")
    train_repr = train_repr.drop(columns=	'Unnamed: 0')
    return train_repr

def min_max_normalize(data):
    min_ = data.min()
    max_ = data.max()
    data = (data - min_) / (max_ - min_ + ZERO)
    return data

def adjust_fake_label(fake_label, golden_labels):
    fake_label.loc[golden_labels['Label'] == 1] = 1
    fake_label.loc[golden_labels['Label'] == 0] = 0
    return fake_label

def fake_labels(LFs, ws_score, ws_model, index_pd, golden_labels, n_iter, N_SAMPLE, ANOMALY_PERCENTAGE, WS_RATIO, N_A_RATIO):
    num_ano_gol = (golden_labels.Label==1).sum()

    if mv_flag == False:
        pos_thres = max(num_ano_gol * 50 / N_SAMPLE, ANOMALY_PERCENTAGE * WS_RATIO)

        # ws_thres_pos và ws_thres_neg được tính toán dựa trên weak supervision model.
        ws_thres_pos = np.percentile(ws_score, 100 - pos_thres)
        ws_thres_neg = np.percentile(ws_score, pos_thres * N_A_RATIO)
        if ws_thres_pos == 1 or ws_thres_neg == 0:
            fake_labels.mv_flag = True
        fake_label = ws_model.gen_fake_label(proba_pos=ws_score, proba_neg=ws_score, thres_pos=ws_thres_pos, thres_neg=ws_thres_neg)
    else:
        num_ano_ws = int(max(2 * num_ano_gol, N_SAMPLE * ANOMALY_PERCENTAGE * WS_RATIO / 100))
        num_nor_ws = int(num_ano_ws * N_A_RATIO)
        sorted_pd = pd.DataFrame((LFs == 1).sum(axis=1) - (LFs == 0).sum(axis=1)).sort_values(by=0)
        ano_id = sorted_pd[-num_ano_ws:].index
        normal_id = sorted_pd[:num_nor_ws].index
        fake_label = index_pd.copy()
        fake_label.loc[ano_id] = 1
        fake_label.loc[normal_id] = 0

    fake_label = adjust_fake_label(fake_label, golden_labels)

    return fake_label

def limit_min_max_normalize(data, limit):
    min_ = np.percentile(data, limit)
    max_ = np.percentile(data, 100-limit)
    data[data>max_] = max_
    data[data<min_] = min_
    data = (data - min_) / (max_ - min_ + ZERO)
    return data

"""## 3.3.**Unsupervised Anomaly Detector (UAD)**

Xây dựng Label Functions (LFs): Sử dụng các phương pháp phát hiện lỗi không giám sát (i-Forest, SR, LOF, RC-Forest và Luminol) để tạo thành các Label Function ban đầu. - Các score predict

Được thực hiện trong file: https://colab.research.google.com/drive/193wgseqMNENb7bmlxLDElgwSVIKLCJ8j?usp=drive_link
"""

train_data, train_labels = load_data(path_split)
uad_scores = load_uad_score(path_uad)

# Phục vụ trong quá trình lặp
train_features = load_feature(path_feature)

uad_score = uad_scores.drop(columns='Date').copy()
anomaly_prob_init = uad_score.sum(axis=1)*0.2 #Tổng của các điểm dữ liệu từ các mô hình UAD và nhân với 0.2.

uad_labels = uad_score.copy()
uad_labels[:] = -1

# Labeling Function này được sử dụng dựa trên luật về ngưỡng (Label ban đầu)
for method in uad_score.columns:
    uad_score[method] = min_max_normalize(uad_score[method])
uad_labels[uad_score > UAD_POS_THR] = 1
uad_labels[uad_score < UAD_NEG_THR] = 0
LFs = uad_labels.copy().astype(int)
# Tính toán nhãn ts2vec dưới dạng UAD - Chưa biết tại sao phải làm vây => chưa thực hiện

anomaly_prob_init

uad_score

index_pd = pd.DataFrame({'Label': [-1] * len(train_data)})
golden_labels = index_pd.copy()

"""## 3.4. **Thực thi**

### 3.4.1 Hàm
"""

active_learning = ActiveLearning(anomaly_prob_init)
ts2vec = LFGenerator()
statistic_model = LFGenerator(model='dnn_features')

N_SAMPLE = len(train_data)

def loop(LFs, count_no_false_prediction, ts_num = 5, c=0):
  global mv_flag, n_iter, n_interact
  print()
  print(" *******", n_iter, "th iteration ******* ")
  print("-----WEAK SUPERVISION-----")
  # Xây dựng mô hình học giám sát yếu: Chọn một trong các mô hình học giám sát yếu như Snorkel
  # để kết hợp thông tin từ các LFs (các score) ban đầu và tạo ra các nhãn yếu cho tập dữ liệu huấn luyện.
  ws_model = WeakSupervisionModel()
  ws_model.fit(LFs, 0.01, seed=123)
  ws_score = ws_model.pred_pos(LFs)
  fake_label = fake_labels(LFs, ws_score, ws_model, index_pd, golden_labels, n_iter, N_SAMPLE, ANOMALY_PERCENTAGE, WS_RATIO, N_A_RATIO)

  # print('-end model-')
  # Xây dựng mô hình cuối cùng: Chọn mô hình phân loại nhị phân LightGBM
  # để huấn luyện trên tập dữ liệu đã được nhãn yếu bởi mô hình học giám sát yếu.
  end_model = LightGBM()
  end_model_train_label=fake_label[fake_label.Label!=-1] # lấy 2 nhãn 0 và 1 để train model còn các sample còn lại chưa gán
  end_model_train_label_index=end_model_train_label.index
  end_model.train(train_features.loc[end_model_train_label_index], end_model_train_label)
  end_model_pred = end_model.predict(train_features) #Dự đoán trên toàn bộ tập dữ liệu =>
  end_model_pred_label = index_pd.copy()
  end_model_pred_label[:] = end_model_pred > np.percentile(end_model_pred, NORMAL_PERCENTAGE)

  print()
  print("-----ACTIVE LEARNING-----")
  # calculate uncertainty # Tính toán các giá trị ko chắc chắn
  active_learning.cal_uncertainty(end_model_pred, LFs, ts2vec, golden_labels)
  top_k = active_learning.get_next_sample(golden_labels, ts_num = ts_num, len_padding=LEN_PADDING)
  print("Top", ts_num, top_k)

  print()
  print("-----HUMAN LABELING-----")
  for sample_index in top_k:
    sample_key = pd.DataFrame(train_data.drop(columns='Date').iloc[sample_index]).T.index  # Lấy điểm dữ liệu tại vị trí sample_index trong dataframe train_data
    l = max(0, sample_index - LEN_PADDING)
    r = min(len(train_data), sample_index + LEN_PADDING)
    sample_arr = train_data.iloc[l:r].drop(columns='Date')  # Lấy các điểm dữ liệu trong khoảng từ l đến r trong dataframe train_data
    sample_labels = train_labels.iloc[l:r]  # Lấy các nhãn tương ứng từ l đến r trong dataframe train_labels
    golden_labels.iloc[l:r] = sample_labels
    n_interact += 1
    num_anomaly = sample_labels.sum().item()  # Đếm số lượng điểm dữ liệu có nhãn là bất thường trong cửa sổ trượt.
    if num_anomaly == 0:
        print("No anomaly is annotated in index", sample_index)
        c +=1
    else:
        print(num_anomaly, "anomalies are labeled in index", sample_index)
        c=0  # Đặt lại biến đếm khi có nhãn sai được tìm thấy
        count_no_false_prediction = 0
        break

    if c==5:
      count_no_false_prediction += 1


  n_iter += 1

  print()
  print("-----GOLDEN LABEL MODEL (ÁP DỤNG END MODEL VÀO DỮ LIỆU CÓ NHÃN)-----")
  only_end_model_train_label=golden_labels[golden_labels.Label!=-1]
  num_labeled_anomaly = int((only_end_model_train_label==1).sum())
  print("Up to now labeled",num_labeled_anomaly)
  only_end_model_train_data=train_features.loc[only_end_model_train_label.index]
  only_end_model = LightGBM()
  only_end_model.train(np.array(only_end_model_train_data), np.array(only_end_model_train_label))
  only_end_model_pred = only_end_model.predict(train_features)
  only_end_model_pred_label = index_pd.copy()
  only_end_model_pred_label[:] = only_end_model_pred >= np.percentile(only_end_model_pred, NORMAL_PERCENTAGE)

  # adding golden end model label to LF on intervals
  if n_iter % 5==0: # đã qua 5 vòng lặp
        end_pred = index_pd.copy()
        end_pred[:] = only_end_model_pred
        pos_thr = np.percentile(end_pred, 100 - ANOMALY_PERCENTAGE/2)
        neg_thr = np.percentile(end_pred, NORMAL_PERCENTAGE/2)
        pred = index_pd.copy()
        pred[end_pred >= pos_thr] = 1
        pred[end_pred <= neg_thr] = 0
        pred.columns = [f'End_{n_iter}']
        # print_p_r(pred, train_label_pd)
        pred = pred.astype(int)
        LFs = pd.concat([LFs, pred], axis=1)

  print()
  print("-----LF GENERATOR-----")
  anomaly_index = (np.where(sample_labels==1)[0] + sample_labels.index[0]).tolist()
  not_anomaly_index = (np.where(sample_labels==0)[0] + sample_labels.index[0]).tolist()
  if anomaly_index != []:
        # truncate, to save time
        if len(anomaly_index) > 100:
            anomaly_index = random.sample(anomaly_index, 100)

        # run ts2vec
        if use_t2v: # chạy mô hình ts2vec để dự đoán nhãn cho các mẫu bất thường - kết quả là 0 hoặc 1
            pred_dict_t2v = {}
            for index in anomaly_index:
                t2v_pred = ts2vec.predict(index)
                t2v_pred = t2v_pred.astype(int)
                pred_dict_t2v[index] = t2v_pred
            if len(pred_dict_t2v) == 0:
                pred = copy.deepcopy(index_pd)
                pred[:] = ABSTAIN
            else:
                pred_sum = pd.DataFrame.from_dict(pred_dict_t2v)
                pred_sum = pred_sum.sum(axis=1)

                pred = pred_sum > -len(pred_dict_t2v)
                pred_int = np.zeros_like(pred).astype(int)
                pred_int[pred==True] = 1
                pred_int[pred==False] = ABSTAIN
                pred = index_pd.copy()
                pred[:] = pred_int[:,np.newaxis]

            pred.iloc[anomaly_index] = 1
            pred.iloc[not_anomaly_index] = 0

            print("Ts2vec labeled",(pred.Label==1).sum() , "anomaly.")
            # print_p_r(pred, train_label_pd)

            pred.columns = [f't2v_{n_iter}']
            pred = pred.astype(int)
            LFs = pd.concat([LFs, pred], axis=1)

        # run statistic model
        if use_stat:
            pred_dict_stat = {}
            for index in anomaly_index:
                # print(sample_key, index)
                static_pred = statistic_model.predict(index)
                static_pred = static_pred.astype(int)
                pred_dict_stat[index] = static_pred
            if len(pred_dict_stat) == 0:
                pred = copy.deepcopy(index_pd)
                pred[:] = ABSTAIN
            else:
                pred_sum = pd.DataFrame.from_dict(pred_dict_stat)
                pred_sum = pred_sum.sum(axis=1)
                pred = pred_sum > -len(pred_dict_stat)
                pred_int = np.zeros_like(pred).astype(int)
                pred_int[pred==True] = 1
                pred_int[pred==False] = ABSTAIN
                pred = index_pd.copy()
                pred[:] = pred_int[:,np.newaxis]
            pred.iloc[anomaly_index] = 1
            pred.iloc[not_anomaly_index] = 0
            print("Statistic model labeled",(pred.Label==1).sum() , "anomaly")
            # print_p_r(pred, train_label_pd)

            # Denoise
            if not use_uad:
                ws_filter = ws_score>ws_score.mean()
                filter_percent = ((ws_filter).sum().item()/len(ws_filter))*100
                filtered_num = len(pred.loc[(pred.label==1) & (ws_filter.score==0)])
                pred.loc[(pred.label==1) & (ws_filter.score==0)] = -1
                print('Weak model filted:', filtered_num, 'anomaly not in top', round(filter_percent,2), '%')
            # print_p_r(pred, train_label_pd)
            pred.columns = [f'stat_{n_iter}']
            pred = pred.astype(int)
            LFs = pd.concat([LFs, pred], axis=1)
  print()
  return LFs, only_end_model, count_no_false_prediction

"""### 3.4.2 Main"""

import joblib

mv_flag = False
N_SAMPLE = len(train_data)
count_no_false_prediction = 0  # Biến đếm số lần liên tiếp không tìm thấy nhãn sai
num_epoch = 150  # Số lần lặp
n_iter = 0
n_interact = 0


for i in range(num_epoch):
    LFs, end_model, count_no_false_prediction = loop(LFs,count_no_false_prediction)
    if count_no_false_prediction >= 2:
        break

"""# Lưu mô hình cuối cùng
with open(path+'LightGBM.pkl', 'wb') as f:
        joblib.dump(end_model, f)
"""