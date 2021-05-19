#### IMPORTS ####
# System
import os
import multiprocessing as mp

# Local
None

# External
import numpy as np
import pandas as pd
from flair.data import Sentence
from flair.embeddings import SentenceTransformerDocumentEmbeddings


#### SUPPORT FUNCTIONS ####
def read_csv_pandas(file_path):
  """ Read in csv at file path as pandas dataframe """
  
  # Try read in file at file path
  try:
    file_data = pd.read_csv(file_path)
  except:
    print("ERROR: failed to read in file at: {}".format(file_path))
    file_data = pd.DataFrame()
  return file_data

def read_pro_data(data_dir):
  """ Read in processed csv data into pandas dataframe """
  
  # Init data, get file paths then load using mp
  file_paths = []
  for folder_name in os.listdir(data_dir):
    folder_dir = data_dir + "/" + folder_name
    file_paths += [folder_dir + "/" + x for x in os.listdir(folder_dir)]
  with mp.Pool() as pool:
    data = pool.map(read_csv_pandas, file_paths)
  data_df = pd.concat(data)
  data_df = data_df.reset_index(drop=True)
  return data_df


#### MAIN ####
# Load in processed afr data
afr_pro_dir = "/home/james/Documents/finance/asx-cloud/data/afr/btb/processed"
data_df = read_pro_data(afr_pro_dir)
data_df = data_df[["date", "title", "summary"]]

#
#location = "/home/james/Documents/finance/asx-cloud/data/cfd/processed/testing.csv"
#testing_df = pd.read_csv(location)
#testing_df = data_df[data_df["date"] == "2021/04/18"]
#data_df = data_df[data_df["date"] != "2021/04/18"]

# Get embeddings
TOKENIZERS_PARALLELISM=False
#embedding = SentenceTransformerDocumentEmbeddings("distilbert-base-uncased")
embedding = SentenceTransformerDocumentEmbeddings("bert-base-nli-mean-tokens")
data_df["summary_encoding"] = data_df["summary"].apply(Sentence)
data_df["summary_encoding"].apply(embedding.embed)
data_df["title_encoding"] = data_df["title"].apply(Sentence)
data_df["title_encoding"].apply(embedding.embed)

# Create the feature dataframe
data_df["encoding"] = data_df["summary_encoding"].apply(lambda x: x.embedding.cpu().numpy())
feature_df = pd.DataFrame(data_df["encoding"].tolist(), index=data_df.index, columns=["feature_" + str(x+1) for x in range(768)])
feature_df["date"] = data_df["date"]
feature_columns = feature_df.columns.values.tolist()
feature_columns.remove("date")

#
asx_pro_dir = "/home/james/Documents/finance/asx-cloud/data/asx/processed"
target_df = read_pro_data(asx_pro_dir)
#location = "/home/james/Documents/finance/asx-cloud/data/cfd/processed/testing.csv"
#target_df = pd.read_csv(location)

#
target_df["target"] = target_df["close"] - target_df["open"] > 0
target_df["target"] = target_df["low"] = target_df["open"]
target_df["target"] = target_df["target"].astype(np.int)
#target_df["target"] = (target_df["close"] - target_df["open"]) / target_df["open"]
target_df = target_df[["date", "target"]]

#
from sklearn.model_selection import RepeatedKFold
from sklearn.ensemble import RandomForestClassifier

#
model_df = pd.merge(feature_df, target_df, on="date")
X = model_df[feature_columns].values
y = model_df["target"].values
clf = RandomForestClassifier(n_estimators=100, max_depth=16, class_weight="balanced", random_state=11)

#
result = []
results = []
n_splits = 5
n_repeats = 5
rkf = RepeatedKFold(n_splits, n_repeats, 11)
for i, indices in enumerate(rkf.split(X)):
  if i % n_splits == 0 and result:
    results += [result]
    result = []
  train_index, test_index = indices
  X_train, X_test = X[train_index], X[test_index]
  y_train, y_test = y[train_index], y[test_index]
  clf.fit(X_train, y_train)
  y_pred = clf.predict(X_test)
  correct = sum(y_pred == y_test)
  total = len(y_test)
  result += [correct/total]

#
#testing_df = pd.merge(testing_df, feature_df)
#testing_df["target"] = testing_df["close"] - testing_df["open"] > 0
#testing_df["target"] = testing_df["target"].astype(np.int)
test_X = testing_df[feature_columns].values
test_y = testing_df["target"].values
#testing_df["summary_encoding"] = testing_df["summary"].apply(Sentence)
#testing_df["summary_encoding"].apply(embedding.embed)
#testing_df["title_encoding"] = testing_df["title"].apply(Sentence)
#testing_df["title_encoding"].apply(embedding.embed)
#testing_df["encoding"] = testing_df["summary_encoding"].apply(lambda x: x.embedding.cpu().numpy())
#testing_feature_df = pd.DataFrame(testing_df["encoding"].tolist(), index=testing_df.index, columns=["feature_" + str(x+1) for x in range(768)])
y_pred = clf.predict(test_X)
