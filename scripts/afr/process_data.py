#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 11 13:50:15 2021

@author: james
"""

# Imports
import os
import json
import pandas as pd
import multiprocessing as mp
from datetime import datetime as dt


#### SUPPORT FUNCTIONS ####
def read_json(file_path):
  """  """
  
  #
  with open(file_path, "r") as f:
    file_data = json.load(f)
  return file_data
  
def read_raw_data(data_dir):
  """  """
  
  #
  file_paths = [data_dir + "/" + x for x in os.listdir(data_dir)]
  with mp.Pool() as pool:
    data = pool.map(read_json, file_paths)
  data_dict = {}
  for d in data:
    data_dict[d["url_id"]] = d
  data_df = pd.DataFrame.from_dict(data_dict, orient="index")
  return data_df

def write_article_data(data_dir, article_data):
  """  """
  
  # 
  try:
    url_id = article_data["url_id"]
    data_location = data_dir + "/" + url_id + ".json"
    with open(data_location, "w") as f:
      json.dump(article_data, f)
    return 1
  except:
    return 0
  

#### MAIN ####
#
raw_dir = "/home/james/Documents/finance/asx-cloud/data/afr/raw"
data_df = read_raw_data(raw_dir)

#
data_df["original_date"] = data_df["date"]
data_df["date"] = data_df["date"].apply(lambda x: dt.strptime(" ".join(x.split(" ")[0:3]), "%b %d, %Y").date())
data_df["year"] = data_df["date"].apply(lambda x: x.year)
data_df["month"] = data_df["date"].apply(lambda x: x.month)
data_df["file_path"] = data_df["year"].astype(str) + "/" + data_df["month"].astype(str) + ".csv"

#
pro_dir = "/home/james/Documents/finance/asx-cloud/data/afr/processed"
years = data_df["year"].value_counts().index.values
for year in years:
  if str(year) not in os.listdir(pro_dir):
    os.mkdir(pro_dir + "/" + str(year))
    
#
data_df = data_df.sort_values("date", ascending=False)
data_df["date"] = data_df["date"].apply(str)
data_df = data_df.drop(["year", "month"], axis = 1)
schema = ["url_id", "url", "date", "title", "summary", "text", "extraction_date", "original_date", "topic", "author", "file_path"]
data_df = data_df[schema]

#
files = data_df["file_path"].value_counts().index.values
for file in files:
  file_df = data_df[data_df["file_path"] == file]
  file_df = file_df.drop("file_path", axis = 1)
  file_df.to_csv(pro_dir + "/" + file, index = False)
  
