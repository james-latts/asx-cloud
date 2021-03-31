#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 18:00:47 2021

@author: james
"""

# Imports
import os
import numpy as np
import pandas as pd
import pickle as pk
import datetime as dt
from dateutil import parser

# Input argument
abs_path = "/home/james/Documents/finance/asx-cloud/data/abs"

# Check previous processing
processed_path = abs_path + "processed.csv"
if os.path.exists(processed_path):
  old_processed_df = pd.read_csv(processed_path)
else:
  old_processed_df = pd.DataFrame()

# Read in raw files
data_path = abs_path + "/raw"
data_list = []
for file_name in os.listdir(data_path):
  file_path = data_path + "/" + file_name
  with open(file_path, "r") as data_file:
    data_list.append(np.array([[value.strip() for value in line.split(",")] for line in data_file.readlines()]))

# Process the raw data
pro_list = []
for lines in data_list:
  lines[0,0] = "Name Location"
  index = np.where(lines[:, 0] == "Series ID")[0][0]
  info_lines = lines[:index+1]
  date_lines = lines[index+2:, 0]
  data_lines = lines[index+2:, 1:]
  
  info = pd.DataFrame(info_lines.T[1:], columns=info_lines[:, 0].tolist())
  column_names = info.columns.values.tolist()
  column_names.remove("Series ID")
  column_names = ["Series ID", "Name", "Location"] + column_names
  column_names.remove("Name Location")
  info["Name"] = info["Name Location"].apply(lambda x: x.split(";")[0].strip())
  info["Location"] = info["Name Location"].apply(lambda x: x.split(";")[1].strip())
  info = info[column_names]
  info["Processing Date"] = dt.datetime.now()
  
  pro_list.append((info, date_lines, data_lines))

# Check if any of the raw data has already been processed
new_processed_df = pd.concat([x[0] for x in pro_list])
new_data = []
for old_series in old_processed_df.iterrows():
  series_id = old_series[1]["Series ID"]
  matches = new_processed_df[new_processed_df["Series ID"] == series_id]
  matches["Series End Date"] = matches["Series End"].apply(lambda x: parser.parse(x))
  match = matches.nlargest(1, "Series End Date")
  for new_series in match.iterrows():
    old_date = parser.parse(old_series[1]["Series End"])
    new_date = parser.parse(new_series[1]["Series End"])
    if new_date - old_date >= dt.timedelta(0):
      new_data.append(new_series[0])
  
# Adjust to only new_data
new_processed_df = new_processed_df.loc[new_data]



class AbsDataset:
  """ ABS Dataset """
  def __init__(self, data):
    info, date, data = data
    
    self.info = info
    self.date = date
    self.data = data
    
    for key in info:
      setattr(self, key.lower().replace(" ", "_"), info[key])
    
    self.formatted_date = [parser.parse(x) for x in self.date]
    self.formatted_name = self.name.lower().replace(" ", "_")
    
    self.df = pd.DataFrame(self.data)
    self.df.index = self.formatted_date
    self.df.columns = [self.series_id]
    self.df.index.name = "date"
    
    self.processing_date = dt.datetime.now()

for x in range(len(info)):
  data = (dict(info.iloc[x]), date_lines, data_lines[:, x])
  data_class = AbsDataset(data)
  