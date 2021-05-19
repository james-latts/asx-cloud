#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 18:31:45 2021

@author: james
"""


import json
import requests
import pandas as pd
from dateutil import parser

session = requests.Session()
base_url = "https://demo-api.ig.com/gateway/deal"


session_url = base_url + "/session"
data = \
  { 
  "identifier": "zlatts", 
  "password": "Lattijm007" 
  }
data = json.dumps(data)
API_KEY = "b7806c2a9e2ad166a6ef07ec3ae43f187c4af137"
headers = \
  {
  "Content-Type": "application/json; charset=UTF-8",
  "Accept": "application/json; charset=UTF-8",
  "VERSION": "2",
  "X-IG-API-KEY": API_KEY
  }
session.headers.update(headers)
response = session.post(session_url, data=data, headers=headers)


CST = response.headers["CST"]
X_SECURITY_TOKEN = response.headers["X-SECURITY-TOKEN"]
session.headers.update({"CST": CST, "X-SECURITY-TOKEN": X_SECURITY_TOKEN})


session.headers.update({"VERSION": "3"})
EPIC = "IX.D.ASX.IFT.IP"
RESOLUTION = "HOUR"
FROM = "2020-12-31T00%3A00%3A00"
TO = "2021-04-25T00%3A00%3A00"
PAGESIZE = 10
data_query = "/prices/{}?resolution={}&from={}&to={}&pageSize={}".format(EPIC, RESOLUTION, FROM, TO, PAGESIZE)
data_url = base_url + data_query
#data_url = "https://demo-api.ig.com/gateway/deal/prices/IX.D.ASX.IFT.IP?resolution=HOUR&from=2020-07-21T00%3A00%3A00&to=2020-07-22T00%3A00%3A00"
data_response = session.get(data_url)


data = json.loads(data_response.content)["prices"]
data_df = pd.DataFrame(data)
location = "/home/james/Documents/finance/asx-cloud/data/cfd/raw/2021.csv"
data_df.to_csv(location)


total_data_df = pd.concat([data_df, extra_data_df]).reset_index(drop=True)
total_data_df = total_data_df[["snapshotTime", "openPrice", "closePrice"]]
total_data_df["datetime"] = total_data_df["snapshotTime"].apply(parser.parse)
total_data_df["hour"] = total_data_df["datetime"].apply(lambda x: x.hour)
open_df = total_data_df[total_data_df["hour"] == 10]
close_df = total_data_df[total_data_df["hour"] == 16]

open_df["date"] = open_df["snapshotTime"].apply(lambda x: x.split(" ")[0])
open_df["open"] = open_df["openPrice"].apply(lambda x: x["bid"])
open_df = open_df.drop(["snapshotTime", "closePrice", "datetime", "hour", "openPrice"], axis = 1)
open_df = open_df.drop_duplicates().reset_index(drop=True)

close_df["date"] = close_df["snapshotTime"].apply(lambda x: x.split(" ")[0])
close_df["close"] = close_df["openPrice"].apply(lambda x: x["ask"])
close_df = close_df.drop(["snapshotTime", "closePrice", "datetime", "hour", "openPrice"], axis = 1)
close_df = close_df.drop_duplicates().reset_index(drop=True)

final_df = pd.merge(open_df, close_df, on="date")
location = "/home/james/Documents/finance/asx-cloud/data/cfd/processed/testing.csv"
final_df.to_csv(location, index=False)
