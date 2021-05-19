#### IMPORTS ####
# System
import os
import json
import copy
import argparse

# Local
None

# External
import pandas as pd
import multiprocessing as mp
from datetime import datetime as dt


#### MAIN ####
def main(raw_dir, pro_dir):
  # First read in the raw data and clean (drop na's and duplicates)
  raw_df = read_raw_data(raw_dir)
  raw_df = raw_df.dropna()
  raw_df = raw_df.drop_duplicates()
  
  # Create a processed dataframe with desired fields (create year/month
  # file paths)
  pro_df = copy.deepcopy(raw_df)
  pro_df["original_date"] = pro_df["date"]
  pro_df["date"] = pro_df["date"].apply(lambda x: dt.strptime(" ".join(x.split(" ")[0:3]), "%b %d, %Y").date())
  pro_df["year"] = pro_df["date"].apply(lambda x: x.year)
  pro_df["month"] = pro_df["date"].apply(lambda x: x.month)
  pro_df["file_path"] = pro_df["year"].astype(str) + "/" + pro_df["month"].astype(str) + ".csv"
  
  # Create the year folders in processed directory
  years = pro_df["year"].value_counts().index.values
  for year in years:
    if str(year) not in os.listdir(pro_dir):
      os.mkdir(pro_dir + "/" + str(year))
  
  # Order the dataframe by date then convert to string and drop cols
  pro_df = pro_df.sort_values("date", ascending=False)
  pro_df["date"] = pro_df["date"].apply(lambda x: x.strftime("%Y/%m/%d"))
  pro_df = pro_df.drop(["year", "month"], axis = 1)
  if "text" in pro_df.columns:
    schema = ["url_id", "url", "date", "title", "summary", "text", "extraction_date", "original_date", "topic", "author", "file_path"]
  else:
    schema = ["url_id", "url", "date", "title", "summary", "extraction_date", "author", "file_path"]
  pro_df = pro_df[schema]
  
  # Write out files to their year/month file paths
  files = pro_df["file_path"].value_counts().index.values
  for file in files:
    file_df = pro_df[pro_df["file_path"] == file]
    file_df = file_df.drop("file_path", axis = 1)
    file_df.to_csv(pro_dir + "/" + file, index = False)
  
  # Return the processed dataframe
  return pro_df


#### SUPPORT FUNCTIONS ####
def read_json(file_path):
  """ Read in json file at given file path """
  
  # Try to read in data else return null
  try:
    with open(file_path, "r") as f:
      file_data = json.load(f)
    return file_data
  except:
    print("ERROR: failed to read data at: {}".format(file_path))
    return {}
  
def read_raw_data(data_dir):
  """ Read in afr raw data """
  
  # Try to read in raw afr data
  data_dict = {}
  try:
    file_paths = [data_dir + "/" + x for x in os.listdir(data_dir)]
    with mp.Pool() as pool:
      data = pool.map(read_json, file_paths)
    for d in data:
      data_dict[d["url_id"]] = d
  except:
    print("ERROR: failed to read raw data at: {}".format(data_dir))
  data_df = pd.DataFrame.from_dict(data_dict, orient="index")
  return data_df


#### RUN MAIN ####
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-rd", "--raw-dir",
    help="directory containing raw asx data",
    default="/home/james/Documents/finance/asx-cloud/data/afr/btb/raw"
  )
  parser.add_argument(
    "-pd", "--pro-dir",
    help="directory containing processed asx data",
    default="/home/james/Documents/finance/asx-cloud/data/afr/btb/processed"
  )
  args = parser.parse_args()
  
  pro_df = main(**args.__dict__)