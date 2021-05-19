#### IMPORTS ####
# System
import os
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
  
  # Create a processed dataframe with only desired fields (create year/month
  # file paths)
  pro_df = pd.DataFrame()
  pro_df["date"] = raw_df["Time"].apply(lambda x:  dt.strptime(x, "%m/%d/%Y").date())
  pro_df["open"] = raw_df["Open"]
  pro_df["high"] = raw_df["High"]
  pro_df["low"] = raw_df["Low"]
  pro_df["close"] = raw_df["Last"]
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
  
  # Write out files to their year/month file paths
  files = pro_df["file_path"].value_counts().index.values
  for file in files:
    file_df = pro_df[pro_df["file_path"] == file]
    file_df = file_df.drop("file_path", axis = 1)
    file_df.to_csv(pro_dir + "/" + file, index = False)  
  
  # Return the processed dataframe
  return pro_df


#### SUPPORT FUNCTIONS ####
def read_csv_pandas(file_path):
  """ Read in csv at file path as pandas dataframe """
  
  # Try read in file at file path
  try:
    file_data = pd.read_csv(file_path)
    if len(file_data) < 250:
      print("INFO: data frame at {} is not a full year".format(file_path))
  except:
    print("ERROR: failed to read in file at: {}".format(file_path))
    file_data = pd.DataFrame()
  return file_data
  
def read_raw_data(data_dir):
  """ Read in asx raw data """
  
  # Try to read in raw asx data
  try:
    file_paths = [data_dir + "/" + x for x in os.listdir(data_dir)]
    with mp.Pool() as pool:
      data = pool.map(read_csv_pandas, file_paths)
    data_df = pd.concat(data)
    data_df = data_df.reset_index(drop=True)
  except:
    print("ERROR: failed to read raw data at: {}".format(data_dir))
    data_df = pd.DataFrame()
  return data_df


#### RUN MAIN ####
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-rd", "--raw-dir",
    help="directory containing raw asx data",
    default="/home/james/Documents/finance/asx-cloud/data/asx/raw"
  )
  parser.add_argument(
    "-pd", "--pro-dir",
    help="directory containing processed asx data",
    default="/home/james/Documents/finance/asx-cloud/data/asx/processed"
  )
  args = parser.parse_args()
  
  pro_df = main(**args.__dict__)