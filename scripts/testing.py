#### IMPORTS
import boto3
import numpy as np
import pandas as pd


#### MAIN
# Main here


#### FUNCTION
def download_file(client, bucket, path):
  """ Download a file from s3 bucket path """
  
  # Get the file object from s3 and return file/response
  response = client.get_object(Bucket=bucket, Key=path)
  file = response["Body"]
  return response, file

def download_csv(client, bucket, path):
  """ Download csv file from s3 to pandas dataframe """
  
  # Download the file and load in pd df
  response, file = download_file(client, bucket, path)
  csv = pd.read_csv(file)
  return response, csv

def upload_file(client, bucket, path, file):
  """ Upload a file to s3 bucket path """
  
  # Put the file object into s3 and return the response
  response = client.put_object(Bucket=bucket, Key=path, Body=file)
  return response

def upload_csv(client, bucket, path, df):
  """ Upload pandas dataframe to s3 as a csv file """
  
  # Convert pd df to file bytes then upload
  file = df.to_csv().encode()
  response = upload_file(client, bucket, path, file)
  return response

def clean_data(data):
  """ Cleans input data from barchart.com """
  
  # Create dataframe
  cleaned_data = pd.DataFrame()
  cleaned_data["date_time"] = pd.to_datetime(data["Time"])
  
  # Add data
  cleaned_data["open"] = data["Open"]
  cleaned_data["high"] = data["High"]
  cleaned_data["mid"] = (data["High"] + data["Low"]) / 2
  cleaned_data["low"] = data["Low"]
  cleaned_data["last"] = data["Last"]
  
  # Set index and filter out open/close hours
  cleaned_data = cleaned_data.set_index("date_time")
  cleaned_data = cleaned_data[cleaned_data.index.hour != 10]
  cleaned_data = cleaned_data[cleaned_data.index.hour != 17]
  return cleaned_data


#### CLASS
# Classes here


#### RUN MAIN
# Create the client and set the bucket and path to the file
s3_client = boto3.client("s3")
bucket = "awsfrees3"
input_path = "original/xjo/hourly/2021/02.csv"
output_path = "cleaned/xjo/hourly/2021/02.csv"


# Download and clean the data
input_response, data = download_csv(s3_client, bucket, input_path)
cleaned_data = clean_data(data)
output_response = upload_csv(s3_client, bucket, output_path, cleaned_data)