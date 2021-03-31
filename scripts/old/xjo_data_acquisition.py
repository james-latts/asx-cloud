#### IMPORTS
# System
import io
import os
import time
import random
import argparse
import calendar
import datetime as dt

# External
import boto3
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver import Firefox, FirefoxProfile
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Local
None

# Global variables
TIME_FRAMES = {
  "intraday": 0,
  "daily": 1,
  "weekly": 2
  #"monthly": 3, # currently cant handle this
  #"quarterly": 4 # currently cant handle this
  }


#### MAIN
def main(download_path, username, password, bucket_name, symbol, time_frame, date):      
  # Set the barchart url and symbol
  url = "https://www.barchart.com/login"
  symbol_name = symbol.split(".")[0].lower()
  time_frame_name = "/".join(time_frame)
  month_name = str(date.month) if date.month > 9 else "0" + str(date.month)
  
  # Set the input and output path for
  input_path = "original/{}/{}/{}/{}.csv".format(symbol_name, time_frame_name, str(date.year), month_name)
  output_path = "cleaned/{}/{}/{}/{}.csv".format(symbol_name, time_frame_name, str(date.year), month_name)
  
  # Create the client and set the bucket and path to the file
  s3 = boto3.client("s3")
  
  # Download previously processed data if possible
  try:
    all_objects = s3.list_objects(Bucket = bucket_name)
    if input_path in [x["Key"] for x in all_objects["Contents"]]:
      _, previous_data = download_csv(s3, bucket_name, output_path)
      
      # Check if current data is up to date
      previous_date = max(pd.to_datetime(previous_data["date_time"]))
      if date - previous_date < dt.datetime(1) and time_frame[0] == "intraday":
        current_minutes = date.hour * 60 + date.minute
        previous_minutes = previous_date.hour * 60 + previous_date.minute
        if current_minutes - previous_minutes < int(time_frame[1]):
          return previous_data
      elif date - previous_date < dt.datetime(1):
        return previous_data
  except:
    pass
  
  # Set the start and end date to download data for
  start_date = "{}/01/{}".format(date.month, date.year)
  end_date = "{}/{}/{}".format(date.month, date.day, date.year)
  
  # Get the selenium browser and login to barchart
  browser = get_browser(download_path)
  browser = login(browser, url, username, password)
  
  # Download latest data from barchart 
  download_files = set(os.listdir(download_path))
  browser = download_data(browser, symbol, time_frame, start_date, end_date)
  time.sleep(random.random() * 5 + 10)
  browser.quit()
  new_download_files = set(os.listdir(download_path))
  
  # Read in the downloaded dataset then upload
  data_file_name = list(new_download_files - download_files)[0]
  data_path = download_path + "/" + data_file_name
  original_data = read_data(data_path)  
  _ = upload_csv(s3, bucket_name, output_path, original_data)
  
  # Clean data then upload and return
  cleaned_data = clean_data(original_data)
  _ = upload_csv(s3, bucket_name, output_path, cleaned_data)
  return cleaned_data

#### FUNCTION
def get_browser(download_path):
  """ Start a selenuim Firefox browser (with default web scraping settings) """
  
  # Set the options preferences
  options = Options()
  options.add_argument("--headless")
  options.add_argument("--width=2560")
  options.add_argument("--height=1440")
  options.set_preference("dom.webdriver.enabled", False)
  options.set_preference("useAutomationExtension", False)
  options.set_preference("browser.link.open_newwindow", 3)
  options.set_preference("browser.link.open_newwindow.restriction", 0)
  options.set_preference("browser.download.folderList", 2)
  options.set_preference("browser.download.dir", download_path)
  options.set_preference('browser.download.manager.showWhenStarting', False)
  options.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
  
  # Set profile preferences
  profile = FirefoxProfile()
  profile.set_preference("dom.webdriver.enabled", False)
  profile.set_preference("useAutomationExtension", False)
  profile.set_preference("browser.link.open_newwindow", 3)
  profile.set_preference("browser.link.open_newwindow.restriction", 0)
  profile.set_preference("browser.download.folderList", 2)
  profile.set_preference("browser.download.dir", download_path)
  profile.set_preference('browser.download.manager.showWhenStarting', False)
  profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'text/csv')
  profile.update_preferences()
  
  # Create and return the browser
  browser = Firefox(firefox_profile=profile, options=options)
  return browser

def get_xpath_element(browser, xpath, wait_time=10):
  """ Get the element in the current browser using xpath expression """
  
  # Use the webdriver wait function to get the element and return
  element = WebDriverWait(browser, wait_time).until(
    EC.visibility_of_element_located(
      (By.XPATH, xpath)
    )
  )
  return element

def login(browser, url, username, password):
  """ Login to the barchart website """
  
  # Go to the login page
  browser.get(url)
  
  # Enter username
  xpath = "//input[@aria-label='Login with email']"
  username_box = get_xpath_element(browser, xpath)
  username_box.send_keys(username)
  
  # Enter password
  xpath = "//input[@aria-label='Password']"
  password_box = get_xpath_element(browser, xpath)
  password_box.send_keys(password)
  
  # Click the login button and return the browser
  xpath = "//button[@class='bc-button login-button']"
  login_button = get_xpath_element(browser, xpath)
  login_button.click()
  time.sleep(random.random() * 5 + 10)
  return browser

def download_data(browser, symbol, time_frame, start_date, end_date):
  """ Download historical hourly data """
  
  # Search for xjo
  xpath = "//input[@id='search']"
  search_box = get_xpath_element(browser, xpath)
  search_box.send_keys(symbol)
  search_box.send_keys(Keys.ENTER)
  time.sleep(random.random() * 5 + 10)
  
  # Click on the historical data page
  link_text = "Historical Data"
  historical_button = WebDriverWait(browser, 10).until(
    EC.visibility_of_element_located(
      (By.LINK_TEXT, link_text)
    )
  )
  historical_button.click()
  time.sleep(random.random() * 5 + 10)
  
  # Select the period using the dropdown (time frame)
  xpath = "//select[@aria-label='Select frequency']"
  period_dropdown = get_xpath_element(browser, xpath)
  Select(period_dropdown).options[TIME_FRAMES[time_frame[0]]].click()
  
  # Select time frame if intraday specified
  if time_frame[0] == "intraday":
    xpath = "//input[@aria-label='Enter intraday minutes']"
    minutes_box = get_xpath_element(browser, xpath)
    minutes_box.clear()
    minutes_box.send_keys(time_frame[1])
  
  # Set the start and end dates
  xpath = "//input[@aria-label='Start Date']"
  start_date_box = get_xpath_element(browser, xpath)
  start_date_box.click()
  start_date_box.clear()
  start_date_box.clear()
  start_date_box.send_keys(start_date)
  start_date_box.send_keys(Keys.ENTER)  
  
  # Set the start and end dates
  xpath = "//input[@aria-label='End Date']"
  end_date_box = get_xpath_element(browser, xpath)
  end_date_box.click()
  end_date_box.clear()
  end_date_box.clear()
  end_date_box.send_keys(end_date)
  end_date_box.send_keys(Keys.ENTER)
  time.sleep(random.random() * 5 + 10)
  
  # Click the download historical data button
  xpath = "//a[@class='bc-button add light-blue']"
  download_button = get_xpath_element(browser, xpath)
  download_button.click()
  return browser

def read_data(data_path):
  """ Read in downloaded barchart data """
  
  # Open the file and remove the last line then read into pandas
  with open(data_path, "r") as dfile:
    stringio = io.StringIO("".join(dfile.readlines()[:-1]))
    data = pd.read_csv(stringio)
  return data

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
None


#### RUN MAIN
if __name__ == "__main__":
  # Set input variable parser
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-dp", "--download-path",
    help="The path to download the barchart data",
    default="",
    type=str
  )
  parser.add_argument(
    "-u", "--username",
    help="Username for barchart",
    default="",
    type=str
  )
  parser.add_argument(
    "-p", "--password",
    help="Password for barchart username",
    default="",
    type=str
  )
  parser.add_argument(
    "-bn", "--bucket-name",
    help="Bucket name for s3 bucket",
    default="",
    type=str
  )
  parser.add_argument(
    "-s", "--symbol",
    help="Data symbol on barchart",
    default="",
    type=str
  )
  parser.add_argument(
    "-tf", "--time-frame",
    help="Time frame to extract data in",
    default=["intraday", "60"],
    type=list
  )
  parser.add_argument(
    "-d", "--date",
    help="Extract date (format %d/%m/%Y, default='' => current date)",
    default="",
    type=str
  )
  args = parser.parse_args()
  
  # Set default username/password/bucket name
  args.download_path = "/home/ubuntu/data/downloads"
  args.username = "zlatts@gmail.com"
  args.password = "Lattijm007"
  args.bucket_name = "awsfrees3"
  args.symbol = "XJO.AX"
  args.date = "01/01/2021"
  
  # Check the time frame input is correct
  try:
    if args.time_frame[0].lower() not in TIME_FRAMES.keys():
      print("Error: bad time frame input exiting now")
      exit()
    elif args.time_frame[0] == "intraday" and len(args.time_frame) == 2:
      if int(args.time_frame[1]) > 60:
        print("Error: bad time frame input exiting now")
        exit()
    elif args.time_frame[0] != "intraday" and len(args.time_frame) != 1:
      print("Error: bad time frame input exiting now")
      exit()
    else:
      print("Error: bad time frame input exiting now")
      exit()      
  except:
    print("Error: bad time frame input exiting now")
    exit()
  
  # Check the date input is correct
  try:
    if args.date == "":
      args.date = dt.datetime.now()
    else:
      args.date = dt.datetime.strptime(args.date, "%d/%m/%Y")
      current_date = dt.datetime.now()
      if args.date - current_date > dt.timedelta(0):
        args.date = current_date
      elif args.date.month != current_date.month:
        day = calendar.monthrange(args.date.year, args.date.month)[-1]
        args.date = dt.datetime(year=args.date.year, month=args.date.month, day=day)
        args.date = args.date.replace(hour=23, minute=59)
  except:
    print("Error: bad start/end date input exiting now")
    exit()
  
  # Adjust date to be last weekday of the month
  if args.date.weekday() > 4:
    args.date = args.date - dt.timedelta(days=1 + args.date.weekday() - 5)
    args.date = args.date.replace(hour=23, minute=59)
  
  data = main(**args.__dict__)