#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 18:10:24 2021

@author: james
"""

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

# Set the start and end date to download data for
start_date = "{}/01/{}".format(date.month, date.year)
end_date = "{}/{}/{}".format(date.month, date.day, date.year)

# Get the selenium browser and login to barchart
browser = get_browser(download_path)
browser = login(browser, url, username, password)

# Download latest data from barchart 
download_files = set(os.listdir(download_path))
browser = download_data(browser, symbol, time_frame, start_date, end_date)
new_download_files = set(os.listdir(download_path))

# Read in the downloaded dataset then upload
data_file_name = list(new_download_files - download_files)[0]
data_path = download_path + "/" + data_file_name
original_data = read_data(data_path)  
_ = upload_csv(s3, bucket_name, output_path, original_data)

# Clean data then upload
cleaned_data = clean_data(original_data)
_ = upload_csv(s3, bucket_name, output_path, cleaned_data)































# def login(browser, url, username, password):
#   """ Login to the barchart website """
  
#   # Go to the login page
#   browser.get("https://www.google.com")
  
#   #
#   xpath = "//input[@title='Search']"
#   search_box = get_xpath_element(browser, xpath)
#   search_box.send_keys("barchart login")
#   search_box.send_keys(Keys.ENTER)
  
#   #
#   xpath = "//a[@href='https://www.barchart.com/login']"
#   barchart_result = get_xpath_element(browser, xpath)
#   barchart_result.send_keys()
  
#   # Enter username
#   xpath = "//input[@aria-label='Login with email']"
#   username_box = get_xpath_element(browser, xpath)
#   username_box.send_keys(username)
  
#   # Enter password
#   xpath = "//input[@aria-label='Password']"
#   password_box = get_xpath_element(browser, xpath)
#   password_box.send_keys(password)
  
#   # Click the login button and return the browser
#   xpath = "//button[@class='bc-button login-button']"
#   login_button = get_xpath_element(browser, xpath)
#   login_button.click()
#   return browser


# Search for xjo
xpath = "//input[@id='search']"
search_box = get_xpath_element(browser, xpath)
search_box.send_keys(symbol)
search_box.send_keys(Keys.ENTER)
time.sleep(random.random() + 1)

# Click on the historical data page
link_text = "Historical Data"
historical_button = WebDriverWait(browser, 10).until(
  EC.visibility_of_element_located(
    (By.LINK_TEXT, link_text)
  )
)
historical_button.click()
time.sleep(random.random() + 1)

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

# Click the download historical data button
xpath = "//a[@class='bc-button add light-blue']"
download_button = get_xpath_element(browser, xpath)
download_button.click()


