#### IMPORTS ####
# System
import os
import time
import json
import random
import hashlib
import argparse
import multiprocessing as mp
from datetime import datetime as dt

# Local
None

# External
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#### MAIN ####
def main(raw_dir, topic_url, article_max, proxy):
  # Create the web scraping bot scrape then return the bot
  bot = AfrWebScrapingBot(topic_url, raw_dir, article_max, [proxy])
  bot.scrape_articles()
  return bot

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

def write_json(file_path, data):
  """ Writes out a dictionary data to a json file at given file path """
  
  # Try to write out the data else return null
  try:
    with open(file_path, "w") as f:
      json.dump(data, f)
  except:
    print("ERROR: failed to write data at: {}".format(file_path))
    return 0
  return 1

def start_browser(proxy=False, headless=False):
  """ Creates a (undetectable) selenium browser with proxy and/or headless """
  
  # Get a selenium browser (undetectable)
  options = uc.ChromeOptions()
  if proxy:
    options.add_argument('--proxy-server=http://%s' % proxy)
  if headless:
    options.headless=True
    options.add_argument('--headless')
  browser = uc.Chrome(options=options)
  return browser

def scroll_to_element(browser, xpath, timeout_time=60):
  """ Scroll to element at given xpath in browser (with timeout time) """
  
  # Find element and scroll to it
  element = \
    WebDriverWait(browser, timeout_time).until(
      EC.visibility_of_element_located(
        (By.XPATH, xpath)
      )
    )
  return element


#### CLASSES ####
class AfrWebScrapingBot:
  """ Web scraping bot for the Australian Financial Review (AFR) """
  
  def __init__(self, topic_url, data_dir, article_max, proxy_list):
    self.afr_base_url = "https://www.afr.com"
    self.topic_url = topic_url
    self.data_dir = data_dir
    self.article_max = article_max
    self.proxy = random.choice(proxy_list) if proxy_list else False
    
    self.headless = True
    self.home_browser = None
    self.article_number = 0
    self.load_data()
  
  def __del__(self):
    if self.home_browser:
      self.home_brower.quit()
  
  def load_data(self):
    """ Load previous data from data dir """
    
    # Load list of files from data dir (using mp) and join
    file_paths = [self.data_dir + "/" + x for x in os.listdir(self.data_dir)]
    with mp.Pool() as pool:
      data = pool.map(read_json, file_paths)
    self.data = {x["url_id"]: x for x in data}
  
  def write_data(self):
    """ Write out data to data dir """
    
    # Iterate through url ids in data and write out
    for url_id in self.data:
      data_location = self.data_dir + "/" + url_id + ".json"
      message = write_json(data_location, self.data[url_id])
      if message:
        print("ERROR: failed to write url id: {}".format(url_id))
  
  def get_topic(self):
    """ Get the topic url on home browser """
    
    # Go to the topic page
    self.home_browser = start_browser(self.proxy, False)
    self.home_browser.get(self.afr_base_url + self.topic_url)
  
  def get_articles(self):
    """ Get the articles from current home browser page """
    
    # Try to scroll to an article then extract articles
    article_xpath = "//div[@class='_2slqK']"
    try:
      scroll_to_element(self.home_browser, article_xpath)
    except:
      print("INFO: no articles on current home browser page")
    self.home_html = self.home_browser.page_source
    self.home_soup = BeautifulSoup(self.home_html, features="lxml")
    self.articles = self.home_soup.findAll("div", {"class": "_2slqK"})
    self.number_of_articles = len(self.articles)
  
  def scrape_articles(self):
    """ Scrape the articles for set topic """
    
    # Iterate through the articles and process until article max reached
    self.get_topic()
    self.get_articles()
    continue_scrape = True
    while continue_scrape:
      for article in self.articles:
        metadata = self.scrape_metadata(article)
        if not metadata:
          continue
        url = metadata["url"]
        url_id = hashlib.md5(url.encode()).hexdigest()
        article_data = {}
        if url_id not in self.data:
          article_data["url_id"] = url_id
          article_data["extraction_date"] = dt.now().strftime("%d-%m-%Y")
          article_data.update(metadata)
          data_location = self.data_dir + "/" + url_id + ".json"
          write_json(data_location, article_data)
          self.data[url_id] = article_data
          if self.article_number > self.article_max:
            continue_scrape = False
            break
          else:
            self.article_number += 1
      continue_scrape = self.load_more()  
  
  def scrape_metadata(self, article):
    """ Scrape article metadata from its summary """
    
    # Create metadata dictionary and add data
    metadata = {}
    href = article.findAll("a", {"class": "_20-Rx"})
    if not href:
      return metadata
    metadata["url"] = self.afr_base_url + href[0]["href"]
    title = article.findAll("a", {"class": "_20-Rx"})
    metadata["title"] = title[0].text if title else ""
    summary = article.findAll("p", {"class": "_48ktx"})
    metadata["summary"] = summary[0].text if summary else ""
    date = article.findAll("li", {"class": "_1PXiX"})
    metadata["date"] = date[0].time["datetime"] if date else ""
    author = article.findAll("li", {"class": "_1P-s_"})
    metadata["author"] = author[0].text if author else ""
    print(80*"-")
    print("number: {}\ntitle:  {} \nauthor: {}\ndate:   {}".format( \
      self.article_number, metadata["title"], metadata["author"], metadata["date"]))
    print(80*"-")
    return metadata
  
  def load_more(self):
    """ Loads more articles in the browser """
    
    # Find the show more button and click
    show_more_xpath = "//button[@class='_3zImT']"
    show_more = scroll_to_element(self.home_browser, show_more_xpath)
    show_more.click()
    
    # Keep waiting until more articles load
    retry = 0
    previous_number_of_articles = self.number_of_articles
    while previous_number_of_articles == self.number_of_articles:
      self.get_articles()
      time.sleep(1)
      if retry > 100:
        return False
      else:
        retry += 1
    return True


#### RUN MAIN ####
if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-rd", "--raw-dir",
    help="directory containing raw afr data",
    default="/home/james/Documents/finance/asx-cloud/data/afr/btb/raw"
  )
  parser.add_argument(
    "-tu", "--topic-url",
    help="url to the afr topic to scrape",
    default="/topic/before-the-bell-hwg"
  )
  parser.add_argument(
    "-am", "--article-max",
    help="max number of articles to scrape from afr website",
    default=2000
  )
  parser.add_argument(
    "-p", "--proxy",
    help="proxy to use for selenium driver when webscraping",
    default="165.225.77.44:80" # others ["165.225.77.47:9443", "165.22.64.68:33631", "167.172.184.166:34142", "167.71.41.173:42185", "167.172.171.151:41233", "165.227.173.87:41044"]
  )
  args = parser.parse_args()
  
  bot = main(**args.__dict__)