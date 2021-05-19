#### IMPORTS ####
# System
import os
import time
import json
import random
import hashlib
import datetime as dt
import multiprocessing as mp

# Local
None

# External
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#### SUPPORT FUNCTIONS ####
def read_json(file_path):
  """  """
  
  #
  with open(file_path, "r") as f:
    file_data = json.load(f)
  return file_data

def write_json(file_path, data):
  """  """
  
  # 
  try:
    with open(file_path, "w") as f:
      json.dump(data, f)
  except:
    return 0
  return 1

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

def scrape_metadata(article, afr_url):
  """ Scrape article metadata from its summary """
  
  # Create metadata dictionary and add data
  metadata = {}
  metadata["url"] = \
    afr_url + article.findAll("a", {"class": "_20-Rx"})[0]["href"]
  metadata["topic"] = \
    article.findAll("a", {"class": "_2_TMO"})[0].text
  metadata["title"] = \
    article.findAll("a", {"class": "_20-Rx"})[0].text
  metadata["summary"] = \
    article.findAll("p", {"class": "_48ktx"})[0].text
  metadata["date"] = \
    article.findAll("li", {"class": "_1PXiX"})[0].time["datetime"]
  metadata["author"] = \
    article.findAll("li", {"class": "_1P-s_"})[0].text
  return metadata

def scrape_article(proxy, url):
  """  """
  
  # Load article url
  browser = get_browser(proxy)
  try:
    browser.get(url)
  except:
    time.sleep(random.random()*30 + 60)
  try:
    WebDriverWait(browser, 60).until(
      EC.visibility_of_element_located(
        (By.XPATH, "//h1[@class='_3lFzE']")
      )
    )
    WebDriverWait(browser, 60).until(
      EC.visibility_of_element_located(
        (By.XPATH, "//a[@class='sLxGg']")
      )
    )
    WebDriverWait(browser, 60).until(
      EC.visibility_of_element_located(
        (By.XPATH, "//div[@class='_2cdD4']")
      )
    )
    html = browser.page_source
  except:
    browser.quit()
    return 0
  soup = BeautifulSoup(html, features="lxml")
  
  # Check if a paywall has been found
  paywall = soup.findAll("p", {"class": "_3MT_w"})
  if paywall != []:
    browser.quit()
    return 0
  
  # Print out article information  
  title = soup.findAll("h1", {"class": "_3lFzE"})[0].text
  author = soup.findAll("a", {"class": "sLxGg"})[0].text
  date = soup.findAll("div", {"class": "_2cdD4"})[0].time.text
  print(80*"-")
  print("number: {}\ntitle:  {} \nauthor: {}\ndate:   {}".format(article_number, title, author, date))
  print(80*"-")
  
  # Scrape article text
  tags = soup.findAll("article")[0].findAll("div", {"class": "tl7wu"})
  text = {}
  heading = "heading"
  paragraph = []
  for text_tag in tags:
    for tag in text_tag.findAll():
      if tag.name == "h2":
        if paragraph == [] or heading == "": 
          continue
        text[heading] = paragraph
        heading = tag.text
        paragraph = []
      elif tag.name == "p":
        paragraph += [tag.text]
      elif tag.name == "ul":
        for t in tag:
          if t.name == "li":
            paragraph += [t.text]
  
  # Wait, quit browser and add to previous ids
  time.sleep(random.random()*30 + 30)
  browser.quit()
  return text


#### CLASSES ####
class AfrWebScrapingBot:
  """  """
  
  def __init__(self, topic_url, data_dir, proxy_list):
    self.afr_base_url = "https://www.afr.com"
    self.topic_url = topic_url
    self.topic_home_url = self.afr_base_url + self.topic_url
    self.data_dir = data_dir
    self.proxy_list = []
    self.home_browser = None
    
  def load_data(self):
    """  """
    
    #
    file_paths = [self.data_dir + "/" + x for x in os.listdir(self.data_dir)]
    with mp.Pool() as pool:
      data = pool.map(read_json, file_paths)
    self.article_data = {x["url_id"]: x for x in data}
  
  def start_browser(self, proxy=False, headless=False):
    """ Creates a (undetectable) selenium browser with proxy and/or headless """
  
    # Get a selenium browser (undetectable)
    options = uc.ChromeOptions()
    if proxy:
      options.add_argument('--proxy-server=http://%s' % proxy)
    if headless:
      options.headless=True
      options.add_argument('--headless')
    self.home_browser = uc.Chrome(options=options)
    
  def get_topic(self):
    """  """
    
    # Go to the topic page, get the page soup and articles
    self.home_browser.get(self.afr_base_url + self.topic_url)
    
  def get_topic_soup(self):
    """  """
    
    #
    WebDriverWait(self.home_browser, 60).until(
      EC.visibility_of_element_located(
        (By.XPATH, "//div[@class='_2slqK']")
      )
    )
    self.home_html = self.home_browser.page_source
    self.home_soup = BeautifulSoup(self.home_html, features="lxml")
    self.articles = self.home_soup.findAll("div", {"class": "_2slqK"})
    return articles
  
  

#### MAIN ####
# Start chrome
proxy = "165.225.77.47:9443"
#proxy = "165.22.64.68:33631"
#proxy = "167.172.184.166:34142"
#proxy = "167.71.41.173:42185"
#proxy = "167.172.171.151:41233"
#proxy = "165.227.173.87:41044"
proxy = "165.225.77.44:80"
home_browser = get_browser(proxy)

# Load previous data
data_dir = "/home/james/Documents/finance/asx-cloud/data/afr/raw"
previous_ids = read_previous_ids(data_dir)

# Load afr topic feed
afr_url = "https://www.afr.com"
topic_url = "/topic/before-the-bell-hwg"
article_max = 20
articles = scrape_topic(home_browser, afr_url, topic_url)

# Iterate through the articles
article_number = 0
continue_scrape = 1
while continue_scrape:
  for article in articles:
    
    # Scrape metadata
    metadata = scrape_metadata(article, afr_url)
    
    # Get the url id and check that it hasnt been processed else process
    url = metadata["url"]
    url_id = hashlib.md5(url.encode()).hexdigest()
    if url_id in previous_ids:
      continue
    else:
      article_data = {}
      article_data["url_id"] = url_id
      article_data["extraction_date"] = dt.datetime.now().strftime("%d-%m-%Y")
      article_data.update(metadata)
    
    # Scrape the article text and write out the article data
    try:
      text = scrape_article(proxy, url)
    except:
      text = 0
    if len(text) == 1:
      text = 0
    if text:
      article_data["text"] = text
    else:
      continue_scrape = 0
      break
    previous_ids += [url_id]
    write_article_data(data_dir, article_data)
    
    #
    if article_number > article_max:
      continue_scrape = 0
      break
    else:
      article_number += 1
  
  #
  show_more = WebDriverWait(home_browser, 60).until(
    EC.visibility_of_element_located(
      (By.XPATH, "//button[@class='_3zImT']")
    )
  )
  show_more.click()
  number_articles = len(articles)
  while number_articles == len(articles):
    html = home_browser.page_source
    soup = BeautifulSoup(html, features="lxml")
    articles = soup.findAll("div", {"class": "_2slqK"})
    time.sleep(1)

# 
home_browser.quit()
