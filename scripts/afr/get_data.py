#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 17:32:47 2021

@author: James_Latts
"""


#### IMPORTS ####
import os
import time
import json
import random
import hashlib
import datetime as dt
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#### SUPPORT FUNCTIONS ####
def read_previous_ids(data_dir):
  """  """
  
  #
  files = os.listdir(data_dir)
  previous_ids = [x.split(".")[0] for x in files]
  return previous_ids

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

def get_browser(proxy=False, headless=False):
  """ Returns a (undetectable) selenium browser with proxy and/or headless """
  
  # Get a selenium browser (undetectable)
  options = uc.ChromeOptions()
  if proxy:
    options.add_argument('--proxy-server=http://%s' % proxy)
  if headless:
    options.headless=True
    options.add_argument('--headless')
  browser = uc.Chrome(options=options)
  return browser

def scrape_topic(browser, afr_url, topic_url):
  """  """
  
  # Go to the topic page, get the page soup and articles
  home_url = afr_url + topic_url
  browser.get(home_url)
  WebDriverWait(browser, 60).until(
    EC.visibility_of_element_located(
      (By.XPATH, "//div[@class='_2slqK']")
    )
  )
  html = browser.page_source
  soup = BeautifulSoup(html, features="lxml")
  articles = soup.findAll("div", {"class": "_2slqK"})
  return articles

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
  paywall = soup.findAll("div", {"class": "_3MT_w"}) 
  if paywall != []:
    browser.quit()
    return 0
  
  # Print out article information  
  title = soup.findAll("h1", {"class": "_3lFzE"})[0].text
  author = soup.findAll("a", {"class": "sLxGg"})[0].text
  date = soup.findAll("div", {"class": "_2cdD4"})[0].time.text
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


#### MAIN ####
# Start chrome
proxy = "165.225.77.47:9443"
proxy = "165.22.64.68:33631"
proxy = "167.172.184.166:34142"
proxy = "167.71.41.173:42185"
proxy = "167.172.171.151:41233"
#proxy = "165.227.173.87:41044"
home_browser = get_browser(proxy)

# Load afr topic feed
afr_url = "https://www.afr.com"
topic_url = "/topic/before-the-bell-hwg"
articles = scrape_topic(home_browser, afr_url, topic_url)

# Load previous data
data_dir = "/home/james/Documents/finance/asx-cloud/data/afr/raw"
previous_ids = read_previous_ids(data_dir)

# Iterate through the articles
article_number = 0
print(80*"-")
for article in articles:
  
  # Scrape metadata
  metadata = scrape_metadata(article, afr_url)
  
  # Get the url id and check that it hasnt been processed else process
  url = metadata["url"]
  url_id = hashlib.md5(url.encode()).hexdigest()
  if url_id in previous_ids:
    continue
  else:
    article_number += 1
    article_data = {}
    article_data["url_id"] = url_id
    article_data["extraction_date"] = dt.datetime.now().strftime("%d-%m-%Y")
    article_data.update(metadata)
  
  # Scrape the article text and write out the article data
  text = scrape_article(proxy, url)
  if text:
    article_data["text"] = text
  else:
    continue
  previous_ids += [url_id]
  write_article_data(data_dir, article_data)

# 
home_browser.quit()
