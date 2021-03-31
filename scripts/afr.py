#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 17:32:47 2021

@author: James_Latts
"""


#
import hashlib
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

options = uc.ChromeOptions()
#options.headless=True
#options.add_argument('--headless')
#PROXY = "23.23.23.23:3128" # IP:PORT or HOST:PORT
#options.add_argument('--proxy-server=http://%s' % PROXY)
home_browser = uc.Chrome(options=options)

#
afr_url = "https://www.afr.com"
home_url = afr_url + "/topic/before-the-bell-hwg"
home_browser.get(home_url)
html = home_browser.page_source
soup = BeautifulSoup(html)

#
article_data = {}
articles = soup.findAll("div", {"class": "_2slqK"})
for article in articles[:2]:
  
  #
  article_json = {}
  url = afr_url + article.findAll("a", {"class": "_20-Rx"})[0]["href"]
  url_id = hashlib.md5(url.encode()).hexdigest()
  article_json["url"] = url
  article_json["topic"] = \
    article.findAll("a", {"class": "_2_TMO"})[0].text
  article_json["title"] = \
    article.findAll("a", {"class": "_20-Rx"})[0].text
  article_json["summary"] = \
    article.findAll("p", {"class": "_48ktx"})[0].text
  article_json["date"] = \
    article.findAll("li", {"class": "_1PXiX"})[0].time["datetime"]
  article_json["author"] = \
    article.findAll("li", {"class": "_1P-s_"})[0].text  
  article_data[url_id] = article_json
  
  #
  new_browser = uc.Chrome()
  new_browser.get(url)
  new_html = new_browser.page_source
  new_soup = BeautifulSoup(new_html)
  new_browser.close()
  
  #
  #title = new_soup.findAll("h1", {"class": "_3lFzE"})[0].text
  #author = new_soup.findAll("a", {"class": "sLxGg"})[0].text
  #date = new_soup.findAll("div", {"class": "_2cdD4"})[0].time.text
  text_tags = new_soup.findAll("article")[0].findAll("div", {"class": "tl7wu"})
  text_json = {}
  heading = "heading"
  paragraph = []
  for text_tag in text_tags:
    for tag in text_tag.findAll():
      if tag.name == "h2":
        text_json[heading] = paragraph
        heading = tag.text
        paragraph = []
      elif tag.name == "p":
        paragraph += [tag.text]
      elif tag.name == "ul":
        for t in tag:
          if t.name == "li":
            paragraph += [t.text]
  article_data[url_id]["text"] = text_json