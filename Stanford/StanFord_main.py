import time
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import numpy
start = time.time()

# initialize the list of all the data you need through lists

urls = []
name = []
types = []
duration = []
difficulty_level = []
Course_description = []
platforms = []

driver = webdriver.Chrome(
    '/home/paritosh/PycharmProjects/Youtube/chromedriver')  # this is chrome you have to download it from google

# this is scrolling code if you need more data add this code in main function

"""
        while True:
            scroll_height = 3000
            document_height_before = driver.execute_script("return document.documentElement.scrollHeight")
            driver.execute_script(f"window.scrollTo(0, {document_height_before + scroll_height});")
            time.sleep(1.5)
            document_height_after = driver.execute_script("return document.documentElement.scrollHeight")
            if document_height_after == document_height_before:
                break    
"""

page = 8


def Data():
    for i in range(0, page):

        # this is the main link dont touch this

        print("Loading Page number ", i)

        driver.get(
            'https://online.stanford.edu/search-catalog?type=All&free_or_paid[free]=free&free_or_paid[paid]=paid&page={}'.format(i))

        content = driver.page_source.encode('utf-8').strip()  # this get content of a page in normal way

        soup = BeautifulSoup(content, 'lxml')  # this is used to find the things we need in the source code

        link_class = soup.findAll("div", class_="search-results--table")  # titles of youtube videos is stored here


Data()

print(urls)
driver.close()
