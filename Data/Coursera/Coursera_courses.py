import time
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin

start = time.time()

# initialize the list of all the data you need through lists

urls = []
name = []
types = []
duration = []
difficulty_level = []
Course_description = []
platforms = []
price = []

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

page = 20


def Data():
    for i in range(1, page):

        # this is the main link dont touch this

        print("Loading Page number ", i)

        driver.get(
            'https://www.coursera.org/search?query=python%20programming&page={}&index=prod_all_launched_products_term_optimization'.format(
                i))

        content = driver.page_source.encode('utf-8').strip()  # this get content of a page in normal way

        soup = BeautifulSoup(content, 'lxml')  # this is used to find the things we need in the source code

        link_class = soup.findAll("li", class_="ais-InfiniteHits-item")  # titles of youtube videos is stored here

        for j in link_class[0:]:
            link_tag = j.find("a")
            base = 'https://www.coursera.org/'
            try:
                if 'href' in link_tag.attrs:
                    link = link_tag.get('href')
            except:
                pass

            url = urljoin(base, link)
            urls.append(url)


Data()
'''
for links in range(0, len(urls)):
    driver.get(urls[links])

    new_soup = BeautifulSoup(driver.page_source, "lxml")

    title = new_soup.find("div", class_="field field-name-title")

    subject = new_soup.find("div", class_="field field-name-subject-area field-type-ds field-label-inline clearfix")
    subject_tag = subject.find("a")

    length = new_soup.find("div", class_="field field-name-field-duration")

    difficulty = new_soup.find("div",
                               class_="field field-name-field-difficulty field-type-list-text field-label-inline clearfix")
    difficulty_tag = difficulty.find("div", class_="field")

    description = new_soup.find("div", class_="field field-name-body field-type-text-with-summary field-label-above")
    description_tag = description.find("p")

    platform = new_soup.find("div", class_="field field-name-platform")
    try:
        platform_tag = platform.find("div", class_="field field-name-field-course-platform")
    except AttributeError:
        pass

    prices = new_soup.find("div", class_="field field-name-price")

    name.append(title.text)
    types.append(subject_tag.text)
    try:
        duration.append(length.text)
    except AttributeError:
        duration.append("self paced")
        pass
    difficulty_level.append(difficulty_tag.text)
    Course_description.append(description_tag.text)
    platforms.append(platform_tag.text)
    price.append(prices.text)
    print("getting details of page no ", links)

'''
print(len(urls))
print(urls)

driver.close()
'''
df = pd.DataFrame({"links": urls,
                   "names": name,
                   'Type': types,
                   "Duration": duration,
                   "Price": price,
                   "difficulty": difficulty_level,
                   "Description": Course_description,
                   "platform": platforms})

df.to_csv("Harvard_courses.csv")
'''