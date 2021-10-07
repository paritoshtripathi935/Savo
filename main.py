import time
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin

start = time.time()

# initialize the list of all the data you need through lists

urls = []
name = []
view = []
time_published = []
likes = []
no_comments = []

programming_languages = ["python", "java", "Cpp", "Javascript", "Sql"]  # add programing languages here only it will get all the data in one csv

driver = webdriver.Chrome('/home/paritosh/PycharmProjects/Youtube/chromedriver')  # this is chrome you have to download it from google

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


def Data():
    for j in range(0, len(programming_languages)):
        driver.get('https://www.youtube.com/results?search_query={}+programming+tutorial+&sp=CAMSAhAB'.format(programming_languages[j]))  # this is main try not touching it

        content = driver.page_source.encode('utf-8').strip()  # this get content of a page in normal way

        soup = BeautifulSoup(content, 'lxml')  # this is used to find the things we need in the source code

        titles = soup.findAll('a', id='video-title')  # titles of youtube videos is stored here

        video_urls = soup.findAll('a', id='video-title')  # links of urls are stored here
        i = 0
        j = 0

        for title in titles[0:]:
            name.append(title.text)  # adding names to the list

            base = "https://www.youtube.com/watch?v="  # base link

            link = video_urls[j].get('href')  # getting link

            url = urljoin(base, link)
            urls.append(url)
            i += 2
            j += 1


# calling the function
Data()

for links in range(0, len(urls)):
    import time

    driver.get(urls[links])  # going to each individual page

    time.sleep(2)  # giving 2 sec to load each website

    new_soup = BeautifulSoup(driver.page_source, 'lxml')  # have to built new soup since we are getting content from video page

    views = new_soup.find("span", class_="view-count style-scope ytd-video-view-count-renderer")  # getting views of each video

    like = new_soup.find("yt-formatted-string", class_="style-scope ytd-toggle-button-renderer style-text")  # getting likes of videos

    time = new_soup.find("span", class_="style-scope ytd-video-primary-info-renderer")  # Getting the age of video

    #no_comment = new_soup.find("span", class_="style-scope yt-formatted-string")  # getting no of comments

    print("downloading content", links)

    view.append(views.text)  # adding views to list
    likes.append(like.text)  # adding content to list
    time_published.append(time.text)  # adding age to list
    #no_comments.append(no_comment.text)  # adding age to


df = pd.DataFrame({"links": urls, "names": name, 'Views': view, "likes": likes, 'age': time_published})
df.to_csv("youtube.csv")


