import time
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin

start = time.time()

urls = []
name = []
view = []
time_published = []


def main():
    driver = webdriver.Chrome('/home/paritosh/PycharmProjects/Youtube/chromedriver')
    driver.get('https://www.youtube.com/results?search_query=python+programming+tutorial+in+english+&sp=CAMSAhAB')

    while True:
        scroll_height = 2000
        document_height_before = driver.execute_script("return document.documentElement.scrollHeight")
        driver.execute_script(f"window.scrollTo(0, {document_height_before+ scroll_height});")
        time.sleep(1.5)
        document_height_after = driver.execute_script("return document.documentElement.scrollHeight")
        if document_height_after == document_height_before:
            break

    content = driver.page_source.encode('utf-8').strip()
    soup = BeautifulSoup(content, 'lxml')
    titles = soup.findAll('a', id='video-title')
    views = soup.findAll('span', class_="style-scope ytd-video-meta-block")
    video_urls = soup.findAll('a', id='video-title')
    i = 0
    j = 0
    for title in titles[0:]:
        name.append(title.text)
        view.append(views[i+2].text)
        time_published.append(views[i+1].text)
        base = "https://www.youtube.com/watch?v="
        link = video_urls[j].get('href')
        url = urljoin(base, link)
        urls.append(url)
        i += 2
        j += 1


main()

df = pd.DataFrame({"links": urls, "names": name, "views": view, "age": time_published})
df.to_csv("youtube_python.csv")

end = time.time()

print((end - start) / 60, "min")
