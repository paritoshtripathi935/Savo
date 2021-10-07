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

programming_languages = ["python", "java", "Cpp"]

driver = webdriver.Chrome('/home/paritosh/PycharmProjects/Youtube/chromedriver')
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

def main():
    for i in range(0, len(programming_languages)):
        driver.get('https://www.youtube.com/results?search_query={}+programming+tutorial+&sp=CAMSAhAB'.format(programming_languages[i]))

        content = driver.page_source.encode('utf-8').strip()
        soup = BeautifulSoup(content, 'lxml')
        titles = soup.findAll('a', id='video-title')
        video_urls = soup.findAll('a', id='video-title')
        i = 0
        j = 0

        for title in titles[0:]:
            name.append(title.text)
            base = "https://www.youtube.com/watch?v="
            link = video_urls[j].get('href')
            url = urljoin(base, link)
            urls.append(url)
            i += 2
            j += 1


main()


df = pd.DataFrame({"links": urls, "names": name})
df.to_csv("youtube_python.csv")

end = time.time()

print((end - start) / 60, "min")
