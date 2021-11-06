import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

start = time.time()

rows = []  # list to store all the elements
delay = 15

programming_languages = ["python",
                         "java",
                         "C",
                         "Assembly Language",
                         "PHP",
                         "JavaScript",
                         "R",
                         "Sql"]  # add programing languages here only it will get all the data in one csv

driver = webdriver.Chrome('/home/paritosh/PycharmProjects/Youtube/chromedriver')  # this is chromedriver location

Page = 5  # here write number of pages you want to scrape

for i in range(1, Page):
    print('Opening Search Pages ' + str(i))
    # make changes here for the multiple courses
    website = "https://www.udemy.com/courses/search/?p={}&q=python+programming&src=ukw".format(i)
    driver.get(website)
    print('Accessing Webpage OK \n')

driver.quit()

end = time.time()

print((end - start) / 60, "min")

# time taken to run the program
