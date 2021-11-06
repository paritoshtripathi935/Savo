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


def extract_text(soup_obj, tag, attribute_name, attribute_value):
    txt = soup_obj.find(tag, {attribute_name: attribute_value}).text.strip() if soup_obj.find(tag, {
        attribute_name: attribute_value}) else ''
    return txt


driver = webdriver.Chrome('/home/paritosh/PycharmProjects/Youtube/chromedriver')  # this is chromedriver location

Page = 20  # here write number of pages you want to scrape

for i in range(1, Page):
    for j in range(0, len(programming_languages)):

        print('Opening Search Pages ' + programming_languages[j] + " " + str(i))
        # make changes here for the multiple courses

        website = ("https://www.udemy.com/courses/search/?p={}&q={}&src=ukw".format(i, programming_languages[j]))  # Main url here please make different copy of code if want to add changes
        driver.get(website)

        print('Accessing Webpage OK \n')

        time.sleep(5)  # delay given for pages to load

        try:
            WebDriverWait(driver, delay).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'course-list--container--3zXPS')))
        except TimeoutException:
            print('Loading exceeds delay time')
            break
        else:
            soup = BeautifulSoup(driver.page_source,
                                 'html.parser')  # setting up the soup and gets content of all the soup
            course_list = soup.find('div', {
                'class': 'course-list--container--3zXPS'})  # you have to inspect element for to find this classes

            courses = course_list.find_all('a',
                                           {'class': 'udlite-custom-focus-visible browse-course-card--link--3KIkQ'})

            for course in courses:
                course_url = '{0}{1}'.format('https://www.udemy.com', course['href'])  # this find links and joins

                course_title = course.select('div[class*="course-card--course-title"]')[
                    0].text  # this find titles of courses

                course_headline = extract_text(course, 'p', 'data-purpose',
                                               'safely-set-inner-html:course-card:course-headline')  # description of the course

                author = extract_text(course, 'div', 'data-purpose',
                                      'safely-set-inner-html:course-card:visible-instructors')  # instructor of the course

                course_rating = extract_text(course, 'span', 'data-purpose', 'rating-number')  # ratings of the course

                course_price = extract_text(course, 'div', 'data-purpose', 'course-price-text')  # price of the course

                course_detail = course.find_all('span', {'class': 'course-card--row--29Y0w'})

                try:  # have to use this because of the null values
                    course_length = course_detail[0].text
                    number_of_lectures = course_detail[1].text
                    difficulty = course_detail[2].text
                except IndexError:
                    pass

                rows.append(
                    [course_url,
                     course_title,
                     course_headline,
                     author,
                     course_rating,
                     course_price,
                     course_length,
                     number_of_lectures,
                     difficulty,
                     programming_languages[j]]
                )

columns = ['url',
           'Title',
           'Course Headline',
           'Instructor',
           'Rating',
           'Price',
           'length',
           'number_of_lectures',
           'difficulty',
           'type']

df = pd.DataFrame(data=rows, columns=columns)
df.to_csv('Udemy_programming_languages.csv', index=False)
driver.quit()

end = time.time()

print((end - start) / 60, "min")

# time taken to run the program
