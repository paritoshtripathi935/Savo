# open chrome browser using selenium

from seleniumwire import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import time

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(executable_path='/home/paritosh/anakin/enterprise/chromedriver_linux64/chromedriver', chrome_options=chrome_options)

driver.get("https://www.enterprise.com/en/home.html")

# (By.CLASS_NAME, "rs-input__field selected"))
time.sleep(5)

element = driver.find_element(By.CSS_SELECTOR, "#pickupLocationTextBox")
driver.execute_script('arguments[0].click()', element)
#element.clear()
element.send_keys("New York, NY")
print(element.text)

# click #continueButton

element = driver.find_element(By.CSS_SELECTOR, "#continueButton")
driver.execute_script('arguments[0].click()', element)
print(element.text)


time.sleep(5)


# check if click was successful
print(driver.current_url)

# now get all the traffic from the website
# and save it in a file
for request in driver.requests:
    if request.response:    
        # save all the traffic in a file
        with open('traffic.txt', 'a') as f:
            f.write(str(request.url) + "\n")

        api = "https://prd-east.webapi.enterprise.com/enterprise-ewt/current-session"
        if request.url == api:
            head = request.headers
            print(head['cookie'])
            with open('head.json', 'w') as f:
                f.write(str(head))

        
    
