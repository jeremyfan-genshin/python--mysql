# -*- coding: utf-8 -*-
"""
Created on Sat Jul 19 20:43:56 2025

@author: SWDL_15_DIS
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--headless')  # 如果需要無頭模式

service = Service('path_to_chromedriver')  # 指定正確路徑
driver = webdriver.Chrome(service=service, options=options)

try:
    driver.get("https://www.google.com")
    print(driver.title)
finally:
    driver.quit()
