from selenium import webdriver
from selenium.webdriver.common.by import By
import time

driver = webdriver.Chrome(executable_path="C:/chromedriver/chromedriver.exe")
driver.get("https://buy.yungching.com.tw/region/台中市")
time.sleep(10)

# 1️⃣ 點「div.areaSpan」展開地區選單
# area_span = driver.find_element(By.CSS_SELECTOR, "div.area > div.areaSpan")
# area_span.click()
# time.sleep(1)

# # 2️⃣ 點選特定地區（例如：台中市太平區）
# target_area = driver.find_element(By.XPATH, "//div[@class='areaSpan']/span[text()='台中市太平區']")
# target_area.click()
# time.sleep(1)

# 3️⃣ 點擊「搜尋按鈕」
# search_button = driver.find_element(By.CSS_SELECTOR, "div.search-btn")
# search_button.click()

search_button = driver.find_element(By.CSS_SELECTOR, "div.search-btn")
driver.execute_script("arguments[0].click();", search_button)
print("✅ 已點擊搜尋按鈕")

# 4️⃣ 等待新資料載入
time.sleep(3)

# # 5️⃣ 擷取房屋列表
# houses = driver.find_elements(By.CSS_SELECTOR, ".search-result-list .house-info")
# for house in houses[:3]:
#     title = house.find_element(By.CSS_SELECTOR, ".house-title a").text
#     print(f"房屋：{title}")

driver.quit()
