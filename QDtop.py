from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

def extract_data(html_source):
    # 使用BeautifulSoup解析HTML
    soup = BeautifulSoup(html_source, 'html.parser')

    page_data = []

    try:
        # 获取所有大学卡片
        university_cards = soup.select(".new-ranking-cards.normal-row")

        for card in university_cards:
            try:
                rank = card.select_one(".left-div-200 .dark-blue .rank-no").get_text(strip=True)
                overall_score = card.select_one(".left-div-200 .light-blue .rank-score").get_text(strip=True)
                university_name = card.select_one(".right-div-200 .uni-link").get_text(strip=True)
                location_tag = card.select_one(".right-div-200 .location")
                location = location_tag.get_text(strip=True) if location_tag else ""
                location = re.sub(r'\s+', ' ', location).replace("location", "").strip()

                page_data.append({
                    '排名': rank,
                    '总分': overall_score,
                    '大学名称': university_name,
                    '地址': location
                })
            except Exception as e:
                print(f"提取单张卡片数据出错: {e}")
                continue

    except Exception as e:
        print(f"提取卡片列表出错: {e}")

    return pd.DataFrame(page_data)

def get_pages(keyword):
    service = Service(r'C:\Users\Administrator\miniconda3\Scripts\chromedriver.exe')
    browser = webdriver.Chrome(service=service)
    browser.maximize_window()
    url = 'https://www.topuniversities.com/world-university-rankings/2025?items_per_page=150'
    browser.get(url)

    browser.find_element(By.XPATH, '//*[@id="it-will-be-fixed-top"]/div[2]/div/button/span[1]').click()
    # browser.find_element(By.XPATH,'//*[@id="nav-searchform"]/div[1]/input').send_keys(keyword)
    browser.find_element(By.XPATH, '//*[@id="filterAccordion"]/div[1]/div/div/div').click()
    browser.find_element(By.XPATH, '//*[@id="filterAccordion"]/div[1]/div/div/div/div[2]/div[3]').click()
    browser.find_element(By.XPATH, '//*[@id="newRankingFilters_newUI"]/div[3]/div[7]/div[1]/div[2]/i').click()
    browser.find_element(By.XPATH, '//*[@id="newRankingFilters_newUI"]/div[3]/div[7]/div[1]/div[2]/div[2]/div[4]').click()
    time.sleep(5)

    # all_handles = browser.window_handles
    # browser.switch_to.window(all_handles[-1])

    page_data = []
    page_data.append(extract_data(browser.page_source))
    all_data = pd.concat(page_data, ignore_index=True)
    all_data.to_excel('QStop大学排名.xlsx', index=False)
    print(f"Done,get {len(all_data)} messages")
    browser.quit()

get_pages('QStop')