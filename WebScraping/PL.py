from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from tqdm import tqdm
import datetime
import pandas as pd
import os
import time

from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.keys import Keys

opts = ChromeOptions()

driver = webdriver.Chrome(options=opts)

driver.get(
    "https://www.google.com/search?q=liverpool+-+wolverhampton&rlz=1C1GCEU_nlNL821NL821&sxsrf=APwXEdc8_VHSroMUjL4tcvw46K1Xagtfmw%3A1686658863337&ei=L1-IZPz5E-qK9u8P2pWEmAo&oq=liv&gs_lcp=Cgxnd3Mtd2l6LXNlcnAQAxgAMgcIIxCKBRAnMgcIIxCKBRAnMgoILhCxAxCKBRBDMgcIABCKBRBDMgcIABCKBRBDMgcIABCKBRBDMhEILhCABBCxAxCDARDHARDRAzIHCAAQigUQQzIHCAAQigUQQzIHCAAQigUQQzoHCCMQsAMQJzoKCAAQRxDWBBCwAzoNCAAQ5AIQ1gQQsAMYAToPCC4QigUQyAMQsAMQQxgCOhIILhDUAhCKBRDIAxCwAxBDGAI6BwguEIoFECc6BAgjECc6BwguEIoFEEM6EwguEIoFEJcFENwEEN4EEOAEGAM6BQgAEIAESgQIQRgAUKcHWMAQYP8caAVwAXgAgAFiiAHHA5IBATWYAQCgAQHAAQHIAQ_aAQYIARABGAnaAQYIAhABGAjaAQYIAxABGBQ&sclient=gws-wiz-serp#sie=lg;/g/11c74zg7g7;2;/m/02_tc;dt;fp;1;;;"
)

data_dir = "Code/Data/PL_raw"

cookies = driver.find_element(By.XPATH, '//*[@id="L2AGLb"]')
cookies.click()

time.sleep(1)

body = driver.find_element(By.CSS_SELECTOR, "body")

for j in range(10):
    time.sleep(1)
    for i in range(5):
        body.send_keys(Keys.HOME)

matches = driver.find_elements(By.CSS_SELECTOR, "div.imspo_mt__mtc-no")

match_lineups = pd.DataFrame(
    columns=[
        "date",
        "home_team_name",
        "home_team_lineup",
        "away_team_name",
        "away_team_lineup",
    ]
)

for m in matches:
    if m.get_attribute("innerHTML") == "":
        continue
    try:
        m.click()
    except:
        body.send_keys(Keys.PAGE_DOWN)
        m.click()

    if "Opstellingen" in driver.page_source:
        time.sleep(2.5)
        line_up = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[3]/div/div[1]/div/ol/li[2]',
        )
        try:
            line_up.click()
        except:
            driver.refresh()
            time.sleep(1)
            line_up = driver.find_element(
                By.XPATH,
                '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[3]/div/div[1]/div/ol/li[2]',
            )
            line_up.click()

        # try:
        #     line_up = driver.find_element(
        #         By.XPATH,
        #         '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[3]/div/div[1]/div/ol/li[2]',
        #     )
        #     line_up.click()
        # except:
        #     driver.refresh()
        #     time.sleep(0.5)
        #     line_up = driver.find_element(
        #         By.XPATH,
        #         '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[3]/div/div[1]/div/ol/li[2]',
        #     )
        #     line_up.click()
        time.sleep(0.5)

        date = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[1]/div[3]/div/div/div/div[1]/div[1]/div[1]/div/div/span[2]',
        ).get_attribute("innerHTML")

        home_team_name = driver.find_element(
            By.XPATH,
            '//*[@id="lrvl_ht"]',
        ).get_attribute("innerHTML")

        home_team_lineup = driver.find_element(
            By.XPATH,
            '//*[@id="match-lineups"]/div/div/div[1]/div[1]/div[1]/span[2]',
        ).get_attribute("innerHTML")

        away_team_name = driver.find_element(
            By.XPATH,
            '//*[@id="lrvl_at"]',
        ).get_attribute("innerHTML")

        away_team_lineup = driver.find_element(
            By.XPATH,
            '//*[@id="match-lineups"]/div/div/div[1]/div[3]/div[2]/span[2]',
        ).get_attribute("innerHTML")

        match_lineups.loc[len(match_lineups)] = [
            date,
            home_team_name,
            home_team_lineup,
            away_team_name,
            away_team_lineup,
        ]
    driver.back()
    time.sleep(0.5)
