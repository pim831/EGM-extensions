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
from selenium.webdriver.support.wait import WebDriverWait

opts = ChromeOptions()
opts.add_argument("--window-size=720,1080")
# opts.add_argument("--headless")


driver = webdriver.Chrome(options=opts)

data_dir = "../Data/MLS_raw"

driver.get(
    "https://www.google.com/search?q=lafc&oq=lafc&aqs=chrome.0.69i59l4j0i131i433i512j0i433i512j0i131i433i512j0i433i512j0i131i433i512j46i340i512.607j0j7&sourceid=chrome&ie=UTF-8#bsht=CgVic2hocBIECAQwAQ&sie=t;/m/0126951x;2;/m/0jfpf;mt;fp;1;;;"
)

# Body to send page_up/page_down to scroll up and down


cookies = driver.find_element(By.XPATH, '//*[@id="L2AGLb"]')
cookies.click()


time.sleep(1)

body = driver.find_element(By.CSS_SELECTOR, "body")

for j in range(10):
    time.sleep(2)
    for i in range(5):
        body.send_keys(Keys.PAGE_UP)

matches = driver.find_elements(
    By.XPATH,
    '//*[@id="liveresults-sports-immersive__updatable-team-matches"]/*/div/table/tbody/*/*',
)
for i, match in enumerate(matches):
    if match.get_attribute("innerHTML") == "":
        continue
    try:
        match.click()
    except:
        body.send_keys(Keys.PAGE_DOWN)
        match.click()

    time.sleep(2.5)

    if "Tijdlijn" in driver.page_source:
        timeline = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[3]/div/div[1]/div/ol/li[1]',
        )
        timeline.click()
        time.sleep(1)

        events = driver.find_elements(By.XPATH, '//*[@id="match-feed"]/*')

        match_data = pd.DataFrame(columns=["record", "minute", "name", "team_pos"])
        #
        players = driver.find_elements(By.CSS_SELECTOR, "div.imso_gf__pl-nm")
        team_positions = driver.find_elements(By.CSS_SELECTOR, "div.imso_gf__pl-info")
        player_count = 0
        for i, e in enumerate(events):
            if "Rust" in e.get_attribute("innerHTML"):
                continue
            if "AFTRAP" in e.get_attribute("innerHTML"):
                continue
            if "EINDE WEDSTRIJD" in e.get_attribute("innerHTML"):
                continue
            # VAR event
            if (
                "https://ssl.gstatic.com/onebox/sports/game_feed/var_icon.svg"
                in e.get_attribute("innerHTML")
            ):
                continue
            if "DOELPUNT" in e.get_attribute("innerHTML"):
                minute = driver.find_element(
                    By.XPATH,
                    '//*[@id="match-feed"]/div['
                    + str(i + 1)
                    + "]/div/div/div/div/div[1]/div[1]/div[2]",
                ).get_attribute("innerHTML")
                name = players[player_count].get_attribute("innerHTML")
                team_pos = team_positions[player_count].get_attribute("innerHTML")
                player_count += 1
                record = "goal"
                if "EIGEN" in e.get_attribute("innerHTML"):
                    record = "own_goal"
            elif "GELE" in e.get_attribute("innerHTML"):
                minute = driver.find_element(
                    By.XPATH,
                    '//*[@id="match-feed"]/div['
                    + str(i + 1)
                    + "]/div/div/div/div/div[1]/div[1]/div/div[2]",
                ).get_attribute("innerHTML")
                name = players[player_count].get_attribute("innerHTML")
                team_pos = team_positions[player_count].get_attribute("innerHTML")
                player_count += 1
                record = "yellow_card"
            elif "RODE" in e.get_attribute("innerHTML"):
                minute = driver.find_element(
                    By.XPATH,
                    '//*[@id="match-feed"]/div['
                    + str(i + 1)
                    + "]/div/div/div/div/div[1]/div[1]/div/div[2]",
                ).get_attribute("innerHTML")
                name = players[player_count].get_attribute("innerHTML")
                team_pos = team_positions[player_count].get_attribute("innerHTML")
                player_count += 1
                record = "red_card"
            elif "WISSEL" in e.get_attribute("innerHTML"):
                minute = driver.find_element(
                    By.XPATH,
                    '//*[@id="match-feed"]/div['
                    + str(i + 1)
                    + "]/div/div/div/div/div[1]/div[1]/div/div[2]",
                ).get_attribute("innerHTML")
                player_in = players[player_count].get_attribute("innerHTML")
                team_pos_in = team_positions[player_count].get_attribute("innerHTML")
                player_count += 1
                player_out = players[player_count].get_attribute("innerHTML")
                team_pos_out = team_positions[player_count].get_attribute("innerHTML")
                player_count += 1
                name = player_in + "--" + player_out
                team_pos = team_pos_in + "--" + team_pos_out
                record = "substitution"
            elif "GEMISTE STRAFSCHOP" in e.get_attribute("innerHTML"):
                player_count += 1
            match_data.loc[len(match_data)] = [record, minute, name, team_pos]

        # Scroll upwards so the line_up element is visible
        b = driver.find_element(By.CSS_SELECTOR, "body")
        for i in range(5):
            b.send_keys(Keys.PAGE_UP)

        line_up = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[3]/div/div[1]/div/ol/li[2]',
        )
        line_up.click()
        time.sleep(1)

        # Team 1
        team1_line_up = pd.DataFrame(columns=["team", "name", "line"])
        # Some matches don't contain lineups, ignore those matches
        try:
            team1 = driver.find_element(By.XPATH, '//*[@id="lrvl_ht"]').get_attribute(
                "innerHTML"
            )
        except:
            driver.back()
            time.sleep(0.5)
            continue

        team1_shape = list(
            map(
                int,
                driver.find_element(
                    By.XPATH,
                    '//*[@id="match-lineups"]/div/div/div[1]/div[1]/div[1]/span[2]',
                )
                .get_attribute("innerHTML")
                .split("-"),
            )
        )
        team1_players = driver.find_elements(
            By.XPATH,
            '//*[@id="match-lineups"]/div/div/div[1]/div[1]/div[2]/span/*/div/*/div[2]/span',
        )
        line = 0
        # keeper
        team1_line_up.loc[len(team1_line_up)] = [
            team1,
            team1_players[0].get_attribute("innerHTML"),
            line,
        ]
        # field players
        for i, player in enumerate(team1_players[1:]):
            base = 0
            for j, l in enumerate(team1_shape):
                if i + 1 <= l + base:
                    line = j + 1
                    break
                else:
                    base += l
            team1_line_up.loc[len(team1_line_up)] = [
                team1,
                player.get_attribute("innerHTML"),
                line,
            ]
        # Players starting on the bench
        subs_team1 = driver.find_elements(
            By.XPATH,
            '//*[@id="match-lineups"]/div/div/div[2]/div[2]/table/tbody/*/td[2]/div/div/span[1]',
        )
        for sub in subs_team1:
            team1_line_up.loc[len(team1_line_up)] = [
                team1,
                sub.get_attribute("innerHTML"),
                "bench",
            ]

        # Team 2
        team2_line_up = pd.DataFrame(columns=["team", "name", "line"])
        team2 = driver.find_element(By.XPATH, '//*[@id="lrvl_at"]').get_attribute(
            "innerHTML"
        )
        team2_shape = list(
            map(
                int,
                driver.find_element(
                    By.XPATH,
                    '//*[@id="match-lineups"]/div/div/div[1]/div[3]/div[2]/span[2]',
                )
                .get_attribute("innerHTML")
                .split("-"),
            )
        )
        team2_players = driver.find_elements(
            By.XPATH,
            '//*[@id="match-lineups"]/div/div/div[1]/div[3]/div[1]/span/*/div/*/div[2]/span',
        )

        line = 0
        # keeper
        team2_line_up.loc[len(team2_line_up)] = [
            team2,
            team2_players[0].get_attribute("innerHTML"),
            line,
        ]
        # field players
        for i, player in enumerate(team2_players[1:]):
            base = 0
            for j, l in enumerate(team2_shape):
                if i + 1 <= l + base:
                    line = j + 1
                    break
                else:
                    base += l
            team2_line_up.loc[len(team2_line_up)] = [
                team2,
                player.get_attribute("innerHTML"),
                line,
            ]
        # Players starting on the bench
        subs_team2 = driver.find_elements(
            By.XPATH,
            '//*[@id="match-lineups"]/div/div/div[2]/div[2]/table/tbody/*/td[4]/div/div/span[1]',
        )
        for sub in subs_team2:
            team2_line_up.loc[len(team2_line_up)] = [
                team2,
                sub.get_attribute("innerHTML"),
                "bench",
            ]

        t1 = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[1]/div/div/div/div/div[1]/div[1]/div[2]/div[1]/div/div[1]/div[2]/div/span',
        ).get_attribute("innerHTML")
        t2 = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[1]/div/div/div/div/div[1]/div[1]/div[2]/div[1]/div/div[3]/div[2]/div/span',
        ).get_attribute("innerHTML")
        d = driver.find_element(
            By.XPATH,
            '//*[@id="liveresults-sports-immersive__match-fullpage"]/div/div[2]/div[4]/div[1]/div/div/div/div/div[1]/div[1]/div[1]/div/div/span[2]',
        ).get_attribute("innerHTML")
        dates = d.split("-")
        if len(dates) < 3:
            dates.append(str(datetime.date.today().year))
        day, month, year = dates
        date = "{}-{}-{}".format(year, month, day)

        path = os.path.join(data_dir, (date + "_" + t1 + "-" + t2))
        os.mkdir(path)
        match_data.to_pickle(os.path.join(path, "match_data"))

        team1_line_up.to_pickle(os.path.join(path, "team1"))
        team2_line_up.to_pickle(os.path.join(path, "team2"))

    driver.back()
    # Wait so the browser can load the page
    time.sleep(0.5)


driver.close()
