import pandas as pd
import numpy as np
from itertools import chain
from tqdm import tqdm
import json

league = "England"

print("loading data...")

with open(
    "Code/Data/figshare/events/events_" + league + ".json", encoding="utf-8"
) as f:
    events = json.load(f)
f.close()

with open(
    "Code/Data/figshare/matches/matches_" + league + ".json", encoding="utf-8"
) as f:
    matches = json.load(f)
f.close()

with open("Code/Data/figshare/teams.json", encoding="utf-8") as f:
    teams = json.load(f)
f.close()


lineups = pd.read_csv("Code/Data/PL_Raw_Lineups_17-18.csv", ",", index_col=0)


event_types = set(list(map(lambda e: e["subEventName"], events)))
unique_players = set(list(map(lambda e: e["playerId"], events)))

team_dict = {}
for t in teams:
    id = t["wyId"]
    name = t["name"]
    team_dict[id] = name

lp1 = lineups.drop(columns=["away_team_name", "away_team_lineup"])
lp1 = lp1.rename(columns={"home_team_name": "name", "home_team_lineup": "lineup"})
lp2 = lineups.drop(columns=["home_team_name", "home_team_lineup"])
lp2 = lp2.rename(columns={"away_team_name": "name", "away_team_lineup": "lineup"})
lp = pd.concat([lp1, lp2], ignore_index=True)

lineup_dict = {}
for d in lp["date"].unique():
    lineup_dict[d] = {}

for index, row in lp.iterrows():
    lineup_dict[row["date"]][row["name"]] = row["lineup"]


for dict in lineup_dict:
    if "Bournemouth" in lineup_dict[dict].keys():
        lineup_dict[dict]["AFC Bournemouth"] = lineup_dict[dict].pop("Bournemouth")
    if "West Brom" in lineup_dict[dict].keys():
        lineup_dict[dict]["West Bromwich Albion"] = lineup_dict[dict].pop("West Brom")
    if "Huddersfield" in lineup_dict[dict].keys():
        lineup_dict[dict]["Huddersfield Town"] = lineup_dict[dict].pop("Huddersfield")
    if "Brighton" in lineup_dict[dict].keys():
        lineup_dict[dict]["Brighton & Hove Albion"] = lineup_dict[dict].pop("Brighton")
    if "Man Utd" in lineup_dict[dict].keys():
        lineup_dict[dict]["Manchester United"] = lineup_dict[dict].pop("Man Utd")
    if "Newcastle" in lineup_dict[dict].keys():
        lineup_dict[dict]["Newcastle United"] = lineup_dict[dict].pop("Newcastle")
    if "Swansea" in lineup_dict[dict].keys():
        lineup_dict[dict]["Swansea City"] = lineup_dict[dict].pop("Swansea")
    if "Tottenham" in lineup_dict[dict].keys():
        lineup_dict[dict]["Tottenham Hotspur"] = lineup_dict[dict].pop("Tottenham")
    if "West Ham" in lineup_dict[dict].keys():
        lineup_dict[dict]["West Ham United"] = lineup_dict[dict].pop("West Ham")


def remove_leading_zeros(number):
    return str(int(number))


match_dict = {}
for i, m in enumerate(matches):
    dates = m["dateutc"].split(" ")[0][2:].split("-")
    dates.reverse()
    dates = list(map(remove_leading_zeros, dates))
    d = "-".join(dates)

    team_info = {}
    for i, t_key in enumerate(m["teamsData"]):
        match_info = {}
        match_info["formation"] = lineup_dict[d][team_dict[int(t_key)]]

        lineup = list(
            map(lambda t: t["playerId"], m["teamsData"][t_key]["formation"]["lineup"])
        )
        bench = list(
            map(lambda t: t["playerId"], m["teamsData"][t_key]["formation"]["bench"])
        )
        result = [[0, lineup.copy(), bench.copy()]]
        if m["teamsData"][t_key]["formation"]["substitutions"] != "null":
            for sub in m["teamsData"][t_key]["formation"]["substitutions"]:
                lineup.remove(sub["playerOut"])
                lineup.append(sub["playerIn"])
                bench.remove(sub["playerIn"])
                bench.append(sub["playerOut"])
                result.append([sub["minute"], lineup.copy(), bench.copy()])
        match_info["timeline"] = result
        team_info[int(t_key)] = match_info

    match_dict[m["wyId"]] = team_info

df = pd.DataFrame(
    columns=["team", "record", "formation", "for-against", *unique_players]
)

clean_data = []

for i, e in tqdm(enumerate(events)):
    record = e["subEventName"]
    teamId = e["teamId"]
    matchId = e["matchId"]
    for team_key in match_dict[matchId]:
        if team_key == teamId:
            factor = 1
        else:
            factor = -1
        team_timeline = match_dict[matchId][team_key]["timeline"]
        formation = match_dict[matchId][team_key]["formation"]

        time = int(e["eventSec"] / 60)
        if e["matchPeriod"] == "2H":
            time += 45

        tp = -1
        for l in team_timeline:
            if time >= l[0]:
                tp += 1

        new_row = {
            "team": [team_key],
            "record": [record],
            "formation": [formation],
            "for-against": [factor],
        }
        for t in team_timeline[tp][1]:
            new_row[t] = [1]
        for t in team_timeline[tp][2]:
            new_row[t] = [2]
        df = pd.concat([df, pd.DataFrame.from_dict(new_row)]).fillna(0)
    if i % 199 == 0:
        clean_data.append(df)
        df = pd.DataFrame(
            columns=["team", "record", "formation", "for-against", *unique_players]
        )

clean_data.append(df)
full_event_set = pd.concat(clean_data)
full_event_set = full_event_set.fillna(0)

full_event_set.to_csv(
    path_or_buf="Code/Data/PL_clean_events_negative_v2.csv", header=True
)
